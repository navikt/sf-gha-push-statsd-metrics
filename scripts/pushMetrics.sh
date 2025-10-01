#!/usr/bin/env bash
# shellcheck shell=bash
set -euo pipefail

# ---------------------------------------------------------------------------
# pushMetrics.sh - Secure Prometheus Metrics Transmission
# 
# Purpose: Sign (optional) and transmit Prometheus exposition-format metrics to
#          a remote ingestion endpoint with retry + jitter backoff.
#
# Security Model:
# - Uses umask 077 and ephemeral temp files for private key handling
# - Private keys never written to persistent storage during execution  
# - SHA-256 + RSA signatures for payload integrity verification
# - Supports unsigned mode for development/testing environments
#
# Retry Logic:
# - Exponential backoff: attempt n (n>=2) sleeps 2^(n-2) seconds
# - Random jitter: + random(0..JITTER_MAX_MS)/1000 to prevent thundering herd
# - Example: attempt 1=0s, attempt 2=0-0.4s, attempt 3=1-1.4s, attempt 4=2-2.4s
#
# Data Flow: read_metrics() -> build_payload() -> post_payload()
# ---------------------------------------------------------------------------

umask 077  # Restrictive permissions for any created files

# --- Defaults / Tunables ----------------------------------------------------
MAX_BYTES="${MAX_BYTES:-1000000}"
RETRIES=3
JITTER_MAX_MS=400

# --- Mode Flags ------------------------------------------------------------
USE_STDIN=true
DRY_RUN=false
NO_SIGN=false

# --- Formatting / Decoration (blank by default) -----------------------------
ERROR_PREFIX=""
WARN_PREFIX=""
LOG_GROUP_PREFIX=""
LOG_GROUP_SUFFIX=""

# --- Inputs / Derived -------------------------------------------------------
ENDPOINT=""
RUNNER_NAME=""
METRICS_FILE=""

# --- Internal Scratch (avoid leaking into env) ------------------------------
_metrics=""
_payload=""

# --- Helpers ----------------------------------------------------------------
# Safely append an EXIT trap without overwriting existing ones
# This allows multiple cleanup operations to be chained together
# Args: $1 - command to execute on EXIT
add_exit_trap() {
    local new="$1" existing
    existing=$(trap -p EXIT | awk -F'"' '{print $2}' || true)
    if [[ -n $existing ]]; then
        trap "$existing; $new" EXIT
    else
        trap "$new" EXIT
    fi
}

die() { echo "${ERROR_PREFIX}$*" >&2; exit 1; }
warn() { echo "${WARN_PREFIX}$*" >&2; }
need() { command -v "$1" >/dev/null || die "Missing dependency '$1'"; }

emit_group_start() { [[ -n $LOG_GROUP_PREFIX ]] && echo "${LOG_GROUP_PREFIX}$1" || true; }
emit_group_end()   { [[ -n $LOG_GROUP_SUFFIX ]] && echo "$LOG_GROUP_SUFFIX" || true; }

usage() {
        cat <<EOF
Usage: $0 [options]

Options:
    --endpoint <url>           Target ingestion endpoint (required)
    --runner-name <name>       Logical source identifier (required)
    --max-bytes <n>            Maximum payload size (default: ${MAX_BYTES})
    --metrics-file <file>      Read metrics from file instead of stdin
    --log-group-prefix <str>   Prefix for group start (default: blank)
    --log-group-suffix <str>   Suffix for group end   (default: blank)
    --retries <n>              Retry attempts on POST failure (default: ${RETRIES})
    --jitter-max-ms <n>        Max random jitter added to backoff (default: ${JITTER_MAX_MS})
    --error-prefix <str>       Prefix prepended to error messages (default: blank)
    --warn-prefix <str>        Prefix prepended to warning messages (default: blank)
    --dry-run                  Perform all steps except the final POST (signs unless --no-sign)
    --no-sign                  Skip signing entirely (payload sent unsigned)
    --help                     Show this help

Environment:
    RUNNER_NAME        Source identifier if not passed via --runner-name
    METRICS_KEY        PEM private key (required unless --no-sign)
    MAX_BYTES          Override default maximum payload size

Exit codes:
    0 success
    1 failure (validation, signing, or POST retries exhausted)

Examples:
    # Production signed POST reading from stdin
    cat metrics.prom | RUNNER_NAME=prod-runner METRICS_KEY="\$(cat private.pem)" \\
        $0 --endpoint https://ingest.example/metrics

    # Local development with file input (unsigned for testing)
    $0 --endpoint https://ingest.example/metrics \\
       --runner-name local-dev --metrics-file metrics.prom --dry-run --no-sign

    # GitHub Actions integration with proper grouping
    cat metrics.prom | RUNNER_NAME=\${GITHUB_RUN_ID} $0 \\
        --endpoint https://ingest.example/metrics \\
        --log-group-prefix '::group::' --log-group-suffix '::endgroup::' \\
        --error-prefix '::error::' --warn-prefix '::warning::'

    # Custom retry configuration for unreliable networks  
    cat metrics.prom | RUNNER_NAME=edge-node $0 \\
        --endpoint https://ingest.example/metrics --retries 5 --jitter-max-ms 1000

EOF
}

validate_and_prepare() {
    for c in jq curl awk; do need "$c"; done
    # Only require openssl when signing is enabled (allows unsigned local use without openssl installed)
    if ! $NO_SIGN; then need openssl; fi

    [[ $RETRIES =~ ^[1-9][0-9]*$ ]] || die "--retries must be a positive integer"
    [[ $JITTER_MAX_MS =~ ^[0-9]+$ ]] || die "--jitter-max-ms must be a non-negative integer"
    [[ $MAX_BYTES =~ ^[0-9]+$ ]] || die "--max-bytes must be a number"
    [[ -n ${RUNNER_NAME:-} ]] || die "Missing --runner-name"
    [[ -n $ENDPOINT ]] || die "Missing --endpoint"
    if ! $NO_SIGN; then [[ -n ${METRICS_KEY:-} ]] || die "Missing METRICS_KEY env var (required unless --no-sign)"; fi
    if ! $USE_STDIN && [[ -z ${METRICS_FILE:-} ]]; then die "Must provide --metrics-file if not using --stdin"; fi
    [[ $MAX_BYTES -ge 1000 ]] || die "--max-bytes must be at least 1000"
}

# Read metrics from stdin or file, validate size and content
# Sets global variable: _metrics
# Outputs: metrics summary, content, and hash to stdout/stderr
read_metrics() {
    if $USE_STDIN; then
        echo "Reading metrics from standard input" >&2
        _metrics=$(cat)
    else
        [[ -f "$METRICS_FILE" ]] || die "Metrics file not found: $METRICS_FILE"
        echo "Reading metrics from $METRICS_FILE" >&2
        _metrics=$(<"$METRICS_FILE")
    fi
    # Strip trailing newlines to normalize input
    while [[ $_metrics == *$'\n' ]]; do _metrics=${_metrics%$'\n'}; done
    # Ensure we have non-whitespace content
    [[ -n "${_metrics//[[:space:]]/}" ]] || die "No metrics generated"
    local bytes line_count
    bytes=$(LC_ALL=C printf %s "$_metrics" | wc -c)
    (( bytes <= MAX_BYTES )) || die "Metrics payload too large (${bytes} > ${MAX_BYTES})"
    emit_group_start "Metrics summary"
    line_count=$(printf '%s' "$_metrics" | awk 'END{print NR}')
    printf 'Lines: %s\n' "$line_count"
    printf 'Bytes: %s\n' "$bytes"
    emit_group_end
    emit_group_start "Metrics"
    echo "$_metrics"
    emit_group_end
    emit_group_start "Payload hash"
    printf %s "$_metrics" | sha256sum | awk '{print "sha256=" $1}'
    emit_group_end
}

# Build JSON payload with metrics data, optionally signed
# Requires: _metrics global variable set by read_metrics()
# Sets: _payload global variable with JSON string
# Uses temporary files for secure key handling and proper JSON escaping
build_payload() {
    local metricsfile
    metricsfile=$(mktemp)
    add_exit_trap "rm -f '$metricsfile'"
    printf %s "$_metrics" > "$metricsfile"
    
    if $NO_SIGN; then
        warn "Signing disabled (--no-sign)"
        _payload=$(jq -c --arg r "$RUNNER_NAME" --rawfile m "$metricsfile" --null-input '{"runner":$r,"metrics":$m}')
    else
        local keyfile sig
        keyfile=$(mktemp)
        add_exit_trap "rm -f '$keyfile'"
        cat > "$keyfile" <<< "${METRICS_KEY:?METRICS_KEY missing}"
        if ! openssl pkey -in "$keyfile" -check -noout >/dev/null 2>&1; then die "Invalid private key"; fi
        # Create SHA-256 signature of metrics data, base64 encoded
        sig=$(printf %s "$_metrics" | openssl dgst -sha256 -sign "$keyfile" -out - | openssl base64 -A)
        # Build signed payload using rawfile to safely handle special characters
        _payload=$(jq -c --arg r "$RUNNER_NAME" --arg s "$sig" --rawfile m "$metricsfile" --null-input '{"runner":$r,"metrics":$m,"signature":$s}')
    fi

    emit_group_start "Metrics payload"
    echo "$_payload"
    emit_group_end
}

# Exponential backoff with jitter for retry attempts
# Formula: 2^(attempt-2) seconds + random(0..JITTER_MAX_MS)/1000
# Args: $1 - attempt number (1-based)
# Returns: immediately for attempt 1, sleeps for attempts 2+
backoff_sleep() {
    local attempt=$1
    (( attempt>1 )) || return 0
    local base=$(( 2**(attempt-2) ))
    local jitter_ms=$(( RANDOM % JITTER_MAX_MS ))
    sleep "$(awk -v s="$base" -v j="$jitter_ms" 'BEGIN{printf "%.3f", s + (j/1000)}')"
}

# POST payload to endpoint with retry logic and comprehensive error handling
# Requires: _payload global variable set by build_payload()
# Features: exponential backoff, response capture, detailed error reporting
# Returns: 0 on success (2xx response), exits with error on failure
post_payload() {
    if $DRY_RUN; then
        warn "--dry-run enabled: skipping POST to $ENDPOINT"
        return 0
    fi
    emit_group_start "POST"
    local success=0 http_status="" response_headers response_body
    for ((i=1;i<=RETRIES;i++)); do
        backoff_sleep "$i"
        response_headers=$(mktemp); add_exit_trap "rm -f '$response_headers'"
        response_body=$(mktemp); add_exit_trap "rm -f '$response_body'"
        # POST with response capture; || true prevents script exit on curl failure
        http_status=$(curl -sS -o "$response_body" -w '%{http_code}' -D "$response_headers" -H 'Content-Type: application/json' \
            --data-binary "$_payload" "$ENDPOINT" || true)
        # Check for any 2xx success status
        if [[ $http_status =~ ^2[0-9][0-9]$ ]]; then
            cat "$response_headers"
            rm -f "$response_headers" "$response_body"
            success=1
            break
        else
            echo "Attempt $i failed (status=${http_status:-none}); retrying..." >&2
            cat "$response_headers" >&2 || true
            if [[ -s $response_body ]]; then
                echo "--- response body (truncated) ---" >&2
                head -c 2048 "$response_body" >&2 || true
                echo -e "\n--- end body ---" >&2
            fi
            rm -f "$response_headers" "$response_body"
        fi
    done
    (( success == 1 )) || die "Curl failed after $RETRIES attempts (last status=${http_status:-none})"
    emit_group_end
}

main() {
    validate_and_prepare
    read_metrics
    build_payload
    post_payload
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --endpoint)
            ENDPOINT=$2
            shift 2
            ;;
        --runner-name)
            RUNNER_NAME=$2
            shift 2
            ;;
        --max-bytes)
            MAX_BYTES=$2
            shift 2
            ;;
        --metrics-file)
            METRICS_FILE=$2
            USE_STDIN=false
            shift 2
            ;;
        --log-group-prefix)
            LOG_GROUP_PREFIX=$2
            shift 2
            ;;
        --log-group-suffix)
            LOG_GROUP_SUFFIX=$2
            shift 2
            ;;
        --error-prefix)
            ERROR_PREFIX=$2
            shift 2
            ;;
        --warn-prefix)
            WARN_PREFIX=$2
            shift 2
            ;;
        --retries)
            RETRIES=$2
            shift 2
            ;;
        --jitter-max-ms)
            JITTER_MAX_MS=$2
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --no-sign)
            NO_SIGN=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        --*)
            die "Unknown option: $1"
            ;;
        *)
            die "Unexpected extra argument: $1"
            ;;
    esac
done

main "$@"