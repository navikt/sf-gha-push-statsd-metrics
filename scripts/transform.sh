#!/usr/bin/env bash

set -euo pipefail

# Restrictive permissions for any files/directories we create (e.g., --output path)
umask 077

# Transform a JSON lines metrics log (StatsD-esque) into Prometheus exposition format.
#
# Input line example:
#   {"metric":"sfpowerscripts.scratchorgs.active.remaining","type":"guage","value":19,"timestamp":1758008623383,"tags":{"target_org":"sfdx.integration.user@nav.no"}}
# Output example:
#   # HELP sfpowerscripts_scratchorgs_active_remaining Remaining active scratch org slots
#   # TYPE sfpowerscripts_scratchorgs_active_remaining gauge
#   sfpowerscripts_scratchorgs_active_remaining{target_org="sfdx.integration.user@nav.no"} 19
#
# Features:
# - Dots in metric names become underscores (Prometheus style)
# - Type typo "guage" normalized to "gauge"; counters get _total suffix if missing
# - Emits one HELP/TYPE block per metric family
# - Optional timestamps (ms -> seconds) with --with-timestamps
# - Optional regex filtering of original metric name with --match <regex>
# - Strict mode can fail fast on malformed input with --strict
# - Optional metric metadata file (metrics_config.json or --config <file>) supplies TYPE/HELP overrides and alias mappings
#
# Usage:
#   ./transform.sh [metrics_file] [--with-timestamps] [--match <regex>] [--strict] [--config metrics_config.json]
#
# If metrics_file omitted, defaults to logs/metrics.log
# If a metrics_config.json file exists in the working directory (or --config passed), HELP/TYPE lines use its definitions.
# Aliases can unify multiple source metrics into one target metric with injected labels.

LOG_FILE=""
WITH_TIMESTAMPS=false
MATCH_REGEX=""
STRICT=false
CONFIG_FILE=""
CONFIG_JSON=""
OUTPUT_FILE=""
ERROR_PREFIX="ERROR: "
WARN_PREFIX="WARN: "
NO_META=false
FAIL_ON_WARN=false
readonly MAX_FUTURE_TS_MS=32503680000000 # ~ year 3000 in ms
QUIET=false
LOG_FILE_SET=false
WARN_COUNT=0
PROCESSED_COUNT=0
EMITTED_COUNT=0
SKIPPED_INVALID_JSON=0
SKIPPED_REGEX=0
DEFAULT_COUNTER_VALUE=1

print_help() {
    cat <<EOF
Usage: $0 [metrics_file] [options]

Options:
  --log-file <file>       Path to metrics log file
  --with-timestamps       Include millisecond timestamps (converted to seconds) as third column
  --match <regex>         Only include metrics whose original metric name matches the regex
  --strict                Exit on first invalid JSON or missing metric/value (instead of skipping)
  --config <file>         JSON file mapping original metric names to {"type":"GAUGE|COUNT","help":"desc"}
  --output <file>         Write all Prometheus output to <file> (directories auto-created)
  --no-meta               Suppress HELP/TYPE emission (only samples)
  --quiet                 Suppress non-fatal warnings (also hides summary)
  --fail-on-warn          Exit with code 11 if any warnings occurred
  --error-prefix <str>    Prefix to use for error messages (default: 'ERROR: ')
  --warn-prefix <str>     Prefix to use for warning messages (default: 'WARN: ')
  --default-counter-value <n>  If a count/counter metric line omits 'value', assume this (default: 1)
  --help                  Show this help message

If metrics_file is omitted, defaults to logs/metrics.log
If a metrics_config.json file exists in the working directory (or --config passed), HELP/TYPE lines use its definitions.

Exit codes:
  0 success
  1 generic / config error
  3 invalid JSON (strict mode)
  4 gauge naming violation (_total suffix)
  8 alias cycle detected
 11 warnings encountered (with --fail-on-warn)

Summary (stderr): processed, emitted, skipped_invalid_json, skipped_regex, warnings.
Summary & warnings suppressed by --quiet. --fail-on-warn exits with 11 if any warnings were emitted.
EOF
}

warn() {
  local msg="$1"
  # Do not count summary lines as warnings
  if [[ $msg != Summary:* && $msg != "# Summary:"* ]]; then
    ((WARN_COUNT++))
  fi
  $QUIET || echo "${WARN_PREFIX}${msg}" >&2
}
die() { echo "${ERROR_PREFIX}$*" >&2; exit 1; }
die_code() { local code=$1; shift; echo "${ERROR_PREFIX}$*" >&2; exit "$code"; }

need() { command -v "$1" >/dev/null || die "Missing dependency '$1'"; }

main() {
  # Check dependencies
  need jq awk

  # Resolve LOG_FILE precedence: explicit --log-file > positional > default
  if [[ -z $LOG_FILE && -n ${POSITIONAL_FILE:-} ]]; then
    LOG_FILE="$POSITIONAL_FILE"
  fi
  if [[ -z $LOG_FILE ]]; then
    LOG_FILE="logs/metrics.log"
  fi

  # Auto-detect default config file if not explicitly provided
  if [[ -z $CONFIG_FILE && -f metrics_config.json ]]; then
      CONFIG_FILE="metrics_config.json"
  fi

  if [[ -n $CONFIG_FILE ]]; then
      if [[ ! -f $CONFIG_FILE ]]; then
          die "Config file not found: $CONFIG_FILE"
      fi
      # Load entire JSON (compact) once
      if ! CONFIG_JSON=$(jq -c '.' "$CONFIG_FILE" 2>/dev/null); then
          die "Invalid JSON in config file: $CONFIG_FILE"
      fi
  fi

  if [[ -n $OUTPUT_FILE ]]; then
      mkdir -p "$(dirname "$OUTPUT_FILE")"
      # Redirect all subsequent standard output to the file
      exec >"$OUTPUT_FILE"
      # Keep errors/help on stderr
      echo "Writing Prometheus metrics to $OUTPUT_FILE" >&2
  fi

  # (jq dependency already validated above with need jq)

  # Lightweight alias cycle detection (self + mutual) if config present
  if [[ -n $CONFIG_JSON ]]; then
    alias_edges=$(jq -c 'to_entries | map(select(.value.alias and .value.alias.name)) | map({from:.key,to:.value.alias.name})' <<<"$CONFIG_JSON")
    if [[ -n $alias_edges && $alias_edges != "[]" ]]; then
      self_cycle=$(jq -r '.[] | select(.from==.to) | .from' <<<"$alias_edges" || true)
      if [[ -n $self_cycle ]]; then die_code 8 "Alias self-cycle detected for metric: $self_cycle"; fi
      mutual=$(jq -r '[.[]] | combinations(2) | map(select((.[0].from==.[1].to) and (.[1].from==.[0].to))) | .[0][0].from + "," + .[0][1].from' <<<"$alias_edges" 2>/dev/null || true)
      if [[ -n $mutual ]]; then die_code 8 "Alias mutual cycle detected between: $mutual"; fi
    fi
  fi

  if [[ ! -f $LOG_FILE ]]; then
      die "Metrics file not found: $LOG_FILE"
  fi

  declare -A SEEN_METRICS
  # Prepare a safe JSON object for jq config arg even when CONFIG_JSON is empty
  local JQ_CONFIG_JSON='{}'
  if [[ -n $CONFIG_JSON ]]; then
    JQ_CONFIG_JSON="$CONFIG_JSON"
  fi

  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    [[ $line == \#* ]] && continue

    # Single jq invocation to extract needed fields + config/alias resolution & pre-escaped labels
    parsed=$(jq -r --argjson cfg "$JQ_CONFIG_JSON" '
      . as $o
      | if .==null then empty else . end
      | ( .metric // empty ) as $metric
      | ( ($o.type // "gauge") | ascii_downcase | sub("guage";"gauge") ) as $decl_type
      | ($cfg[$metric] // {}) as $cfg_entry
      | ($cfg_entry.alias.name // empty) as $alias_name
      | ($cfg_entry.alias.labels // {}) as $alias_labels
      | ($o.tags // {}) as $orig_tags
      | ($alias_labels + $orig_tags) as $merged_tags
      | ($cfg[$alias_name] // {}) as $alias_cfg_entry
      | ($cfg_entry.help // $alias_cfg_entry.help // "") as $help
      | ($cfg_entry.type // $alias_cfg_entry.type // $decl_type) as $resolved_type
      | ($help | gsub("\t";" ") | gsub("\r";" ") | gsub("\n";" ")) as $help_clean
      | ($merged_tags
          | to_entries
          | sort_by(.key)
          | map(.key + "=\"" +
                ( .value
                  | tostring
                  | gsub("\\\\";"\\\\")
                  | gsub("\""; "\\\"")
                  | gsub("\r"; "\\r")
                  | gsub("\t"; "\\t")
                  | gsub("\n"; "\\n")
                ) + "\"")
          | join(",")
        ) as $labels_str
      | [
          $metric,
          $alias_name,
          $help_clean,
          $decl_type,
          ($resolved_type|ascii_downcase),
          (if has("value") then (.value|tostring) else "" end),
          (.timestamp // ""),
          $labels_str,
          ((($cfg_entry.type // empty)!="" and ($alias_cfg_entry.type // empty)!="" and (($cfg_entry.type|ascii_downcase) != ($alias_cfg_entry.type|ascii_downcase))))
        ] | @tsv' <<<"$line" 2>/dev/null) || parsed=""

    if [[ -z $parsed ]]; then
      # Invalid JSON
      if $STRICT; then die_code 3 "Invalid JSON line encountered (strict mode)"; else ((SKIPPED_INVALID_JSON++)); continue; fi
    fi

    IFS=$'\t' read -r metric alias_name cfg_help decl_type resolved_type value ts labels type_conflict <<<"$parsed"

    # Basic required metric check
    if [[ -z $metric ]]; then
      if $STRICT; then die "Missing metric name (strict mode)"; else continue; fi
    fi

    # Regex filtering on original metric name
    if [[ -n $MATCH_REGEX && ! $metric =~ $MATCH_REGEX ]]; then
      ((SKIPPED_REGEX++))
      continue
    fi

    # Type conflict warning/error surfaced from jq output (boolean string true/false)
    if [[ $type_conflict == "true" ]]; then
      if $STRICT; then
        die "Type conflict for $metric -> $alias_name"
      else
        warn "Type conflict for $metric -> $alias_name"
      fi
    fi

    raw_type=$resolved_type
    metric_for_processing=${alias_name:-$metric}

    # Substitute default counter value if missing
    if [[ -z $value ]]; then
      if [[ $raw_type == count || $raw_type == counter ]]; then
        value=$DEFAULT_COUNTER_VALUE
      else
        if $STRICT; then die "Missing value for $metric (strict mode)"; else continue; fi
      fi
    fi

    # Validate numeric value after substitution
    if ! [[ $value =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
      if $STRICT; then
        die_code 3 "Non-numeric value for $metric (strict mode)"
      else
        warn "Non-numeric value for $metric (skipping)"
        continue
      fi
    fi

    # Normalize type -> Prometheus
    case "$raw_type" in
      count|counter) prom_type="counter" ;;
      gauge) prom_type="gauge" ;;
      *) prom_type="gauge" ;;
    esac

    norm_metric=${metric_for_processing//./_}
    metric_name=$norm_metric
    if [[ $prom_type == "counter" && ! $metric_name =~ _total$ ]]; then
      metric_name="${metric_name}_total"
    fi

    if [[ $prom_type == "gauge" && $metric_name =~ _total$ ]]; then
      $STRICT && die_code 4 "GAUGE metric '$metric_name' ends with _total" || warn "GAUGE metric '$metric_name' ends with _total"
    fi

    if [[ $metric_name =~ __ ]]; then
      warn "Metric name has consecutive underscores: $metric_name"
    fi
    if [[ $metric_name =~ ^[0-9] ]]; then
      warn "Metric name starts with a digit: $metric_name"
    fi
    metric_name=${metric_name//[[:space:]]/_}

    # Emit HELP/TYPE once (using cfg_help from jq, already sanitized for tabs/newlines)
    if ! $NO_META && [[ -z ${SEEN_METRICS[$metric_name]+x} ]]; then
      if [[ -n $cfg_help ]]; then
        esc_help=${cfg_help//\\/\\\\}
        esc_help=${esc_help//\"/\\\"}
        esc_help=${esc_help//$'\n'/\\n}
        esc_help=${esc_help//$'\r'/\\r}
        esc_help=${esc_help//$'\t'/\\t}
        echo "# HELP $metric_name $esc_help"
      else
        echo "# HELP $metric_name Metric $metric_name imported from StatsD log"
      fi
      echo "# TYPE $metric_name $prom_type"
      SEEN_METRICS[$metric_name]=1
    fi

    sample="$metric_name"
    [[ -n $labels ]] && sample+="{$labels}"
    sample+=" $value"

    if $WITH_TIMESTAMPS && [[ -n $ts && $ts =~ ^[0-9]+$ ]]; then
      if (( ts < MAX_FUTURE_TS_MS )); then
        ts_seconds=$(awk -v ms="$ts" 'BEGIN { printf "%.3f", ms/1000 }')
        sample+=" $ts_seconds"
      fi
    fi

    ((PROCESSED_COUNT++))
    echo "$sample"
    ((EMITTED_COUNT++))
  done < "$LOG_FILE" || true

  if ! $QUIET; then
    echo "Summary: processed=$PROCESSED_COUNT emitted=$EMITTED_COUNT skipped_invalid_json=$SKIPPED_INVALID_JSON skipped_regex=$SKIPPED_REGEX warnings=$WARN_COUNT" >&2
  fi
  if $FAIL_ON_WARN && (( WARN_COUNT > 0 )); then
    die_code 11 "Warnings present (count=$WARN_COUNT) and --fail-on-warn enabled"
  fi
}

# Argument parsing
POSITIONAL_FILE=""
while [[ $# -gt 0 ]]; do
    case $1 in
    --log-file)
      LOG_FILE="$2"; LOG_FILE_SET=true
      shift 2
      ;;
        --with-timestamps)
            WITH_TIMESTAMPS=true
            shift
            ;;
        --match)
            MATCH_REGEX="$2"
            shift 2
            ;;
        --strict)
            STRICT=true
            shift
            ;;
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
    --output)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --no-meta)
      NO_META=true
      shift
      ;;
    --quiet)
      QUIET=true
      shift
      ;;
    --fail-on-warn)
      FAIL_ON_WARN=true
      shift
      ;;
    --default-counter-value)
      DEFAULT_COUNTER_VALUE="$2"
      shift 2
      ;;
    --error-prefix)
      ERROR_PREFIX="$2"
      shift 2
      ;;
    --warn-prefix)
      WARN_PREFIX="$2"
      shift 2
      ;;
        --help|-h)
            print_help
            exit 0
            ;;
        --*)
            die "Unknown option: $1"
            ;;
        *)
            if [[ -z $POSITIONAL_FILE ]]; then
                POSITIONAL_FILE="$1"
            else
                warn "Multiple positional files provided; using first ($POSITIONAL_FILE) and ignoring '$1'"
            fi
            shift
            ;;
    esac
done

# Warn if both positional file and --log-file were specified (and differ)
if [[ -n ${POSITIONAL_FILE:-} && $LOG_FILE_SET == true && $POSITIONAL_FILE != "$LOG_FILE" ]]; then
  warn "Both positional metrics file ($POSITIONAL_FILE) and --log-file ($LOG_FILE) provided; using --log-file"
fi

# Additional argument validation / hardening
# Ensure default counter value is a non-negative number (int or float)
if ! [[ $DEFAULT_COUNTER_VALUE =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  die "Invalid --default-counter-value (must be non-negative number): $DEFAULT_COUNTER_VALUE"
fi

# Validate --match regex (if provided) using grep -E compile test
if [[ -n $MATCH_REGEX ]]; then
  if ! echo "" | grep -E "$MATCH_REGEX" >/dev/null 2>&1; then
    die "Invalid --match regex: $MATCH_REGEX"
  fi
fi

main "$@"