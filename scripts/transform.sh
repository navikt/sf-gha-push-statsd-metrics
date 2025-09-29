#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# transform.sh
# -----------------------------------------------------------------------------
# Convert newline-delimited JSON StatsD-like metrics into Prometheus exposition
# format, enriching with metadata & alias information from a config JSON.
#
# Key Features
#   * Metric name dots -> underscores (Prometheus convention)
#   * Typo "guage" normalized to "gauge"
#   * Counters automatically get _total suffix if missing
#   * Optional alias remaps a source metric into a unified family w/ injected labels
#   * Emits HELP/TYPE once per metric family (unless --no-meta)
#   * Optional timestamp output (ms -> seconds) with --with-timestamps
#   * Regex filtering of ORIGINAL metric name via --match <regex>
#   * Strict mode: fail fast on malformed JSON, missing values, conflicts, etc.
#   * Exit codes distinguish different failure classes (see help)
#   * Debug mode (--debug) prints per-line jq transform output to stderr
# -----------------------------------------------------------------------------

set -euo pipefail
umask 077

# ----------------------------- Defaults / Globals ----------------------------
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
QUIET=false
DEBUG=false
readonly MAX_FUTURE_TS_MS=32503680000000   # sanity upper bound (~ year 3000)
LOG_FILE_SET=false
WARN_COUNT=0
PROCESSED_COUNT=0
EMITTED_COUNT=0
SKIPPED_INVALID_JSON=0
SKIPPED_REGEX=0
DEFAULT_COUNTER_VALUE=1

# ----------------------------- Helper Functions ------------------------------
print_help() {
  cat <<'EOF'
Usage: ./scripts/transform.sh [metrics_file] [options]

If metrics_file (positional) is omitted, defaults to logs/metrics.log.
If config/metrics_config.json exists (or --config <file> provided), its metadata
(type, help, alias) overrides the raw input.

Options:
  --log-file <file>            Metrics log file (overrides positional)
  --with-timestamps            Include timestamps (converted ms -> seconds)
  --match <regex>              Include only metrics whose ORIGINAL name matches regex
  --strict                     Fail fast on malformed input / conflicts
  --config <file>              JSON metadata (type/help + optional alias mapping)
  --output <file>              Write exposition output to file (creates dirs)
  --no-meta                    Suppress HELP/TYPE prelude lines
  --quiet                      Suppress warnings & summary
  --fail-on-warn               Exit code 11 if any warnings occurred
  --error-prefix <str>         Prefix for error messages (default: 'ERROR: ')
  --warn-prefix <str>          Prefix for warning messages (default: 'WARN: ')
  --default-counter-value <n>  Value used when counter missing 'value' (default: 1)
  --debug                      Emit per-line jq transformed row (TSV) to stderr
  --help                       Show this help and exit

Exit Codes:
  0  Success
  1  Generic/config error
  3  Invalid JSON / non-numeric / missing required data (strict)
  4  Gauge named with _total suffix (strict)
  8  Alias cycle detected
  11 Warnings encountered (with --fail-on-warn)
  12 Empty transform output for a JSON line (strict)
EOF
}

warn() { 
  local msg="$1"
  if [[ $msg != Summary:* && $msg != "# Summary:"* ]]; then 
    ((WARN_COUNT++))
  fi
  $QUIET || echo "${WARN_PREFIX}${msg}" >&2
}

die() { 
  echo "${ERROR_PREFIX}$*" >&2
  exit 1
}

die_code() { 
  local c=$1
  shift
  echo "${ERROR_PREFIX}$*" >&2
  exit "$c"
}

need() { 
  command -v "$1" >/dev/null || die "Missing dependency '$1'"
}

# ------------------------------ Arg Parsing ----------------------------------
POSITIONAL_FILE=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --log-file)             LOG_FILE="$2"; LOG_FILE_SET=true; shift 2;;
    --with-timestamps)      WITH_TIMESTAMPS=true; shift;;
    --match)                MATCH_REGEX="$2"; shift 2;;
    --strict)               STRICT=true; shift;;
    --config)               CONFIG_FILE="$2"; shift 2;;
    --output)               OUTPUT_FILE="$2"; shift 2;;
    --no-meta)              NO_META=true; shift;;
    --quiet)                QUIET=true; shift;;
    --fail-on-warn)         FAIL_ON_WARN=true; shift;;
    --default-counter-value)DEFAULT_COUNTER_VALUE="$2"; shift 2;;
    --error-prefix)         ERROR_PREFIX="$2"; shift 2;;
    --warn-prefix)          WARN_PREFIX="$2"; shift 2;;
    --debug)                DEBUG=true; shift;;
    --help|-h)              print_help; exit 0;;
    --*)                    die "Unknown option: $1";;
    *)                      
      if [[ -z $POSITIONAL_FILE ]]; then 
        POSITIONAL_FILE="$1"
      else 
        warn "Multiple positional files; using first ($POSITIONAL_FILE), ignoring '$1'"
      fi
      shift;;
  esac
done

if [[ -n ${POSITIONAL_FILE:-} && $LOG_FILE_SET == true && $POSITIONAL_FILE != "$LOG_FILE" ]]; then
  warn "Both positional ($POSITIONAL_FILE) and --log-file ($LOG_FILE) provided; preferring --log-file"
fi

# ------------------------------ Validation -----------------------------------
need jq
need awk

[[ -z $LOG_FILE ]] && LOG_FILE="logs/metrics.log"

# Auto-discover config file if not specified: prefer root metrics_config.json, then config/metrics_config.json
if [[ -z $CONFIG_FILE ]]; then
  if [[ -f metrics_config.json ]]; then
    CONFIG_FILE="metrics_config.json"
  elif [[ -f config/metrics_config.json ]]; then
    CONFIG_FILE="config/metrics_config.json"
  fi
fi

if [[ -n $CONFIG_FILE ]]; then
  [[ -f $CONFIG_FILE ]] || die "Config file not found: $CONFIG_FILE"
  CONFIG_JSON=$(jq -c '.' "$CONFIG_FILE" 2>/dev/null) || die "Invalid JSON in config file: $CONFIG_FILE"
fi

if ! [[ $DEFAULT_COUNTER_VALUE =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  die "Invalid --default-counter-value: $DEFAULT_COUNTER_VALUE"
fi

if [[ -n $MATCH_REGEX ]]; then
  echo "" | grep -E "$MATCH_REGEX" >/dev/null 2>&1 || die "Invalid --match regex: $MATCH_REGEX"
fi

[[ -f $LOG_FILE ]] || die "Metrics file not found: $LOG_FILE"

if [[ -n $OUTPUT_FILE ]]; then
  mkdir -p "$(dirname "$OUTPUT_FILE")"
  exec >"$OUTPUT_FILE"
  echo "Writing Prometheus metrics to $OUTPUT_FILE" >&2
fi

# -------------------------- Alias Cycle Detection ----------------------------
if [[ -n $CONFIG_JSON ]]; then
  alias_edges=$(jq -c 'to_entries | map(select(.value.alias and .value.alias.name)) | map({from:.key,to:.value.alias.name})' <<<"$CONFIG_JSON")
  if [[ -n $alias_edges && $alias_edges != "[]" ]]; then
    self_cycle=$(jq -r '.[] | select(.from==.to) | .from' <<<"$alias_edges" || true)
    [[ -n $self_cycle ]] && die_code 8 "Alias self-cycle detected: $self_cycle"
    
    mutual=$(jq -r '[.[]] | combinations(2) | map(select((.[0].from==.[1].to) and (.[1].from==.[0].to))) | .[0][0].from + "," + .[0][1].from' <<<"$alias_edges" 2>/dev/null || true)
    [[ -n $mutual ]] && die_code 8 "Alias mutual cycle: $mutual"
  fi
fi

# ---------------------------- Processing Loop --------------------------------
declare -A SEEN_METRICS
JQ_CFG='{}'
[[ -n $CONFIG_JSON ]] && JQ_CFG="$CONFIG_JSON"

while IFS= read -r line; do
  [[ -z $line ]] && continue
  [[ $line == \#* ]] && continue

  # Raw pre-filter by original metric name regex (cheap): extract "metric" value
  if [[ -n $MATCH_REGEX ]]; then
    if ! [[ $line =~ \"metric\"\s*:\s*\"([^\"]+)\" ]]; then
      ((SKIPPED_INVALID_JSON++))
      continue
    fi
    orig_name_candidate="${BASH_REMATCH[1]}"
    if ! [[ $orig_name_candidate =~ $MATCH_REGEX ]]; then
      ((SKIPPED_REGEX++))
      continue
    fi
  fi

  jq_out=""
  if ! jq_out=$(jq -r --argjson cfg "$JQ_CFG" '
      . as $o
      | (.metric // empty) as $metric
      | select($metric != "")
      | ($cfg[$metric] // {}) as $cfg_entry
      | ($cfg_entry.alias.name // $metric) as $family
      | ($cfg_entry.alias.labels // {}) as $alias_labels
      | ($cfg[$family] // {}) as $family_cfg
      | ($cfg_entry.help // $family_cfg.help // "") as $help
      | ($cfg_entry.type // $family_cfg.type // ($o.type // "gauge")) as $resolved_type
      | ($cfg_entry.type // empty) as $source_type
      | ($family_cfg.type // empty) as $family_type
      | ($o.tags // {}) as $orig_tags
      | ($alias_labels + $orig_tags) as $merged_tags
      | ($merged_tags
          | to_entries
          | sort_by(.key)
          | map(.key+"=\""+(
              .value
              | tostring
              | gsub("\\\\";"\\\\")
              | gsub("\"";"\\\"")
              | gsub("\n";"\\n")
              | gsub("\r";"\\r")
              | gsub("\t";"\\t")
            )+"\"")
          | join(",")
        ) as $labels_raw
      | ($labels_raw // "") as $labels
      | (( ($source_type//empty)!="" and ($family_type//empty)!="" and ($source_type|ascii_downcase)!=( $family_type|ascii_downcase))) as $conflict
      | [ $metric
          , $family
          , $help
          , ($resolved_type|ascii_downcase)
          , (if has("value") then (.value|tostring) else "" end)
          , (.timestamp // "")
          , $labels
          , $conflict
        ]
      | @tsv' <<<"$line" 2>/dev/null); then
    if $STRICT; then 
      die_code 3 "Invalid JSON (jq parse failure) line: $line"
    else 
      ((SKIPPED_INVALID_JSON++))
      continue
    fi
  fi

  if [[ -z $jq_out ]]; then
    if $STRICT; then 
      die_code 12 "Empty transform output line: $line"
    else 
      warn "Empty transform output (skipping)"
      continue
    fi
  fi
  
  $DEBUG && echo "DEBUG: jq parsed line -> $jq_out" >&2

  IFS=$'\t' read -r orig_metric family help resolved_type value ts labels type_conflict <<<"$jq_out"
  [[ -z $orig_metric ]] && { 
    if $STRICT; then 
      die "Missing metric name (strict)"
    else 
      continue
    fi
  }

  # Fill default counter value
  if [[ -z $value ]]; then
    if [[ $resolved_type == count || $resolved_type == counter ]]; then
      value=$DEFAULT_COUNTER_VALUE
    else
      if $STRICT; then 
        die "Missing value for $orig_metric (strict)"
      else 
        continue
      fi
    fi
  fi

  # Validate numeric
  if ! [[ $value =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    if $STRICT; then 
      die_code 3 "Non-numeric value for $orig_metric"
    else 
      warn "Non-numeric value for $orig_metric (skipping)"
      continue
    fi
  fi

  # Type conflict
  if [[ $type_conflict == true || $type_conflict == "true" ]]; then
    if $STRICT; then 
      die "Type conflict for $orig_metric -> $family"
    else 
      warn "Type conflict for $orig_metric -> $family"
    fi
  fi

  # Normalize type
  case "$resolved_type" in
    count|counter) prom_type=counter ;;
    gauge)         prom_type=gauge ;;
    *)             prom_type=gauge ;;
  esac

  metric_name=${family//./_}
  if [[ $prom_type == counter && ! $metric_name =~ _total$ ]]; then
    metric_name+="_total"
  fi
  if [[ $prom_type == gauge && $metric_name =~ _total$ ]]; then
    if $STRICT; then
      die_code 4 "GAUGE metric '$metric_name' ends with _total"
    else
      warn "GAUGE metric '$metric_name' ends with _total"
    fi
  fi
  
  [[ $metric_name =~ __ ]] && warn "Metric name has consecutive underscores: $metric_name"
  [[ $metric_name =~ ^[0-9] ]] && warn "Metric name starts with a digit: $metric_name"
  metric_name=${metric_name//[[:space:]]/_}

  # HELP/TYPE emission
  if ! $NO_META && [[ -z ${SEEN_METRICS[$metric_name]+x} ]]; then
    if [[ -n $help ]]; then
      esc_help=${help//\\/\\\\}
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

  # Build sample line (skip literal 'false' as label artifact)
  sample="$metric_name"
  if [[ -n $labels && $labels != "false" ]]; then
    sample+="{$labels}"
  fi
  sample+=" $value"

  if $WITH_TIMESTAMPS && [[ -n $ts && $ts =~ ^[0-9]+$ && $ts -lt $MAX_FUTURE_TS_MS ]]; then
    ts_sec=$(awk -v ms="$ts" 'BEGIN{printf "%.3f", ms/1000}')
    sample+=" $ts_sec"
  fi

  ((PROCESSED_COUNT++))
  echo "$sample"
  ((EMITTED_COUNT++))

done < "$LOG_FILE" || true

# ------------------------------ Summary / Exit -------------------------------
if ! $QUIET; then
  echo "Summary: processed=$PROCESSED_COUNT emitted=$EMITTED_COUNT skipped_invalid_json=$SKIPPED_INVALID_JSON skipped_regex=$SKIPPED_REGEX warnings=$WARN_COUNT" >&2
fi

if $FAIL_ON_WARN && (( WARN_COUNT > 0 )); then
  die_code 11 "Warnings present (count=$WARN_COUNT) and --fail-on-warn enabled"
fi

exit 0