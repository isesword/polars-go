#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PYTHON_BIN="${PYTHON_BIN:-}"
FIXTURE_DIR="${FIXTURE_DIR:-/tmp/polars-go-compare}"
SIZES="${SIZES:-4000,16000}"
GO_IMPORT_MODE="${GO_IMPORT_MODE:-json}"

CASES=(
  "query_collect_like"
  "to_maps"
  "scan_csv"
  "scan_parquet"
  "group_by_agg"
  "join_inner"
  "sink_ndjson_file"
)

usage() {
  cat <<EOF
Usage: scripts/compare_memory_with_python_polars.sh --python /path/to/python [--fixture-dir DIR] [--sizes 4000,16000] [--go-import-mode json|arrow]

Options:
  --python PATH        Python executable that can import polars.
  --fixture-dir DIR    Shared CSV/Parquet fixture directory. Default: /tmp/polars-go-compare
  --sizes LIST         Comma-separated row counts. Default: 4000,16000
  --go-import-mode M   Go in-memory import mode: json or arrow. Default: json
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python)
      PYTHON_BIN="$2"
      shift 2
      ;;
    --fixture-dir)
      FIXTURE_DIR="$2"
      shift 2
      ;;
    --sizes)
      SIZES="$2"
      shift 2
      ;;
    --go-import-mode)
      GO_IMPORT_MODE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$PYTHON_BIN" ]]; then
  echo "--python or PYTHON_BIN is required" >&2
  usage >&2
  exit 1
fi

echo "==> Preparing shared fixtures"
"$PYTHON_BIN" "$REPO_ROOT/scripts/compare_python_polars.py" \
  --prepare-only \
  --fixture-dir "$FIXTURE_DIR" \
  --sizes "$SIZES"

echo
echo "size,case,engine,us_per_op,maxrss_mib"
IFS=',' read -r -a SIZE_LIST <<< "$SIZES"
for size in "${SIZE_LIST[@]}"; do
  for case_name in "${CASES[@]}"; do
    py_result="$("$PYTHON_BIN" "$REPO_ROOT/scripts/compare_python_polars.py" \
      --fixture-dir "$FIXTURE_DIR" \
      --case "$case_name" \
      --size "$size" | tail -n 1)"
    py_us="$(echo "$py_result" | sed -E 's/.*us_per_op=([0-9]+).*/\1/')"
    py_mem="$(echo "$py_result" | sed -E 's/.*maxrss_mib=([0-9]+(\.[0-9]+)?).*/\1/')"
    echo "$size,$case_name,python,$py_us,$py_mem"

    go_result="$(cd "$REPO_ROOT" && go run ./scripts/compare_go_polars.go \
      --fixture-dir "$FIXTURE_DIR" \
      --import-mode "$GO_IMPORT_MODE" \
      --case "$case_name" \
      --size "$size" | tail -n 1)"
    go_us="$(echo "$go_result" | sed -E 's/.*us_per_op=([0-9]+).*/\1/')"
    go_mem="$(echo "$go_result" | sed -E 's/.*maxrss_mib=([0-9]+(\.[0-9]+)?).*/\1/')"
    echo "$size,$case_name,go,$go_us,$go_mem"
  done
done
