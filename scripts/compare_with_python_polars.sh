#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PYTHON_BIN="${PYTHON_BIN:-}"
FIXTURE_DIR="${FIXTURE_DIR:-/tmp/polars-go-compare}"
SIZES="${SIZES:-4000,16000}"

usage() {
  cat <<EOF
Usage: scripts/compare_with_python_polars.sh --python /path/to/python [--fixture-dir DIR] [--sizes 4000,16000]

Options:
  --python PATH        Python executable that can import polars.
  --fixture-dir DIR    Shared CSV/Parquet fixture directory. Default: /tmp/polars-go-compare
  --sizes LIST         Comma-separated row counts. Default: 4000,16000

Environment:
  PYTHON_BIN           Same as --python
  FIXTURE_DIR          Same as --fixture-dir
  SIZES                Same as --sizes
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
echo "==> Python Polars"
"$PYTHON_BIN" "$REPO_ROOT/scripts/compare_python_polars.py" \
  --fixture-dir "$FIXTURE_DIR" \
  --sizes "$SIZES"

echo
echo "==> Go bridge"
(
  cd "$REPO_ROOT"
  go run ./scripts/compare_go_polars.go \
    --fixture-dir "$FIXTURE_DIR" \
    --sizes "$SIZES"
)
