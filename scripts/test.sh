#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

OS=$(uname -s | tr '[:upper:]' '[:lower:]')

case "$OS" in
  linux*)
    LIB_NAME="libpolars_bridge.so"
    ;;
  darwin*)
    LIB_NAME="libpolars_bridge.dylib"
    ;;
  *)
    echo "Unsupported OS: $OS"
    exit 1
    ;;
esac

export POLARS_BRIDGE_LIB="$SCRIPT_DIR/../$LIB_NAME"

go test "$SCRIPT_DIR/../polars/" -v -count=1
