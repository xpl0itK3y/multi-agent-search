#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${VENV_PYTHON:-$ROOT_DIR/venv/bin/python}"
MANIFEST_PATH="$ROOT_DIR/native/text_processing/Cargo.toml"

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "Python interpreter not found at $VENV_PYTHON"
  echo "Set VENV_PYTHON=/path/to/python and retry."
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required to build the native module."
  exit 1
fi

if ! "$VENV_PYTHON" -m pip show maturin >/dev/null 2>&1; then
  echo "maturin is not installed in the selected Python environment."
  echo "Install it with:"
  echo "  $VENV_PYTHON -m pip install maturin"
  exit 1
fi

echo "Building native text-processing module from $MANIFEST_PATH"
"$VENV_PYTHON" -m maturin develop --release --manifest-path "$MANIFEST_PATH"
echo "Native module build finished."
