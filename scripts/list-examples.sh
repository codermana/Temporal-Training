#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Explanation snippets (path includes the language subfolder where present):"
find "$ROOT_DIR/examples" -maxdepth 3 -type f \
  ! -path "$ROOT_DIR/examples/runnable/*" \
  ! -name "README.md" \
  | sed "s#^$ROOT_DIR/examples/##" \
  | sort

echo
echo "Runnable Maven examples:"
find "$ROOT_DIR/examples/runnable" -mindepth 1 -maxdepth 1 -type d \
  | sed "s#^$ROOT_DIR/examples/runnable/##" \
  | sort

