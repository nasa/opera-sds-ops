#!/usr/bin/env bash
# Launches the opera-launchpad interactive CLI.
#
# On startup, the CLI checks every configured tool's cloned repo against its
# remote branch and warns you if anything is out of date or not yet cloned,
# with instructions to run `./setup.sh --update`.
#
# Usage:
#   ./start.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR=".venv"

if [ ! -x "$VENV_DIR/bin/python" ]; then
    echo "No .venv found. Run ./setup.sh first."
    exit 1
fi

exec "$VENV_DIR/bin/python" cli.py
