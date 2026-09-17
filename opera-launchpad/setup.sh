#!/usr/bin/env bash
# Sets up opera-launchpad's own dependencies (in a local .venv) and runs
# bootstrap.py to clone/install every configured tool.
#
# Usage:
#   ./setup.sh              # install deps + bootstrap everything
#   ./setup.sh --update     # also fetch/reset already-cloned repos
#   ./setup.sh --product DSWX_S1 --tool accountability

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR=".venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "==> Creating virtual environment ($VENV_DIR)"
    python3 -m venv "$VENV_DIR"
fi

echo "==> Installing opera-launchpad dependencies"
"$VENV_DIR/bin/pip" install --quiet --upgrade pip
"$VENV_DIR/bin/pip" install --quiet -e .

echo "==> Running bootstrap.py $*"
"$VENV_DIR/bin/python" bootstrap.py "$@"

echo ""
echo "==> Bootstrap finished. Launch the interactive menu with:"
echo "    ./start.sh"
