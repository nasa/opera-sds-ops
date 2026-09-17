#!/usr/bin/env bash
# Finds a Python 3.12 interpreter and echoes its path to stdout. 3.12 is the
# one version that satisfies both sides of a real constraint conflict for
# tools built on opera-sds-pcm's [cmr_audit] extras *plus* the hysds
# package (e.g. DISP_S1.audit):
#   - opera-sds-pcm's setup.py declares `python_requires=">=3.12"`, so
#     anything older makes its own base install unresolvable (pip
#     backtracks endlessly rather than failing cleanly).
#   - hysds pins old dependencies (lxml<5.0.0, gevent, etc.) that have no
#     prebuilt wheels for very new Python (3.13+) and fail to compile from
#     source there (their C extensions use CPython internal APIs removed
#     in newer versions) — but do still have cp312 wheels.
# So this deliberately does not fall back to older (3.9-3.11) or newer
# (3.13+) interpreters even if found; it isn't a "nearest available"
# search, just "isn't 3.12 installed under any of these common names".
#
# Usage: PYBIN=$($OPERA_LAUNCHPAD_ROOT/scripts/pick_legacy_python.sh)
set -uo pipefail

version_of() {
    "$1" -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")' 2>/dev/null
}

for candidate in python3.12 /opt/homebrew/bin/python3.12 /usr/local/bin/python3.12 /usr/bin/python3.12; do
    path="$(command -v "$candidate" 2>/dev/null || true)"
    if [ -z "$path" ] && [ -x "$candidate" ]; then
        path="$candidate"
    fi
    [ -z "$path" ] && continue

    if [ "$(version_of "$path")" = "3.12" ]; then
        echo "$path"
        exit 0
    fi
done

echo "ERROR: No Python 3.12 interpreter found." >&2
echo "This tool needs exactly Python 3.12: opera-sds-pcm's setup.py requires" >&2
echo ">=3.12, while hysds's own pins (lxml<5.0.0, gevent, etc.) have no" >&2
echo "prebuilt wheels for 3.13+ and fail to compile from source there." >&2
echo "Install it (e.g. 'brew install python@3.12' on macOS, 'apt install" >&2
echo "python3.12' on Linux, or via pyenv/conda) and re-run setup." >&2
exit 1
