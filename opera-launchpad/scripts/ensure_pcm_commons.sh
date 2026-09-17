#!/usr/bin/env bash
# Clones and installs `pcm_commons` — a private JPL GitHub Enterprise repo —
# into a tool's own venv, the same way opera-sds-pcm's own docker/Dockerfile
# does it. Needed by data_subscriber.es_conn_util (imported transitively by
# cmr_audit_disp_s1.py's DISP-S1 accountability validator), which only works
# with live access to the OPERA SDS's internal Elasticsearch/GRQ cluster
# anyway — i.e. from an SDS cluster node/VM with that access, not a personal
# laptop.
#
# Requires:
#   GIT_OAUTH_TOKEN     - a JPL GitHub Enterprise personal access token with
#                         read access to IEMS-SDS/pcm_commons. Generate one
#                         at https://github.jpl.nasa.gov/settings/tokens
#   PCM_COMMONS_BRANCH  - branch/tag to check out (default: 3.1.2, matching
#                         the version opera-sds-pcm's own setup.py pins)
#
# Usage (run from the tool's own working directory, as a post_setup step):
#   $OPERA_LAUNCHPAD_ROOT/scripts/ensure_pcm_commons.sh [path-to-pip]
set -uo pipefail

if [ -z "${GIT_OAUTH_TOKEN:-}" ]; then
    echo "ERROR: GIT_OAUTH_TOKEN is not set." >&2
    echo "pcm_commons is a private JPL GitHub Enterprise repo. Generate a" >&2
    echo "personal access token (read access to IEMS-SDS/pcm_commons) at" >&2
    echo "https://github.jpl.nasa.gov/settings/tokens, then:" >&2
    echo "    export GIT_OAUTH_TOKEN=<your token>" >&2
    echo "and re-run setup." >&2
    exit 1
fi

detect_pip() {
    if [ -x "venv/bin/pip" ]; then
        echo "venv/bin/pip"
    elif [ -x "venv/Scripts/pip.exe" ]; then
        echo "venv/Scripts/pip.exe"
    else
        echo "venv/bin/pip"
    fi
}

PIP="${1:-$(detect_pip)}"
BRANCH="${PCM_COMMONS_BRANCH:-3.1.2}"
CLONE_DIR=".pcm_commons"

rm -rf "$CLONE_DIR"
git clone "https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/pcm_commons.git" "$CLONE_DIR"
(cd "$CLONE_DIR" && git checkout "$BRANCH")
"$PIP" install -e "$CLONE_DIR"
