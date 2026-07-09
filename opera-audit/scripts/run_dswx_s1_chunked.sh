#!/usr/bin/env bash
#
# Run DSWx-S1 accountability over a long time range by splitting into
# 3-month chunks. Each chunk is invoked as a separate `opera-audit accountability`
# call so intermediate results are checkpointed to disk and CMR/ES memory usage
# stays bounded.
#
# Usage:
#   scripts/run_dswx_s1_chunked.sh [START YYYY-MM-DD] [END YYYY-MM-DD] [CHUNK_MONTHS]
#
# Defaults:
#   START        = 2024-08-28
#   END          = 2026-07-01
#   CHUNK_MONTHS = 1
#
# Output layout (auto-created inside this scripts/ folder):
#   scripts/output/opera_dswx_s1_chunked_<timestamp>/
#     chunk_01_2024-08-28_to_2024-11-28/   <-- opera-audit --output-dir for chunk 1
#     chunk_02_2024-11-28_to_2025-02-28/
#     ...
#
# Requirements:
#   - opera-audit installed in the venv at opera-audit/.venv
#   - OPERA_MGRS_DB env var pointing at the MGRS collection sqlite
#     (defaults to accountability_tools/dswx_s1/MGRS_tile_collection_v0.3.sqlite
#      relative to the opera-sds-ops repo root)

set -euo pipefail

START_DATE="${1:-2024-08-28}"
END_DATE="${2:-2026-07-01}"
CHUNK_MONTHS="${3:-1}"

# Locate repo root (parent of opera-audit/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUDIT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${AUDIT_ROOT}/.." && pwd)"

OPERA_AUDIT_BIN="${AUDIT_ROOT}/.venv/bin/opera-audit"
VENV_PYTHON="${AUDIT_ROOT}/.venv/bin/python"
: "${OPERA_MGRS_DB:=${REPO_ROOT}/accountability_tools/dswx_s1/MGRS_tile_collection_v0.3.sqlite}"
export OPERA_MGRS_DB

if [[ ! -x "${OPERA_AUDIT_BIN}" ]]; then
    echo "ERROR: opera-audit not found at ${OPERA_AUDIT_BIN}" >&2
    exit 1
fi
if [[ ! -f "${OPERA_MGRS_DB}" ]]; then
    echo "ERROR: MGRS DB not found at ${OPERA_MGRS_DB}" >&2
    exit 1
fi

# Create a fresh base output directory inside scripts/output/
TS="$(date +%Y%m%d_%H%M%S)"
BASE_DIR="${SCRIPT_DIR}/output/opera_dswx_s1_chunked_${TS}"
mkdir -p "${BASE_DIR}"

echo "=================================================================="
echo "DSWx-S1 chunked accountability"
echo "  range        : ${START_DATE} .. ${END_DATE}"
echo "  chunk size   : ${CHUNK_MONTHS} month(s)"
echo "  base out dir : ${BASE_DIR}"
echo "  MGRS DB      : ${OPERA_MGRS_DB}"
echo "=================================================================="

# Build the list of (chunk_start, chunk_end) pairs with pure Python for
# calendar-correct month arithmetic (macOS date has no portable +Nmonths).
# NOTE: we avoid `mapfile` because macOS ships bash 3.2 which does not support it.
CHUNK_FILE="$(mktemp -t opera_dswx_s1_chunks.XXXXXX)"
trap 'rm -f "${CHUNK_FILE}"' EXIT

"${VENV_PYTHON}" - > "${CHUNK_FILE}" <<PY
from datetime import date
from dateutil.relativedelta import relativedelta

start = date.fromisoformat("${START_DATE}")
end   = date.fromisoformat("${END_DATE}")
step  = relativedelta(months=${CHUNK_MONTHS})

cursor = start
while cursor < end:
    nxt = min(cursor + step, end)
    print(f"{cursor.isoformat()} {nxt.isoformat()}")
    cursor = nxt
PY

TOTAL=$(wc -l < "${CHUNK_FILE}" | tr -d ' ')
if (( TOTAL == 0 )); then
    echo "No chunks to run (start >= end)."
    exit 0
fi

echo "Planned ${TOTAL} chunk(s):"
sed 's/^/  /' "${CHUNK_FILE}"
echo

FAILED=()
idx=0
while read -r chunk_start chunk_end; do
    idx=$((idx + 1))
    # Zero-pad chunk index for stable lexical sort in listings.
    printf -v pad_idx '%02d' "${idx}"
    chunk_dir="${BASE_DIR}/chunk_${pad_idx}_${chunk_start}_to_${chunk_end}"
    mkdir -p "${chunk_dir}"

    echo "------------------------------------------------------------------"
    echo "[${idx}/${TOTAL}] DSWx-S1 accountability  ${chunk_start} -> ${chunk_end}"
    echo "     output : ${chunk_dir}"
    echo "------------------------------------------------------------------"

    # Let opera-audit's stdout/stderr flow directly to the terminal.
    set +e
    "${OPERA_AUDIT_BIN}" accountability DSWX_S1 \
            --start "${chunk_start}" \
            --end "${chunk_end}" \
            --save \
            --output-dir "${chunk_dir}" \
            --mgrs-db "${OPERA_MGRS_DB}"
    rc=$?
    set -e

    if [[ "${rc}" -eq 0 ]]; then
        echo "     status : OK"
    else
        echo "     status : FAILED (exit ${rc}) -- continuing"
        FAILED+=("${chunk_start}..${chunk_end}")
    fi
done < "${CHUNK_FILE}"

echo "=================================================================="
echo "Results under: ${BASE_DIR}"
if (( ${#FAILED[@]} == 0 )); then
    echo "All ${TOTAL} chunk(s) completed successfully."
else
    echo "${#FAILED[@]} chunk(s) failed:"
    printf '  %s\n' "${FAILED[@]}"
    exit 1
fi
