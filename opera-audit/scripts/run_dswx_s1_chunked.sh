#!/usr/bin/env bash
#
# Run DSWx-S1 accountability over a long time range by splitting into
# 1-day chunks. Each day is invoked as a separate `opera-audit accountability`
# call so intermediate results are checkpointed to disk and CMR/ES memory usage
# stays bounded.
#
# At the end, this script builds a combined final DSWx-S1 report by:
#   1. loading every daily rtc_survey.json and dswx_survey.json
#   2. globally deduping RTC-S1 and DSWx-S1 records by configured unique fields
#   3. recomputing RTC -> DSWx mapping
#   4. recomputing missing RTCs
#   5. recomputing MGRS tile sets and cycle indices
#
# Usage:
#   scripts/run_dswx_s1_daily.sh [START YYYY-MM-DD] [END YYYY-MM-DD]
#
# Defaults:
#   START = 2024-08-28
#   END   = 2026-07-01
#
# Optional environment variables:
#   VENUE        = PROD or UAT, default PROD
#   OPERA_MGRS_DB = path to MGRS tile collection sqlite
#
# Output layout:
#   scripts/output/opera_dswx_s1_daily_<timestamp>/
#     day_001_2024-08-28_to_2024-08-29/
#     day_002_2024-08-29_to_2024-08-30/
#     ...
#     combined/
#       reports/accountability/DSWX_S1/<date>/
#         rtc_survey.json
#         dswx_survey.json
#         missing_rtc_products.json
#         rtc_to_dswx_map.json
#         missing_rtcs_to_tile_sets.json
#         missing_mgrs_set_cycle_indices.json
#         summary.json
#         summary.txt
#         combine_manifest.json

set -euo pipefail

START_DATE="${1:-2024-08-28}"
END_DATE="${2:-2026-07-01}"
VENUE="${VENUE:-PROD}"

# Locate repo root
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

if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "ERROR: python not found at ${VENV_PYTHON}" >&2
    exit 1
fi

if [[ ! -f "${OPERA_MGRS_DB}" ]]; then
    echo "ERROR: MGRS DB not found at ${OPERA_MGRS_DB}" >&2
    exit 1
fi

TS="$(date +%Y%m%d_%H%M%S)"
BASE_DIR="${SCRIPT_DIR}/output/opera_dswx_s1_daily_${TS}"
COMBINED_DIR="${BASE_DIR}/combined"

mkdir -p "${BASE_DIR}"
mkdir -p "${COMBINED_DIR}"

echo "=================================================================="
echo "DSWx-S1 daily accountability"
echo "  range        : ${START_DATE} .. ${END_DATE}"
echo "  chunk size   : 1 day"
echo "  venue        : ${VENUE}"
echo "  base out dir : ${BASE_DIR}"
echo "  combined dir : ${COMBINED_DIR}"
echo "  MGRS DB      : ${OPERA_MGRS_DB}"
echo "=================================================================="

DAY_FILE="$(mktemp -t opera_dswx_s1_days.XXXXXX)"
trap 'rm -f "${DAY_FILE}"' EXIT

"${VENV_PYTHON}" - > "${DAY_FILE}" <<PY
from datetime import date, timedelta

start = date.fromisoformat("${START_DATE}")
end = date.fromisoformat("${END_DATE}")

cursor = start
while cursor < end:
    nxt = min(cursor + timedelta(days=1), end)
    print(f"{cursor.isoformat()} {nxt.isoformat()}")
    cursor = nxt
PY

TOTAL=$(wc -l < "${DAY_FILE}" | tr -d ' ')

if (( TOTAL == 0 )); then
    echo "No days to run because start >= end."
    exit 0
fi

echo "Planned ${TOTAL} day chunk(s):"
sed 's/^/  /' "${DAY_FILE}"
echo

FAILED=()
idx=0

while read -r day_start day_end; do
    idx=$((idx + 1))

    printf -v pad_idx '%03d' "${idx}"
    day_dir="${BASE_DIR}/day_${pad_idx}_${day_start}_to_${day_end}"

    mkdir -p "${day_dir}"

    echo "------------------------------------------------------------------"
    echo "[${idx}/${TOTAL}] DSWx-S1 accountability ${day_start} -> ${day_end}"
    echo "     output : ${day_dir}"
    echo "------------------------------------------------------------------"

    set +e
    "${OPERA_AUDIT_BIN}" accountability DSWX_S1 \
        --start "${day_start}" \
        --end "${day_end}" \
        --venue "${VENUE}" \
        --save \
        --output-dir "${day_dir}" \
        --mgrs-db "${OPERA_MGRS_DB}"
    rc=$?
    set -e

    if [[ "${rc}" -eq 0 ]]; then
        echo "     status : OK"
    else
        echo "     status : FAILED (exit ${rc}) -- continuing"
        FAILED+=("${day_start}..${day_end}")
    fi

done < "${DAY_FILE}"

echo "=================================================================="
echo "Daily runs complete."
echo "Results under: ${BASE_DIR}"
echo "=================================================================="

if (( ${#FAILED[@]} > 0 )); then
    echo "${#FAILED[@]} day chunk(s) failed:"
    printf '  %s\n' "${FAILED[@]}"
    echo
    echo "Skipping final combine because some days failed."
    exit 1
fi

echo "Combining daily results (streaming dedupe)..."
echo "  input  : ${BASE_DIR}/day_*"
echo "  output : ${COMBINED_DIR}"
echo

"${VENV_PYTHON}" "${SCRIPT_DIR}/combine_dswx_s1.py" \
    "${BASE_DIR}" \
    --combined-dir "${COMBINED_DIR}" \
    --start "${START_DATE}" \
    --end "${END_DATE}" \
    --venue "${VENUE}" \
    --mgrs-db "${OPERA_MGRS_DB}"

echo
echo "=================================================================="
echo "All ${TOTAL} day chunk(s) completed successfully."
echo "Combined final report under: ${COMBINED_DIR}"
echo "=================================================================="