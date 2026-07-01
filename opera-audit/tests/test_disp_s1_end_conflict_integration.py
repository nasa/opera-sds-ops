"""Integration test for DISP-S1 end-conflict detection.

Compares opera-audit's end-conflict detection against an independent CMR
analysis that re-implements Gerald's algorithm using the shared
``DISP_S1_END_CONFLICT_PATTERN``.

Marked as ``integration`` / ``slow`` because it hits live CMR.
"""

from datetime import datetime

import pytest

from opera_accountability import CONFIG
from opera_accountability.cmr import query_cmr
from opera_accountability.duplicates import detect_disp_s1_end_conflicts
from test_cmr_integration import (
    query_cmr_directly,
    analyze_disp_s1_end_conflicts_from_cmr,
)


@pytest.mark.integration
@pytest.mark.slow
def test_disp_s1_end_conflict_detection_matches_cmr():
    """DISP-S1 end-conflict counts and conflict sets should match CMR analysis.

    Uses a narrow PROD window for runtime reasons; update the date range if
    production coverage changes significantly.
    """
    start_str = "2026-02-01"
    end_str = "2026-02-02"

    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")

    ccid = CONFIG["products"]["DISP_S1"]["ccid"]["PROD"]

    # Opera-audit end-conflict detection on the standard CMR client.
    cmr_granules = query_cmr(ccid, start_date, end_date, "PROD")
    opera_results = detect_disp_s1_end_conflicts(cmr_granules)

    # Independent CMR analysis using the raw HTTP client + analyzer helper.
    cmr_granules_direct = query_cmr_directly(ccid, start_str, end_str)
    cmr_results = analyze_disp_s1_end_conflicts_from_cmr(cmr_granules_direct)

    # Summary counts must match exactly.
    assert opera_results["total"] == cmr_results["total"]
    assert (
        opera_results["conflict_groups"]
        == cmr_results["conflict_groups"]
    )
    assert (
        opera_results["conflicting_products"]
        == cmr_results["conflicting_products"]
    )

    # Conflict keys and their begin/product sets should match (order-insensitive).
    assert set(opera_results["conflicts"].keys()) == set(
        cmr_results["conflicts"].keys()
    )
    for key in opera_results["conflicts"]:
        op_conf = opera_results["conflicts"][key]
        cm_conf = cmr_results["conflicts"][key]
        assert set(op_conf["begin_dts"]) == set(cm_conf["begin_dts"])
        assert set(op_conf["products"]) == set(cm_conf["products"])
