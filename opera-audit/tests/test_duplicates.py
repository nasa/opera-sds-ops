"""Tests for duplicate detection logic."""

import pytest
from opera_accountability.duplicates import detect_duplicates


def test_detect_duplicates_with_no_data():
    """Test duplicate detection with empty data."""
    result = detect_duplicates([], "DSWX_HLS")

    assert result["total"] == 0
    assert result["unique"] == 0
    assert result["duplicates"] == 0
    assert result["duplicate_list"] == []
    assert result["by_date"] == {}


def test_detect_duplicates_with_unique_granules():
    """Test duplicate detection when all granules are unique."""
    # Sample CMR response with unique granules
    cmr_granules = [
        {
            "umm": {
                "GranuleUR": "OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0"
            }
        },
        {
            "umm": {
                "GranuleUR": "OPERA_L3_DSWx-HLS_T10TEM_20260116T180931Z_20260116T235959Z_L8_30_v1.0"
            }
        },
        {
            "umm": {
                "GranuleUR": "OPERA_L3_DSWx-HLS_T11SKA_20260115T183045Z_20260115T230000Z_S2A_30_v1.0"
            }
        }
    ]

    result = detect_duplicates(cmr_granules, "DSWX_HLS")

    assert result["total"] == 3
    assert result["unique"] == 3
    assert result["duplicates"] == 0
    assert len(result["duplicate_list"]) == 0


def test_detect_duplicates_with_duplicates():
    """Test duplicate detection when duplicates exist."""
    # Sample CMR response with duplicates (same tile_id, acquisition_ts, sensor)
    cmr_granules = [
        {
            "umm": {
                "GranuleUR": "OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0"
            }
        },
        {
            "umm": {
                "GranuleUR": "OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260116T003045Z_L8_30_v1.0"  # Duplicate!
            }
        },
        {
            "umm": {
                "GranuleUR": "OPERA_L3_DSWx-HLS_T11SKA_20260115T183045Z_20260115T230000Z_S2A_30_v1.0"
            }
        }
    ]

    result = detect_duplicates(cmr_granules, "DSWX_HLS")

    assert result["total"] == 3
    assert result["unique"] == 2
    assert result["duplicates"] == 1

    # The older creation timestamp should be marked as duplicate
    assert "OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0" in result["duplicate_list"]


def test_pattern_matching_dswx_hls():
    """Test that DSWX_HLS pattern extracts fields correctly."""
    import re
    from opera_accountability import CONFIG

    pattern = re.compile(CONFIG["products"]["DSWX_HLS"]["pattern"])
    granule_id = "OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0"

    match = pattern.match(granule_id)
    assert match is not None

    fields = match.groupdict()
    assert fields["tile_id"] == "T10TEM"
    assert fields["acquisition_ts"] == "20260115T180931Z"
    assert fields["creation_ts"] == "20260115T235959Z"
    assert fields["sensor"] == "L8"


def test_pattern_matching_rtc_s1():
    """Test that RTC_S1 pattern extracts fields correctly."""
    import re
    from opera_accountability import CONFIG

    pattern = re.compile(CONFIG["products"]["RTC_S1"]["pattern"])
    granule_id = "OPERA_L2_RTC-S1_T123-456789-IW1_20260115T180931Z_20260115T235959Z_S1A_30_v1.0"

    match = pattern.match(granule_id)
    assert match is not None

    fields = match.groupdict()
    assert fields["burst_id"] == "T123-456789-IW1"
    assert fields["acquisition_ts"] == "20260115T180931Z"
    assert fields["creation_ts"] == "20260115T235959Z"
    assert fields["sensor"] == "S1A"


# TODO: Add more test cases with fixtures once we create sample CMR responses
# - test_date_aggregation()
# - test_multiple_duplicates()
# - test_creation_timestamp_selection()


# ---------------------------------------------------------------------------
# Pattern-matching tests for products ported from Riley's duplicate_check.py
# (TROPO, DIST_ALERT_HLS, CSLC_S1_STATIC, RTC_S1_STATIC)
# ---------------------------------------------------------------------------


def _compiled_pattern(product: str):
    import re
    from opera_accountability import CONFIG
    return re.compile(CONFIG["products"][product]["pattern"])


def test_pattern_matching_tropo():
    """TROPO pattern extracts acquisition_ts, creation_ts, and model."""
    pattern = _compiled_pattern("TROPO")
    granule_id = "OPERA_L4_TROPO-ZENITH_20260115T180931Z_20260115T235959Z_HRES_v1.0"

    match = pattern.match(granule_id)
    assert match is not None

    fields = match.groupdict()
    assert fields["acquisition_ts"] == "20260115T180931Z"
    assert fields["creation_ts"] == "20260115T235959Z"
    assert fields["model"] == "HRES"


def test_pattern_matching_dist_alert_hls():
    """DIST_ALERT_HLS pattern extracts tile_id, timestamps, and sensor."""
    pattern = _compiled_pattern("DIST_ALERT_HLS")
    granule_id = "OPERA_L3_DIST-ALERT-HLS_T10TEM_20260115T180931Z_20260115T235959Z_S2A_30_v1.0"

    match = pattern.match(granule_id)
    assert match is not None

    fields = match.groupdict()
    assert fields["tile_id"] == "T10TEM"
    assert fields["acquisition_ts"] == "20260115T180931Z"
    assert fields["creation_ts"] == "20260115T235959Z"
    assert fields["sensor"] == "S2A"


def test_pattern_matching_cslc_s1_static():
    """CSLC_S1_STATIC pattern extracts burst_id, validity date, and sensor."""
    pattern = _compiled_pattern("CSLC_S1_STATIC")
    granule_id = "OPERA_L2_CSLC-S1-STATIC_T123-456789-IW1_20260115_S1A_v1.0"

    match = pattern.match(granule_id)
    assert match is not None

    fields = match.groupdict()
    assert fields["burst_id"] == "T123-456789-IW1"
    assert fields["validity_ts"] == "20260115"
    assert fields["sensor"] == "S1A"


def test_pattern_matching_rtc_s1_static():
    """RTC_S1_STATIC pattern extracts burst_id, validity date, and sensor."""
    pattern = _compiled_pattern("RTC_S1_STATIC")
    granule_id = "OPERA_L2_RTC-S1-STATIC_T123-456789-IW1_20260115_S1A_30_v1.0"

    match = pattern.match(granule_id)
    assert match is not None

    fields = match.groupdict()
    assert fields["burst_id"] == "T123-456789-IW1"
    assert fields["validity_ts"] == "20260115"
    assert fields["sensor"] == "S1A"


# ---------------------------------------------------------------------------
# Duplicate-detection behavior for the newly ported products
# ---------------------------------------------------------------------------


def test_duplicates_tropo_selects_latest_creation():
    """TROPO duplicates should be resolved by creation_ts (latest wins)."""
    cmr_granules = [
        {"umm": {"GranuleUR": "OPERA_L4_TROPO-ZENITH_20260115T180931Z_20260115T235959Z_HRES_v1.0"}},
        # Same acquisition_ts + model → duplicate; later creation_ts should win.
        {"umm": {"GranuleUR": "OPERA_L4_TROPO-ZENITH_20260115T180931Z_20260116T003045Z_HRES_v1.0"}},
        {"umm": {"GranuleUR": "OPERA_L4_TROPO-ZENITH_20260116T180931Z_20260116T235959Z_HRES_v1.0"}},
    ]

    result = detect_duplicates(cmr_granules, "TROPO")

    assert result["total"] == 3
    assert result["unique"] == 2
    assert result["duplicates"] == 1
    # The older creation timestamp is the duplicate.
    assert ("OPERA_L4_TROPO-ZENITH_20260115T180931Z_20260115T235959Z_HRES_v1.0"
            in result["duplicate_list"])


def test_duplicates_dist_alert_hls_all_unique():
    cmr_granules = [
        {"umm": {"GranuleUR":
                 "OPERA_L3_DIST-ALERT-HLS_T10TEM_20260115T180931Z_20260115T235959Z_S2A_30_v1.0"}},
        {"umm": {"GranuleUR":
                 "OPERA_L3_DIST-ALERT-HLS_T10TEM_20260116T180931Z_20260116T235959Z_S2A_30_v1.0"}},
        {"umm": {"GranuleUR":
                 "OPERA_L3_DIST-ALERT-HLS_T11SKA_20260115T183045Z_20260115T230000Z_L8_30_v1.0"}},
    ]

    result = detect_duplicates(cmr_granules, "DIST_ALERT_HLS")

    assert result["total"] == 3
    assert result["unique"] == 3
    assert result["duplicates"] == 0


def test_duplicates_cslc_s1_static_no_creation_field():
    """
    Static products have no creation_field. When the same (burst_id, validity_ts,
    sensor) repeats, every occurrence after the first is flagged as a duplicate
    (mirrors Riley's duplicate_check.py behavior for static products).
    """
    cmr_granules = [
        {"umm": {"GranuleUR":
                 "OPERA_L2_CSLC-S1-STATIC_T001-000001-IW1_20260115_S1A_v1.0"}},
        {"umm": {"GranuleUR":
                 "OPERA_L2_CSLC-S1-STATIC_T001-000001-IW1_20260115_S1A_v1.0"}},  # duplicate
        {"umm": {"GranuleUR":
                 "OPERA_L2_CSLC-S1-STATIC_T001-000002-IW1_20260115_S1A_v1.0"}},
    ]

    result = detect_duplicates(cmr_granules, "CSLC_S1_STATIC")

    assert result["total"] == 3
    assert result["unique"] == 2
    assert result["duplicates"] == 1


def test_duplicates_rtc_s1_static_date_aggregation():
    """RTC_S1_STATIC uses %Y%m%d validity_ts; by_date should bucket on that date."""
    cmr_granules = [
        {"umm": {"GranuleUR":
                 "OPERA_L2_RTC-S1-STATIC_T001-000001-IW1_20260115_S1A_30_v1.0"}},
        {"umm": {"GranuleUR":
                 "OPERA_L2_RTC-S1-STATIC_T001-000002-IW1_20260115_S1A_30_v1.0"}},
        {"umm": {"GranuleUR":
                 "OPERA_L2_RTC-S1-STATIC_T001-000001-IW1_20260116_S1A_30_v1.0"}},
    ]

    result = detect_duplicates(cmr_granules, "RTC_S1_STATIC")

    assert result["total"] == 3
    assert result["unique"] == 3
    assert result["duplicates"] == 0
    assert set(result["by_date"].keys()) == {"2026-01-15", "2026-01-16"}
    assert result["by_date"]["2026-01-15"]["total"] == 2
    assert result["by_date"]["2026-01-16"]["total"] == 1


def test_new_products_registered_in_config():
    """All 4 ported products should appear in the loaded CONFIG."""
    from opera_accountability import CONFIG

    for product in ("TROPO", "DIST_ALERT_HLS", "CSLC_S1_STATIC", "RTC_S1_STATIC"):
        assert product in CONFIG["products"], f"{product} missing from config.yaml"
        prod_cfg = CONFIG["products"][product]
        assert prod_cfg["ccid"]["PROD"], f"{product} missing PROD CCID"
        assert prod_cfg["pattern"], f"{product} missing pattern"
        assert prod_cfg["unique_fields"], f"{product} missing unique_fields"
        assert prod_cfg["aggregation_field"], f"{product} missing aggregation_field"
        assert prod_cfg["aggregation_format"], f"{product} missing aggregation_format"


def test_grq_index_configured_for_key_products():
    """Products with GRQ support should have grq_index configured."""
    from opera_accountability import CONFIG

    grq_products = [
        "DSWX_HLS", "RTC_S1", "CSLC_S1", "DSWX_S1", "DIST_S1",
        "DISP_S1", "TROPO",
    ]
    for product in grq_products:
        assert product in CONFIG["products"], f"{product} missing"
        grq_index = CONFIG["products"][product].get("grq_index")
        assert grq_index, f"{product} missing grq_index"
        assert grq_index.startswith("grq_"), f"{product} grq_index should start with 'grq_'"


def test_get_granules_from_grq_requires_opensearchpy():
    """get_granules_from_grq raises ImportError when opensearchpy is missing."""
    from unittest.mock import patch
    from opera_accountability.duplicates import get_granules_from_grq

    with patch.dict("sys.modules", {"opensearchpy": None, "opensearchpy.helpers": None}):
        with pytest.raises(ImportError, match="opensearchpy"):
            get_granules_from_grq(
                grq_url="https://grq.example.com",
                index="grq_*_l3_dswx_hls-*",
                product="DSWX_HLS",
            )


def test_get_granules_from_grq_with_mock():
    """get_granules_from_grq returns correctly shaped dicts from mocked scan."""
    from unittest.mock import patch, MagicMock
    from opera_accountability.duplicates import get_granules_from_grq

    mock_hits = [
        {"_id": "id1", "_source": {"metadata": {"FileName": "OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0"}}},
        {"_id": "id2", "_source": {"metadata": {"FileName": "OPERA_L3_DSWx-HLS_T10TEM_20260116T180931Z_20260116T235959Z_L8_30_v1.0"}}},
    ]

    mock_opensearchpy = MagicMock()
    mock_scan = MagicMock(return_value=iter(mock_hits))

    with patch.dict("sys.modules", {
        "opensearchpy": mock_opensearchpy,
        "opensearchpy.helpers": MagicMock(scan=mock_scan),
    }):
        # Re-import to get fresh module with mocked dependencies
        import importlib
        import opera_accountability.duplicates as dup_mod
        importlib.reload(dup_mod)

        result = dup_mod.get_granules_from_grq(
            grq_url="https://grq.example.com",
            index="grq_*_l3_dswx_hls-*",
            product="DSWX_HLS",
        )

        assert len(result) == 2
        assert result[0]["umm"]["GranuleUR"] == mock_hits[0]["_source"]["metadata"]["FileName"]
        assert result[1]["umm"]["GranuleUR"] == mock_hits[1]["_source"]["metadata"]["FileName"]

    # Reload original module to restore state
    importlib.reload(dup_mod)
