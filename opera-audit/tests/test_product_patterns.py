"""Unit tests for product pattern matching (Phase 1: Riley's Tools).

Tests that the regex patterns for the 4 additional products ported from
Riley's duplicate_check.py correctly parse valid granule IDs.
"""

import pytest
import re
from opera_accountability import CONFIG

# Load patterns from config
PRODUCT_PATTERNS = {
    "TROPO": CONFIG["products"]["TROPO"]["pattern"],
    "DIST_ALERT_HLS": CONFIG["products"]["DIST_ALERT_HLS"]["pattern"],
    "CSLC_S1_STATIC": CONFIG["products"]["CSLC_S1_STATIC"]["pattern"],
    "RTC_S1_STATIC": CONFIG["products"]["RTC_S1_STATIC"]["pattern"],
}


class TestTROPOPattern:
    """Tests for TROPO product pattern matching."""
    
    def test_valid_tropo_id_matches(self):
        """Test that valid TROPO granule IDs match the pattern."""
        pattern = re.compile(PRODUCT_PATTERNS["TROPO"])
        valid_ids = [
            "OPERA_L4_TROPO-ZENITH_20240101T000000Z_20240101T120000Z_ERA5_v1.0",
            "OPERA_L4_TROPO-ZENITH_20240101T000000Z_20240101T120000Z_GPT_v1.0",
        ]
        for granule_id in valid_ids:
            assert pattern.match(granule_id) is not None
    
    def test_tropo_extract_groups(self):
        """Test that TROPO pattern extracts expected groups."""
        pattern = re.compile(PRODUCT_PATTERNS["TROPO"])
        granule_id = "OPERA_L4_TROPO-ZENITH_20240101T000000Z_20240101T120000Z_ERA5_v1.0"
        match = pattern.match(granule_id)
        assert match is not None
        groups = match.groupdict()
        assert groups["acquisition_ts"] == "20240101T000000Z"
        assert groups["creation_ts"] == "20240101T120000Z"
        assert groups["model"] == "ERA5"


class TestDISTAlertHLSPattern:
    """Tests for DIST-ALERT-HLS product pattern matching."""
    
    def test_valid_dist_alert_hls_id_matches(self):
        """Test that valid DIST-ALERT-HLS granule IDs match the pattern."""
        pattern = re.compile(PRODUCT_PATTERNS["DIST_ALERT_HLS"])
        valid_ids = [
            "OPERA_L3_DIST-ALERT-HLS_T10TEM_20240101T000000Z_20240101T120000Z_S2A_30_v1.0",
            "OPERA_L3_DIST-ALERT-HLS_T10TEM_20240101T000000Z_20240101T120000Z_L8_30_v1.0",
        ]
        for granule_id in valid_ids:
            assert pattern.match(granule_id) is not None
    
    def test_dist_alert_hls_extract_groups(self):
        """Test that DIST-ALERT-HLS pattern extracts expected groups."""
        pattern = re.compile(PRODUCT_PATTERNS["DIST_ALERT_HLS"])
        granule_id = "OPERA_L3_DIST-ALERT-HLS_T10TEM_20240101T000000Z_20240101T120000Z_S2A_30_v1.0"
        match = pattern.match(granule_id)
        assert match is not None
        groups = match.groupdict()
        assert groups["tile_id"] == "T10TEM"
        assert groups["acquisition_ts"] == "20240101T000000Z"
        assert groups["creation_ts"] == "20240101T120000Z"
        assert groups["sensor"] == "S2A"


class TestCSLCS1StaticPattern:
    """Tests for CSLC-S1-STATIC product pattern matching."""
    
    def test_valid_cslc_s1_static_id_matches(self):
        """Test that valid CSLC-S1-STATIC granule IDs match the pattern."""
        pattern = re.compile(PRODUCT_PATTERNS["CSLC_S1_STATIC"])
        valid_ids = [
            "OPERA_L2_CSLC-S1-STATIC_T123-456789-IW1_20240101_S1A_v1.0",
            "OPERA_L2_CSLC-S1-STATIC_T123-456789-IW2_20240101_S1B_v1.0",
        ]
        for granule_id in valid_ids:
            assert pattern.match(granule_id) is not None
    
    def test_cslc_s1_static_extract_groups(self):
        """Test that CSLC-S1-STATIC pattern extracts expected groups."""
        pattern = re.compile(PRODUCT_PATTERNS["CSLC_S1_STATIC"])
        granule_id = "OPERA_L2_CSLC-S1-STATIC_T123-456789-IW1_20240101_S1A_v1.0"
        match = pattern.match(granule_id)
        assert match is not None
        groups = match.groupdict()
        assert groups["burst_id"] == "T123-456789-IW1"
        assert groups["validity_ts"] == "20240101"
        assert groups["sensor"] == "S1A"


class TestRTCS1StaticPattern:
    """Tests for RTC-S1-STATIC product pattern matching."""
    
    def test_valid_rtc_s1_static_id_matches(self):
        """Test that valid RTC-S1-STATIC granule IDs match the pattern."""
        pattern = re.compile(PRODUCT_PATTERNS["RTC_S1_STATIC"])
        valid_ids = [
            "OPERA_L2_RTC-S1-STATIC_T123-456789-IW1_20240101_S1A_30_v1.0",
            "OPERA_L2_RTC-S1-STATIC_T123-456789-IW2_20240101_S1B_30_v1.0",
        ]
        for granule_id in valid_ids:
            assert pattern.match(granule_id) is not None
    
    def test_rtc_s1_static_extract_groups(self):
        """Test that RTC-S1-STATIC pattern extracts expected groups."""
        pattern = re.compile(PRODUCT_PATTERNS["RTC_S1_STATIC"])
        granule_id = "OPERA_L2_RTC-S1-STATIC_T123-456789-IW1_20240101_S1A_30_v1.0"
        match = pattern.match(granule_id)
        assert match is not None
        groups = match.groupdict()
        assert groups["burst_id"] == "T123-456789-IW1"
        assert groups["validity_ts"] == "20240101"
        assert groups["sensor"] == "S1A"
