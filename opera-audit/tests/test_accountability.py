"""Tests for accountability analysis logic.

This test suite is organized to support future product expansion.
Currently tests DSWX_HLS accountability, but structured to easily
add tests for other products (RTC_S1, CSLC_S1, etc.) when supported.
"""

import pytest
from datetime import datetime
from opera_accountability import accountability
from opera_accountability.accountability import analyze_accountability


@pytest.fixture(autouse=True)
def reset_l9_cutoff():
    """Reset L9_CUTOFF between tests to ensure clean state."""
    accountability.L9_CUTOFF = None
    yield
    accountability.L9_CUTOFF = None


# ============================================================================
# Test Fixtures - Helper functions to create mock CMR data
# ============================================================================

def create_hls_granule(granule_id, acquisition_time, platform="LANDSAT-8"):
    """Create a mock HLS granule in CMR format."""
    return {
        'umm': {
            'GranuleUR': granule_id,
            'TemporalExtent': {
                'RangeDateTime': {
                    'BeginningDateTime': acquisition_time
                }
            },
            'Platforms': [{'ShortName': platform}]
        }
    }


def create_dswx_granule(granule_id, input_granules):
    """Create a mock DSWx-HLS granule in CMR format."""
    return {
        'umm': {
            'GranuleUR': granule_id,
            'InputGranules': input_granules
        }
    }


# ============================================================================
# DSWX_HLS Accountability Tests
# ============================================================================

class TestDSWXHLSAccountability:
    """Core accountability tests for DSWX_HLS product."""

    def test_empty_data(self):
        """Test with no granules."""
        result = analyze_accountability([], [])

        assert result['expected'] == 0
        assert result['actual'] == 0
        assert result['missing_count'] == 0
        assert result['missing'] == []

    def test_perfect_accountability(self):
        """Test when all HLS inputs have DSWx outputs (100% accountability)."""
        hls_granules = [
            create_hls_granule('HLS.S30.T10TEM.2026001T183821.v2.0', '2026-01-01T18:38:21+00:00'),
            create_hls_granule('HLS.L30.T10TEM.2026001T183821.v2.0', '2026-01-01T18:38:21+00:00')
        ]

        dswx_granules = [
            create_dswx_granule(
                'OPERA_L3_DSWx-HLS_T10TEM_20260101T183821Z_20260103T120000Z_S2A_30_v1.0',
                ['HLS.S30.T10TEM.2026001T183821.v2.0.B02.tif']
            ),
            create_dswx_granule(
                'OPERA_L3_DSWx-HLS_T10TEM_20260101T183821Z_20260103T120000Z_L8_30_v1.0',
                ['HLS.L30.T10TEM.2026001T183821.v2.0.B02.tif']
            )
        ]

        result = analyze_accountability(dswx_granules, hls_granules)

        assert result['expected'] == 2
        assert result['actual'] == 2
        assert result['missing_count'] == 0

    def test_missing_outputs(self):
        """Test when some HLS inputs have no DSWx outputs."""
        hls_granules = [
            create_hls_granule('HLS.S30.T10TEM.2026001T183821.v2.0', '2026-01-01T18:38:21+00:00'),
            create_hls_granule('HLS.L30.T10TEM.2026001T183821.v2.0', '2026-01-01T18:38:21+00:00')
        ]

        # Only one DSWx output
        dswx_granules = [
            create_dswx_granule(
                'OPERA_L3_DSWx-HLS_T10TEM_20260101T183821Z_20260103T120000Z_S2A_30_v1.0',
                ['HLS.S30.T10TEM.2026001T183821.v2.0.B02.tif']
            )
        ]

        result = analyze_accountability(dswx_granules, hls_granules)

        assert result['expected'] == 2
        assert result['actual'] == 1
        assert result['missing_count'] == 1
        assert 'HLS.L30.T10TEM.2026001T183821.v2.0' in result['missing']

    def test_hls_band_files_grouped(self):
        """Test that multiple band files from same HLS granule are treated as one input."""
        hls_granules = [
            create_hls_granule('HLS.S30.T10TEM.2026001T183821.v2.0', '2026-01-01T18:38:21+00:00')
        ]

        dswx_granules = [
            create_dswx_granule(
                'OPERA_L3_DSWx-HLS_T10TEM_20260101T183821Z_20260103T120000Z_S2A_30_v1.0',
                [
                    'HLS.S30.T10TEM.2026001T183821.v2.0.B02.tif',
                    'HLS.S30.T10TEM.2026001T183821.v2.0.B03.tif',
                    'HLS.S30.T10TEM.2026001T183821.v2.0.Fmask.tif'
                ]
            )
        ]

        result = analyze_accountability(dswx_granules, hls_granules)

        # All bands should map to the same HLS granule
        assert result['expected'] == 1
        assert result['actual'] == 1
        assert result['missing_count'] == 0

    def test_non_hls_inputs_ignored(self):
        """Test that non-HLS inputs (worldcover, GSHHS) are ignored."""
        hls_granules = [
            create_hls_granule('HLS.S30.T10TEM.2026001T183821.v2.0', '2026-01-01T18:38:21+00:00')
        ]

        dswx_granules = [
            create_dswx_granule(
                'OPERA_L3_DSWx-HLS_T10TEM_20260101T183821Z_20260103T120000Z_S2A_30_v1.0',
                [
                    'HLS.S30.T10TEM.2026001T183821.v2.0.B02.tif',
                    'worldcover_0.tif',
                    'GSHHS_f_L1.shp'
                ]
            )
        ]

        result = analyze_accountability(dswx_granules, hls_granules)

        # Only HLS input should count
        assert result['expected'] == 1
        assert result['actual'] == 1


class TestDSWXHLSLandsat9Filtering:
    """Tests for DSWX_HLS Landsat-9 cutoff date filtering.

    L9 granules before the cutoff date are excluded from accountability
    because they were not expected to produce DSWx outputs.
    """

    def test_l9_cutoff_configuration(self):
        """Test that L9 cutoff date is properly configured."""
        # Trigger parsing by calling analyze_accountability
        analyze_accountability([], [])

        # Verify L9_CUTOFF was set correctly
        assert accountability.L9_CUTOFF is not None
        assert accountability.L9_CUTOFF.year == 2025
        assert accountability.L9_CUTOFF.month == 10
        # Must be timezone-aware for comparison with CMR timestamps
        assert accountability.L9_CUTOFF.tzinfo is not None

    def test_l9_before_cutoff_excluded(self):
        """Test that L9 granules before cutoff are excluded from accountability."""
        hls_granules = [
            create_hls_granule(
                'HLS.L30.T10TEM.2025274T183821.v2.0',
                '2025-09-30T18:38:21+00:00',  # Before cutoff
                platform='LANDSAT-9'
            )
        ]

        result = analyze_accountability([], hls_granules)

        # Should be filtered out, so expected = 0
        assert result['expected'] == 0

    def test_l9_after_cutoff_included(self):
        """Test that L9 granules after cutoff are included in accountability."""
        hls_granules = [
            create_hls_granule(
                'HLS.L30.T10TEM.2026001T183821.v2.0',
                '2026-01-01T18:38:21+00:00',  # After cutoff
                platform='LANDSAT-9'
            )
        ]

        result = analyze_accountability([], hls_granules)

        # Should be included
        assert result['expected'] == 1
        assert result['missing_count'] == 1

    def test_l8_and_sentinel_never_filtered(self):
        """Test that L8 and Sentinel-2 are never filtered regardless of date."""
        hls_granules = [
            create_hls_granule(
                'HLS.L30.T10TEM.2025001T183821.v2.0',
                '2025-01-01T18:38:21+00:00',  # Before L9 cutoff
                platform='LANDSAT-8'
            ),
            create_hls_granule(
                'HLS.S30.T10TEM.2025001T183821.v2.0',
                '2025-01-01T18:38:21+00:00',  # Before L9 cutoff
                platform='SENTINEL-2A'
            )
        ]

        result = analyze_accountability([], hls_granules)

        # Both should be included
        assert result['expected'] == 2


# ============================================================================
# Future Product Tests - Placeholder structure
# ============================================================================
# When accountability is implemented for other products, add test classes here:
#
# class TestRTCS1Accountability:
#     """Accountability tests for RTC_S1 product."""
#     pass
#
# class TestCSLCS1Accountability:
#     """Accountability tests for CSLC_S1 product."""
#     pass
#
# class TestDSWXS1Accountability:
#     """Accountability tests for DSWX_S1 product."""
#     pass
#
# class TestDISPS1Accountability:
#     """Accountability tests for DISP_S1 product."""
#     pass
