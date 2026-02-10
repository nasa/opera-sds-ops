"""Tests for duplicate detection logic."""

import pytest
from opera_accountability.duplicates import detect_duplicates


def test_detect_duplicates_with_no_data():
    """Test duplicate detection with empty data."""
    result = detect_duplicates([], 'DSWX_HLS')

    assert result['total'] == 0
    assert result['unique'] == 0
    assert result['duplicates'] == 0
    assert result['duplicate_list'] == []
    assert result['by_date'] == {}


def test_detect_duplicates_with_unique_granules():
    """Test duplicate detection when all granules are unique."""
    # Sample CMR response with unique granules
    cmr_granules = [
        {
            'umm': {
                'GranuleUR': 'OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0'
            }
        },
        {
            'umm': {
                'GranuleUR': 'OPERA_L3_DSWx-HLS_T10TEM_20260116T180931Z_20260116T235959Z_L8_30_v1.0'
            }
        },
        {
            'umm': {
                'GranuleUR': 'OPERA_L3_DSWx-HLS_T11SKA_20260115T183045Z_20260115T230000Z_S2A_30_v1.0'
            }
        }
    ]

    result = detect_duplicates(cmr_granules, 'DSWX_HLS')

    assert result['total'] == 3
    assert result['unique'] == 3
    assert result['duplicates'] == 0
    assert len(result['duplicate_list']) == 0


def test_detect_duplicates_with_duplicates():
    """Test duplicate detection when duplicates exist."""
    # Sample CMR response with duplicates (same tile_id, acquisition_ts, sensor)
    cmr_granules = [
        {
            'umm': {
                'GranuleUR': 'OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0'
            }
        },
        {
            'umm': {
                'GranuleUR': 'OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260116T003045Z_L8_30_v1.0'  # Duplicate!
            }
        },
        {
            'umm': {
                'GranuleUR': 'OPERA_L3_DSWx-HLS_T11SKA_20260115T183045Z_20260115T230000Z_S2A_30_v1.0'
            }
        }
    ]

    result = detect_duplicates(cmr_granules, 'DSWX_HLS')

    assert result['total'] == 3
    assert result['unique'] == 2
    assert result['duplicates'] == 1

    # The older creation timestamp should be marked as duplicate
    assert 'OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0' in result['duplicate_list']


def test_pattern_matching_dswx_hls():
    """Test that DSWX_HLS pattern extracts fields correctly."""
    import re
    from opera_accountability import CONFIG

    pattern = re.compile(CONFIG['products']['DSWX_HLS']['pattern'])
    granule_id = 'OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0'

    match = pattern.match(granule_id)
    assert match is not None

    fields = match.groupdict()
    assert fields['tile_id'] == 'T10TEM'
    assert fields['acquisition_ts'] == '20260115T180931Z'
    assert fields['creation_ts'] == '20260115T235959Z'
    assert fields['sensor'] == 'L8'


def test_pattern_matching_rtc_s1():
    """Test that RTC_S1 pattern extracts fields correctly."""
    import re
    from opera_accountability import CONFIG

    pattern = re.compile(CONFIG['products']['RTC_S1']['pattern'])
    granule_id = 'OPERA_L2_RTC-S1_T123-456789-IW1_20260115T180931Z_20260115T235959Z_S1A_30_v1.0'

    match = pattern.match(granule_id)
    assert match is not None

    fields = match.groupdict()
    assert fields['burst_id'] == 'T123-456789-IW1'
    assert fields['acquisition_ts'] == '20260115T180931Z'
    assert fields['creation_ts'] == '20260115T235959Z'
    assert fields['sensor'] == 'S1A'


# TODO: Add more test cases with fixtures once we create sample CMR responses
# - test_date_aggregation()
# - test_multiple_duplicates()
# - test_creation_timestamp_selection()
