"""Unit tests for DISP-S1 end conflict detection (Gerald's tool)."""

import pytest
from opera_accountability.duplicates import detect_disp_s1_end_conflicts, DISP_S1_END_CONFLICT_PATTERN


def test_disp_s1_pattern_matches_valid_ids():
    """Test that DISP_S1 pattern matches valid granule IDs (Gerald's pattern: VV|HH only)."""
    valid_ids = [
        'OPERA_L3_DISP-S1_IW_F09154_VV_20240101T000000Z_20240115T000000Z_v1.1_20240116T120000Z',
        'OPERA_L3_DISP-S1_IW_F08622_HH_20240201T000000Z_20240215T000000Z_v1.0_20240216T120000Z',
    ]
    for granule_id in valid_ids:
        assert DISP_S1_END_CONFLICT_PATTERN.match(granule_id) is not None


def test_disp_s1_pattern_rejects_invalid_ids():
    """Test that DISP_S1 pattern rejects invalid granule IDs."""
    invalid_ids = [
        'OPERA_L2_RTC-S1_T151-322284-IW1_20160701T005554Z_20240611T005333Z_S1A_30_v1.1',
        'OPERA_L3_DSWx-S1_T11TJM_20240101T000000Z_20240101T000000Z_S1A_30_v1.0',
        'NOT_A_GRANULE_ID',
    ]
    for granule_id in invalid_ids:
        assert DISP_S1_END_CONFLICT_PATTERN.match(granule_id) is None


def test_detect_end_conflicts_no_conflicts():
    """Test end conflict detection with no conflicts."""
    cmr_granules = [
        {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F09154_VV_20240101T000000Z_20240115T000000Z_v1.1_20240116T120000Z'}},
        {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F08622_HH_20240201T000000Z_20240215T000000Z_v1.0_20240216T120000Z'}},
    ]
    results = detect_disp_s1_end_conflicts(cmr_granules)
    assert results['total'] == 2
    assert results['conflict_groups'] == 0
    assert results['conflicting_products'] == 0
    assert len(results['conflicts']) == 0


def test_detect_end_conflicts_with_conflicts():
    """Test end conflict detection with actual conflicts (same frame+end, different begin).
    
    Gerald's grouping: (frame_id, end_dt) - no polarization included.
    """
    cmr_granules = [
        {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F09154_VV_20240101T000000Z_20240115T000000Z_v1.1_20240116T120000Z'}},
        {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F09154_VV_20240105T000000Z_20240115T000000Z_v1.0_20240116T130000Z'}},
        {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F09154_VV_20240110T000000Z_20240115T000000Z_v1.0_20240116T140000Z'}},
        {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F08622_HH_20240201T000000Z_20240215T000000Z_v1.0_20240216T120000Z'}},
    ]
    results = detect_disp_s1_end_conflicts(cmr_granules)
    assert results['total'] == 4
    assert results['conflict_groups'] == 1
    assert results['conflicting_products'] == 3
    assert len(results['conflicts']) == 1
    
    # Check the conflict details (Gerald's format: F{frame:05d}_{end_dt}, frame_id as int)
    conflict_key = 'F09154_20240115T000000Z'
    assert conflict_key in results['conflicts']
    conflict = results['conflicts'][conflict_key]
    assert conflict['frame_id'] == 9154  # Gerald stores as int, not string
    assert conflict['end_dt'] == '20240115T000000Z'
    assert len(conflict['begin_dts']) == 3
    assert '20240101T000000Z' in conflict['begin_dts']
    assert '20240105T000000Z' in conflict['begin_dts']
    assert '20240110T000000Z' in conflict['begin_dts']
    assert len(conflict['products']) == 3
    # Gerald's output includes production_times and versions
    assert 'production_times' in conflict
    assert 'versions' in conflict


def test_detect_end_conflicts_same_begin_no_conflict():
    """Test that products with same frame+end+begin are not conflicts."""
    cmr_granules = [
        {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F09154_VV_20240101T000000Z_20240115T000000Z_v1.1_20240116T120000Z'}},
        {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F09154_VV_20240101T000000Z_20240115T000000Z_v1.0_20240116T130000Z'}},
    ]
    results = detect_disp_s1_end_conflicts(cmr_granules)
    assert results['total'] == 2
    assert results['conflict_groups'] == 0
    assert results['conflicting_products'] == 0


def test_detect_end_conflicts_different_frames_no_conflict():
    """Test that products with different frames are not conflicts."""
    cmr_granules = [
        {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F09154_VV_20240101T000000Z_20240115T000000Z_v1.1_20240116T120000Z'}},
        {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F08622_VV_20240105T000000Z_20240115T000000Z_v1.0_20240116T130000Z'}},
    ]
    results = detect_disp_s1_end_conflicts(cmr_granules)
    assert results['total'] == 2
    assert results['conflict_groups'] == 0


def test_detect_end_conflicts_empty_input():
    """Test end conflict detection with empty input."""
    results = detect_disp_s1_end_conflicts([])
    assert results['total'] == 0
    assert results['conflict_groups'] == 0
    assert results['conflicting_products'] == 0
    assert results['parse_failures'] == 0


def test_detect_end_conflicts_parse_failures():
    """Test that parse failures are tracked."""
    cmr_granules = [
        {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F09154_VV_20240101T000000Z_20240115T000000Z_v1.1_20240116T120000Z'}},
        {'umm': {'GranuleUR': 'INVALID_GRANULE_ID'}},
        {'umm': {'GranuleUR': 'OPERA_L2_RTC-S1_T151-322284-IW1_20160701T005554Z_20240611T005333Z_S1A_30_v1.1'}},
    ]
    results = detect_disp_s1_end_conflicts(cmr_granules)
    assert results['total'] == 3
    assert results['parse_failures'] == 2
