"""Integration tests comparing opera-audit results with direct CMR queries.

These tests verify that opera-audit's duplicate detection and accountability
analysis match independent CMR analysis. Test parameters (dates and expected
counts) are configurable to allow updates as production issues are corrected.

IMPORTANT: If tests fail, verify whether production data has changed:
- Duplicates may have been removed from CMR
- Missing products may have been produced
- Update TEST_CASES with current production state if needed

ADDING NEW TESTS:
Simply add new entries to TEST_CASES dict below. Tests will be automatically
generated via pytest parametrize. No additional code changes needed.

PRODUCT SUPPORT:
- Duplicate tests: Support ALL products (DSWX_HLS, RTC_S1, CSLC_S1, etc.)
- Accountability tests: Currently only DSWX_HLS supported
"""

import re
import pytest
import requests
from datetime import datetime, timezone
from collections import defaultdict
from os.path import basename

from opera_accountability.cmr import query_cmr
from opera_accountability.duplicates import detect_duplicates
from opera_accountability.accountability import analyze_accountability
from opera_accountability import CONFIG


# =============================================================================
# TEST CONFIGURATION
# =============================================================================
# Update these values if production data changes (duplicates removed, missing products created)
# To add new tests, simply add new entries here - no other code changes needed!

TEST_CASES = {
    # Duplicate detection tests
    'DUPLICATES': {
        'DSWX_HLS_2026_02_06': {
            'product': 'DSWX_HLS',
            'start_date': '2026-02-06',
            'end_date': '2026-02-07',
            'expected_duplicates': 39,
            'tolerance': 0,  # Exact match required
            'description': 'DSWx-HLS duplicate detection for 2026-02-06'
        },
        'CSLC_S1_2026_02_06': {
            'product': 'CSLC_S1',
            'start_date': '2026-02-06',
            'end_date': '2026-02-07',
            'expected_duplicates': 714,
            'tolerance': 0,  # Exact match required
            'description': 'CSLC-S1 duplicate detection for 2026-02-06'
        },
        # Add more duplicate test cases here as needed:
        # 'RTC_S1_SOME_DATE': {
        #     'product': 'RTC_S1',
        #     'start_date': '2026-01-15',
        #     'end_date': '2026-01-16',
        #     'expected_duplicates': 25,
        #     'tolerance': 0,
        #     'description': 'RTC-S1 duplicate detection for 2026-01-15'
        # },
    },

    # Accountability tests (currently only DSWX_HLS supported)
    'ACCOUNTABILITY': {
        'DSWX_HLS_2026_02_06': {
            'start_date': '2026-02-06',
            'end_date': '2026-02-07',
            'expected_missing': 164,
            'tolerance': 0,  # Exact match required
            'description': 'DSWx-HLS accountability analysis for 2026-02-06'
        },
        # Add more accountability test cases here as needed:
        # 'DSWX_HLS_ANOTHER_DATE': {
        #     'start_date': '2026-01-12',
        #     'end_date': '2026-01-13',
        #     'expected_missing': 250,
        #     'tolerance': 0,
        #     'description': 'DSWx-HLS accountability for 2026-01-12'
        # },
    }
}


# =============================================================================
# CMR QUERY HELPERS
# =============================================================================

def query_cmr_directly(collection_id: str, start_date: str, end_date: str) -> list:
    """
    Query CMR directly with pagination support.

    Args:
        collection_id: CMR collection concept ID
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        List of all granules from CMR
    """
    url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json_v1_4"
    params = {
        'collection_concept_id': collection_id,
        'temporal[]': f'{start_date}T00:00:00Z,{end_date}T00:00:00Z',
        'page_size': 2000
    }
    headers = {'Client-Id': 'nasa.jpl.opera.sds.pytest.integration'}

    all_granules = []

    while True:
        response = requests.get(url, params=params, headers=headers, timeout=60)
        response.raise_for_status()
        data = response.json()

        granules = data.get('items', [])
        all_granules.extend(granules)

        search_after = response.headers.get('CMR-Search-After')
        if not search_after:
            break

        headers['CMR-Search-After'] = search_after

    return all_granules


def analyze_duplicates_from_cmr(granules: list, product: str) -> dict:
    """
    Analyze duplicates from CMR granules using same logic as opera-audit.

    Args:
        granules: List of CMR granule dicts
        product: Product name (e.g., 'DSWX_HLS', 'CSLC_S1')

    Returns:
        Dict with duplicate analysis results
    """
    product_config = CONFIG['products'][product]
    pattern = re.compile(product_config['pattern'])
    unique_fields = product_config['unique_fields']
    creation_field = product_config.get('creation_field')

    unique_groups = defaultdict(list)

    for item in granules:
        granule_id = item['umm']['GranuleUR']
        match = pattern.match(granule_id)

        if not match:
            continue

        fields = match.groupdict()
        unique_key = tuple(fields[f] for f in unique_fields)

        creation_ts = fields.get(creation_field, '') if creation_field else ''
        unique_groups[unique_key].append({
            'id': granule_id,
            'creation_ts': creation_ts
        })

    # Find duplicates (keep newest)
    duplicates = []
    for unique_key, group_granules in unique_groups.items():
        if len(group_granules) > 1:
            # Sort by creation_ts, oldest ones are duplicates
            sorted_granules = sorted(group_granules, key=lambda x: x['creation_ts'])
            for g in sorted_granules[:-1]:
                duplicates.append(g['id'])

    return {
        'total': len([item['umm']['GranuleUR'] for item in granules]),
        'unique': len(unique_groups),
        'duplicates': len(duplicates),
        'duplicate_list': sorted(duplicates)
    }


def analyze_accountability_from_cmr(
    dswx_granules: list,
    hls_s30_granules: list,
    hls_l30_granules: list
) -> dict:
    """
    Analyze accountability from CMR granules using same logic as opera-audit.

    Args:
        dswx_granules: List of DSWx-HLS granules
        hls_s30_granules: List of HLS-S30 granules
        hls_l30_granules: List of HLS-L30 granules

    Returns:
        Dict with accountability results
    """
    # L9 cutoff from config
    cutoff_str = CONFIG['products']['DSWX_HLS']['accountability']['l9_cutoff_date']
    cutoff_str = cutoff_str.replace('Z', '')
    naive_dt = datetime.fromisoformat(cutoff_str)
    L9_CUTOFF = naive_dt.replace(tzinfo=timezone.utc)

    # Patterns
    hls_pattern = re.compile(
        CONFIG['products']['DSWX_HLS']['accountability']['hls_pattern']
    )
    hls_suffix_pattern = re.compile(r'[.](B[A-Za-z0-9]{2}|Fmask)[.]tif$')

    # Build HLS to DSWx mapping
    hls_to_dswx = defaultdict(list)

    for granule in dswx_granules:
        granule_id = granule['umm']['GranuleUR']
        input_granules = granule['umm'].get('InputGranules', [])

        for input_file in input_granules:
            input_name = basename(input_file)
            input_name = re.sub(hls_suffix_pattern, '', input_name)

            if hls_pattern.match(input_name):
                hls_to_dswx[input_name].append(granule_id)

    # Process HLS granules with L9 filtering
    all_hls = hls_s30_granules + hls_l30_granules
    filtered_hls = []

    for granule in all_hls:
        granule_id = granule['umm']['GranuleUR']
        acq_time_str = granule['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime']
        acq_time = datetime.fromisoformat(acq_time_str.replace('Z', '+00:00'))

        platforms = [p['ShortName'] for p in granule['umm'].get('Platforms', [])]

        # Filter L9 before cutoff
        if 'LANDSAT-9' in platforms and acq_time < L9_CUTOFF:
            continue

        filtered_hls.append(granule_id)

        if granule_id not in hls_to_dswx:
            hls_to_dswx[granule_id] = []

    # Find missing
    missing = [hls_id for hls_id, dswx_list in hls_to_dswx.items() if len(dswx_list) == 0]

    return {
        'expected': len(filtered_hls),
        'actual': len(filtered_hls) - len(missing),
        'missing': sorted(missing),
        'missing_count': len(missing)
    }


# =============================================================================
# DUPLICATE DETECTION TESTS (Automatically generated from TEST_CASES)
# =============================================================================

@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.parametrize(
    "test_name,test_case",
    [(name, case) for name, case in TEST_CASES['DUPLICATES'].items()],
    ids=[name for name in TEST_CASES['DUPLICATES'].keys()]
)
def test_duplicate_detection_matches_cmr(test_name, test_case):
    """
    Test that duplicate detection matches independent CMR analysis.

    This test is automatically generated for each entry in TEST_CASES['DUPLICATES'].
    To add new tests, simply add entries to the TEST_CASES dict at the top of this file.

    Validates:
    - CMR query and pagination
    - Pattern matching and field extraction
    - Duplicate identification by unique ID
    - Creation timestamp comparison
    """
    product = test_case['product']

    # Parse dates
    start_date = datetime.strptime(test_case['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(test_case['end_date'], '%Y-%m-%d')

    # Run opera-audit duplicate detection
    ccid = CONFIG['products'][product]['ccid']['PROD']
    cmr_granules = query_cmr(ccid, start_date, end_date, 'PROD')
    opera_results = detect_duplicates(cmr_granules, product)

    # Run independent CMR analysis
    cmr_granules_direct = query_cmr_directly(
        ccid,
        test_case['start_date'],
        test_case['end_date']
    )
    cmr_results = analyze_duplicates_from_cmr(cmr_granules_direct, product)

    # Compare results
    opera_duplicates = set(opera_results['duplicate_list'])
    cmr_duplicates = set(cmr_results['duplicate_list'])

    # Check if counts match
    tolerance = test_case['tolerance']
    count_diff = abs(opera_results['duplicates'] - cmr_results['duplicates'])

    if count_diff > tolerance:
        pytest.fail(
            f"\n{'='*70}\n"
            f"DUPLICATE COUNT MISMATCH\n"
            f"{'='*70}\n"
            f"Test: {test_case['description']}\n"
            f"Product: {product}\n"
            f"Date range: {test_case['start_date']} to {test_case['end_date']}\n"
            f"\n"
            f"Opera-audit duplicates: {opera_results['duplicates']}\n"
            f"CMR analysis duplicates: {cmr_results['duplicates']}\n"
            f"Expected duplicates:     {test_case['expected_duplicates']}\n"
            f"Difference:              {count_diff}\n"
            f"Tolerance:               {tolerance}\n"
            f"\n"
            f"⚠️  IMPORTANT: Before considering this a bug, verify:\n"
            f"   1. Have duplicates been removed from CMR since test was created?\n"
            f"   2. Has production been corrected?\n"
            f"   3. If so, update TEST_CASES['DUPLICATES']['{test_name}']['expected_duplicates']\n"
            f"      in tests/test_cmr_integration.py\n"
            f"\n"
            f"Granules only in opera-audit: {len(opera_duplicates - cmr_duplicates)}\n"
            f"Granules only in CMR:         {len(cmr_duplicates - opera_duplicates)}\n"
            f"{'='*70}\n"
        )

    # Check if duplicate lists match
    if opera_duplicates != cmr_duplicates:
        in_opera_not_cmr = opera_duplicates - cmr_duplicates
        in_cmr_not_opera = cmr_duplicates - opera_duplicates

        pytest.fail(
            f"\n{'='*70}\n"
            f"DUPLICATE LIST MISMATCH\n"
            f"{'='*70}\n"
            f"Test: {test_case['description']}\n"
            f"Product: {product}\n"
            f"\n"
            f"Granules in opera-audit but NOT in CMR ({len(in_opera_not_cmr)}):\n"
            f"{list(sorted(in_opera_not_cmr))[:10]}\n"
            f"\n"
            f"Granules in CMR but NOT in opera-audit ({len(in_cmr_not_opera)}):\n"
            f"{list(sorted(in_cmr_not_opera))[:10]}\n"
            f"\n"
            f"⚠️  This indicates a logic difference between opera-audit and CMR analysis.\n"
            f"{'='*70}\n"
        )

    # All checks passed
    assert opera_results['duplicates'] == cmr_results['duplicates']
    assert opera_duplicates == cmr_duplicates


# =============================================================================
# ACCOUNTABILITY TESTS (Automatically generated from TEST_CASES)
# =============================================================================

@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.parametrize(
    "test_name,test_case",
    [(name, case) for name, case in TEST_CASES['ACCOUNTABILITY'].items()],
    ids=[name for name in TEST_CASES['ACCOUNTABILITY'].keys()]
)
def test_accountability_matches_cmr(test_name, test_case):
    """
    Test that accountability analysis matches independent CMR analysis.

    This test is automatically generated for each entry in TEST_CASES['ACCOUNTABILITY'].
    To add new tests, simply add entries to the TEST_CASES dict at the top of this file.

    NOTE: Currently only supports DSWX_HLS accountability.

    Validates:
    - CMR query for DSWx, HLS-S30, and HLS-L30
    - InputGranules metadata extraction
    - Band suffix stripping
    - HLS pattern matching
    - L9 filtering by cutoff date
    - Missing product identification
    """
    # Parse dates
    start_date = datetime.strptime(test_case['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(test_case['end_date'], '%Y-%m-%d')

    # Run opera-audit accountability analysis
    dswx_ccid = CONFIG['products']['DSWX_HLS']['ccid']['PROD']
    hls_s30_ccid = CONFIG['products']['DSWX_HLS']['accountability']['hls_s30_ccid']['PROD']
    hls_l30_ccid = CONFIG['products']['DSWX_HLS']['accountability']['hls_l30_ccid']['PROD']

    dswx_granules = query_cmr(dswx_ccid, start_date, end_date, 'PROD')
    hls_s30_granules = query_cmr(hls_s30_ccid, start_date, end_date, 'PROD')
    hls_l30_granules = query_cmr(hls_l30_ccid, start_date, end_date, 'PROD')

    opera_results = analyze_accountability(dswx_granules, hls_s30_granules + hls_l30_granules)

    # Run independent CMR analysis
    dswx_direct = query_cmr_directly(dswx_ccid, test_case['start_date'], test_case['end_date'])
    hls_s30_direct = query_cmr_directly(hls_s30_ccid, test_case['start_date'], test_case['end_date'])
    hls_l30_direct = query_cmr_directly(hls_l30_ccid, test_case['start_date'], test_case['end_date'])

    cmr_results = analyze_accountability_from_cmr(dswx_direct, hls_s30_direct, hls_l30_direct)

    # Compare results
    opera_missing = set(opera_results['missing'])
    cmr_missing = set(cmr_results['missing'])

    # Check if counts match
    tolerance = test_case['tolerance']
    count_diff = abs(opera_results['missing_count'] - cmr_results['missing_count'])

    if count_diff > tolerance:
        pytest.fail(
            f"\n{'='*70}\n"
            f"MISSING PRODUCT COUNT MISMATCH\n"
            f"{'='*70}\n"
            f"Test: {test_case['description']}\n"
            f"Date range: {test_case['start_date']} to {test_case['end_date']}\n"
            f"\n"
            f"Opera-audit missing:  {opera_results['missing_count']}\n"
            f"CMR analysis missing: {cmr_results['missing_count']}\n"
            f"Expected missing:     {test_case['expected_missing']}\n"
            f"Difference:           {count_diff}\n"
            f"Tolerance:            {tolerance}\n"
            f"\n"
            f"⚠️  IMPORTANT: Before considering this a bug, verify:\n"
            f"   1. Have missing products been produced since test was created?\n"
            f"   2. Has the accountability gap been closed?\n"
            f"   3. If so, update TEST_CASES['ACCOUNTABILITY']['{test_name}']['expected_missing']\n"
            f"      in tests/test_cmr_integration.py\n"
            f"\n"
            f"Accountability metrics:\n"
            f"  Expected HLS granules: {opera_results['expected']}\n"
            f"  Matched DSWx:          {opera_results['actual']}\n"
            f"  Accountability rate:   {(opera_results['actual']/opera_results['expected']*100):.2f}%\n"
            f"{'='*70}\n"
        )

    # Check if missing lists match
    if opera_missing != cmr_missing:
        in_opera_not_cmr = opera_missing - cmr_missing
        in_cmr_not_opera = cmr_missing - opera_missing

        pytest.fail(
            f"\n{'='*70}\n"
            f"MISSING PRODUCT LIST MISMATCH\n"
            f"{'='*70}\n"
            f"Test: {test_case['description']}\n"
            f"\n"
            f"Granules in opera-audit but NOT in CMR ({len(in_opera_not_cmr)}):\n"
            f"{list(sorted(in_opera_not_cmr))[:10]}\n"
            f"\n"
            f"Granules in CMR but NOT in opera-audit ({len(in_cmr_not_opera)}):\n"
            f"{list(sorted(in_cmr_not_opera))[:10]}\n"
            f"\n"
            f"⚠️  This indicates a logic difference between opera-audit and CMR analysis.\n"
            f"{'='*70}\n"
        )

    # All checks passed
    assert opera_results['missing_count'] == cmr_results['missing_count']
    assert opera_missing == cmr_missing


# =============================================================================
# HELPER TESTS
# =============================================================================

@pytest.mark.integration
class TestCMRConnectivity:
    """Test basic CMR connectivity and configuration."""

    def test_cmr_collections_exist(self):
        """Verify that all configured collection IDs exist in CMR."""
        # Get unique products from test cases
        products_to_test = set()
        for test_case in TEST_CASES['DUPLICATES'].values():
            products_to_test.add(test_case['product'])

        for product in products_to_test:
            ccid = CONFIG['products'][product]['ccid']['PROD']

            # Query CMR with minimal parameters
            url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json_v1_4"
            params = {
                'collection_concept_id': ccid,
                'page_size': 1
            }

            response = requests.get(url, params=params, timeout=30)

            assert response.status_code == 200, (
                f"Failed to query {product} collection {ccid} from CMR. "
                f"Status code: {response.status_code}"
            )
