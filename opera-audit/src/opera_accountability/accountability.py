"""Accountability analysis for DSWX_HLS (HLS input mapping)."""

import re
import logging
from datetime import datetime, timezone
from os.path import basename
from typing import Any
from collections import defaultdict

from . import CONFIG

logger = logging.getLogger(__name__)

# Landsat-9 cutoff date (from Riley's dswx-hls-input-map.py)
L9_CUTOFF = None  # Will be set from config


def _parse_l9_cutoff():
    """Parse L9 cutoff date from config."""
    global L9_CUTOFF
    cutoff_str = CONFIG['products']['DSWX_HLS']['accountability']['l9_cutoff_date']
    # Parse ISO format with microseconds: 2025-10-01T00:04:07.135000Z
    # Remove 'Z' and parse as naive, then make timezone-aware with UTC
    cutoff_str = cutoff_str.replace('Z', '')
    naive_dt = datetime.fromisoformat(cutoff_str)
    # Make timezone-aware by attaching UTC timezone
    L9_CUTOFF = naive_dt.replace(tzinfo=timezone.utc)


def analyze_accountability(
    dswx_granules: list[dict],
    hls_granules: list[dict]
) -> dict[str, Any]:
    """
    Analyze accountability for DSWX_HLS by mapping to HLS inputs.

    Algorithm (from Riley's dswx-hls-input-map.py):
    1. Extract HLS inputs from DSWx-HLS metadata
    2. Query all HLS granules in time range
    3. Filter out L9 granules before cutoff date
    4. Find HLS granules with no DSWx output
    5. Aggregate by date

    Args:
        dswx_granules: List of DSWx-HLS granules from CMR
        hls_granules: List of HLS granules from CMR

    Returns:
        Dict with accountability results:
        {
            "expected": int,
            "actual": int,
            "missing": [granule_ids],
            "by_date": {date: {expected, actual, missing}}
        }
    """
    if L9_CUTOFF is None:
        _parse_l9_cutoff()

    hls_config = CONFIG['products']['DSWX_HLS']['accountability']
    hls_pattern = re.compile(hls_config['hls_pattern'])
    hls_suffix_pattern = re.compile(r'[.](B[A-Za-z0-9]{2}|Fmask)[.]tif$')

    # Map HLS inputs to DSWx outputs
    hls_to_dswx = defaultdict(list)

    logger.info(f"Processing {len(dswx_granules)} DSWx-HLS granules")

    for granule in dswx_granules:
        granule_id = granule['umm']['GranuleUR']
        input_granules = granule['umm'].get('InputGranules', [])

        # Extract HLS inputs from DSWx metadata
        for input_file in input_granules:
            # Strip path and HLS band suffix
            input_name = basename(input_file)
            input_name = re.sub(hls_suffix_pattern, '', input_name)

            # Check if it matches HLS pattern
            if hls_pattern.match(input_name):
                hls_to_dswx[input_name].append(granule_id)

    logger.info(f"Mapped DSWx to {len(hls_to_dswx)} unique HLS inputs")
    logger.info(f"Processing {len(hls_granules)} HLS granules")

    # Process HLS granules and filter L9
    filtered_hls = []

    for granule in hls_granules:
        granule_id = granule['umm']['GranuleUR']
        acq_time_str = granule['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime']
        acq_time = datetime.fromisoformat(acq_time_str.replace('Z', '+00:00'))

        platforms = [p['ShortName'] for p in granule['umm'].get('Platforms', [])]

        # Filter out L9 before cutoff date
        if 'LANDSAT-9' in platforms and acq_time < L9_CUTOFF:
            logger.debug(f"Filtering out L9 granule {granule_id} before cutoff")
            continue

        filtered_hls.append(granule_id)

        # Add to mapping if not already there
        if granule_id not in hls_to_dswx:
            hls_to_dswx[granule_id] = []

    logger.info(f"After L9 filtering: {len(filtered_hls)} HLS granules")

    # Find missing DSWx outputs
    missing = [hls_id for hls_id, dswx_list in hls_to_dswx.items() if len(dswx_list) == 0]

    logger.info(f"Found {len(missing)} HLS granules with no DSWx output")

    # Aggregate by date (simplified - just overall counts for now)
    by_date = {}  # TODO: Implement daily breakdown if needed

    return {
        'expected': len(filtered_hls),
        'actual': len(filtered_hls) - len(missing),
        'missing': sorted(missing),
        'missing_count': len(missing),
        'by_date': by_date
    }
