"""Accountability analysis for DSWX_HLS (HLS input mapping).

Ported from Riley's ``dswx-hls-input-map.py`` on PCM develop branch.
"""

import re
import logging
from datetime import datetime, timezone
from os.path import basename
from typing import Any
from collections import defaultdict

from ... import CONFIG

logger = logging.getLogger(__name__)

# Landsat-9 cutoff date (from Riley's dswx-hls-input-map.py)
L9_CUTOFF = None  # Will be set from config

# CMR temporal format used by DSWx-HLS for grouping by date
_CMR_TIME_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"


def _parse_l9_cutoff():
    """Parse L9 cutoff date from config."""
    global L9_CUTOFF
    cutoff_str = CONFIG["products"]["DSWX_HLS"]["accountability"]["l9_cutoff_date"]
    # Parse ISO format with microseconds: 2025-10-01T00:04:07.135000Z
    # Remove 'Z' and parse as naive, then make timezone-aware with UTC
    cutoff_str = cutoff_str.replace("Z", "")
    naive_dt = datetime.fromisoformat(cutoff_str)
    # Make timezone-aware by attaching UTC timezone
    L9_CUTOFF = naive_dt.replace(tzinfo=timezone.utc)


def _format_facet_date(d: datetime) -> str:
    """Format a date for faceted grouping (ported from PCM dswx-hls-input-map.py)."""
    return f'{d.strftime("%Y-%m-%d")} / {d.strftime("%Y-%j")}'


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
    5. Detect DSWx-HLS duplicates (multiple DSWx for same HLS)
    6. Aggregate by date and month

    Args:
        dswx_granules: List of DSWx-HLS granules from CMR
        hls_granules: List of HLS granules from CMR

    Returns:
        Dict with accountability results including by_date and by_month breakdowns.
    """
    if L9_CUTOFF is None:
        _parse_l9_cutoff()

    hls_config = CONFIG["products"]["DSWX_HLS"]["accountability"]
    hls_pattern = re.compile(hls_config["hls_pattern"])
    dswx_pattern = re.compile(CONFIG["products"]["DSWX_HLS"]["pattern"])
    hls_suffix_pattern = re.compile(r"[.](B[A-Za-z0-9]{2}|Fmask)[.]tif$")

    # Map (hls_granule_id, date) -> [dswx_granule_id, ...]
    # Uses tuple key to match PCM's grouping (HLS ID + acquisition date facet).
    hls_to_dswx: dict[tuple[str, str], list[str]] = {}

    logger.info(f"Processing {len(dswx_granules)} DSWx-HLS granules")

    for granule in dswx_granules:
        granule_id = granule["umm"]["GranuleUR"]
        input_granules = granule["umm"].get("InputGranules", [])
        acq_time_str = granule["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
        acq_time = datetime.strptime(acq_time_str, _CMR_TIME_FMT)
        date_facet = _format_facet_date(acq_time)

        # Extract HLS inputs from DSWx metadata
        for input_file in input_granules:
            # Strip path and HLS band suffix
            input_name = basename(input_file)
            input_name = re.sub(hls_suffix_pattern, "", input_name)

            # Check if it matches HLS pattern
            if hls_pattern.match(input_name):
                hls_to_dswx.setdefault((input_name, date_facet), []).append(granule_id)

    n_dswx_hls_inputs = len(hls_to_dswx)
    logger.info(f"Mapped DSWx to {n_dswx_hls_inputs} unique HLS inputs")
    logger.info(f"Processing {len(hls_granules)} HLS granules")

    # Process HLS granules and filter L9
    filtered_hls = []

    for granule in hls_granules:
        granule_id = granule["umm"]["GranuleUR"]
        acq_time_str = granule["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
        acq_time = datetime.fromisoformat(acq_time_str.replace("Z", "+00:00"))

        platforms = [p["ShortName"] for p in granule["umm"].get("Platforms", [])]

        # Filter out L9 before cutoff date
        if "LANDSAT-9" in platforms and acq_time < L9_CUTOFF:
            logger.debug(f"Filtering out L9 granule {granule_id} before cutoff")
            continue

        filtered_hls.append(granule_id)
        date_facet = _format_facet_date(acq_time.replace(tzinfo=None))

        # Add to mapping if not already there
        if (granule_id, date_facet) not in hls_to_dswx:
            hls_to_dswx[(granule_id, date_facet)] = []

    logger.info(f"After L9 filtering: {len(filtered_hls)} HLS granules")
    logger.info(
        f"Found {len(hls_to_dswx) - n_dswx_hls_inputs} HLS granules "
        f"not mapped to an OPERA DSWx-HLS product"
    )

    # Group by date (ported from PCM dswx-hls-input-map.py)
    date_map: dict[str, dict[str, list[str]]] = {}
    hls_mappings: dict[str, list[str]] = {}

    for (hls_granule, date_facet), dswx_list in hls_to_dswx.items():
        date_map.setdefault(date_facet, {})[hls_granule] = dswx_list
        hls_mappings[hls_granule] = dswx_list

    # Aggregate counts by date (matching PCM's 4-metric breakdown)
    by_date = {}
    by_month = {}

    for date_facet, granule_map in date_map.items():
        day_counts = {
            "hls_granules": len(granule_map),
            "matched_dswx_hls_granules": sum(len(v) for v in granule_map.values()),
            "hls_to_many_dswx": len([v for v in granule_map.values() if len(v) > 1]),
            "hls_to_no_dswx": len([v for v in granule_map.values() if len(v) == 0]),
        }
        by_date[date_facet] = day_counts

        # Aggregate monthly
        month_str = datetime.strptime(
            date_facet.split("/")[0].strip(), "%Y-%m-%d"
        ).strftime("%Y-%m")
        if month_str not in by_month:
            by_month[month_str] = dict(day_counts)
        else:
            for k in day_counts:
                by_month[month_str][k] += day_counts[k]

    # Find missing DSWx outputs
    missing = [hls_id for hls_id, dswx_list in hls_mappings.items() if len(dswx_list) == 0]

    # Find DSWx-HLS duplicates (ported from PCM): for each HLS with >0
    # DSWx products, sort by creation_ts and mark all but the latest as dupes.
    duplicates = []
    matched_mappings = {k: v for k, v in hls_mappings.items() if len(v) > 0}
    for product_list in matched_mappings.values():
        if len(product_list) > 1:
            product_list.sort(
                key=lambda x: dswx_pattern.match(x).groupdict()["creation_ts"]
                if dswx_pattern.match(x) else "",
                reverse=True,
            )
            duplicates.extend(product_list[1:])

    # Overall counts
    overall_counts = {
        "hls_granules": sum(v["hls_granules"] for v in by_date.values()),
        "matched_dswx_hls_granules": sum(v["matched_dswx_hls_granules"] for v in by_date.values()),
        "hls_to_many_dswx": sum(v["hls_to_many_dswx"] for v in by_date.values()),
        "hls_to_no_dswx": sum(v["hls_to_no_dswx"] for v in by_date.values()),
    }

    logger.info(f"Found {len(missing)} HLS granules with no DSWx output")
    logger.info(f"Found {len(duplicates)} DSWx-HLS duplicates")

    return {
        "expected": len(filtered_hls),
        "actual": len(filtered_hls) - len(missing),
        "missing": sorted(missing),
        "missing_count": len(missing),
        "duplicates": duplicates,
        "overall_counts": overall_counts,
        "by_date": by_date,
        "by_month": by_month,
    }
