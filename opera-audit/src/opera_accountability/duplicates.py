"""Duplicate detection logic for OPERA products."""

import re
import logging
import gc
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Any, Optional

from . import CONFIG
from .cmr import query_cmr

logger = logging.getLogger(__name__)

# DISP-S1 end conflict detection regex (from Gerald's tool)
DISP_S1_END_CONFLICT_PATTERN = re.compile(
    r'OPERA_L3_DISP-S1_IW_(?P<frame_id>F\d{5})_(?P<pol>VV|VH|HH|HV|VV\+VH|HH\+HV)_'
    r'(?P<begin_dt>\d{8}T\d{6}Z)_(?P<end_dt>\d{8}T\d{6}Z)_v\d+[.]\d+_(?P<production_dt>\d{8}T\d{6}Z)'
)


def detect_duplicates(cmr_granules: list[dict], product: str) -> dict[str, Any]:
    """
    Detect duplicate granules based on product configuration.

    Algorithm (from Riley's duplicate_check.py):
    1. Parse granule IDs with regex pattern
    2. Extract unique identifier from configured fields
    3. Group by unique identifier
    4. Select latest by creation timestamp
    5. Aggregate by acquisition date

    Args:
        cmr_granules: List of CMR granule dicts (UMM JSON format)
        product: Product name (e.g., 'DSWX_HLS')

    Returns:
        Dict with duplicate analysis results:
        {
            "total": int,
            "unique": int,
            "duplicates": int,
            "duplicate_list": [granule_ids],
            "by_date": {date: {total, unique, duplicates}}
        }
    """
    product_config = CONFIG['products'][product]

    # Compile regex pattern
    pattern = re.compile(product_config['pattern'])
    unique_fields = product_config['unique_fields']
    agg_field = product_config['aggregation_field']
    agg_format = product_config['aggregation_format']
    creation_field = product_config.get('creation_field')

    # Extract granule IDs from CMR response
    granule_ids = [g['umm']['GranuleUR'] for g in cmr_granules]

    logger.info(f"Processing {len(granule_ids)} granules for {product}")

    # Track unique granules and duplicates
    unique_granules = {}  # {unique_id_tuple: (granule_id, creation_ts)}
    all_duplicates = []
    by_date = defaultdict(lambda: {'total': 0, 'unique': 0, 'duplicates': 0})

    for granule_id in granule_ids:
        match = pattern.match(granule_id)

        if not match:
            logger.warning(f"Granule ID {granule_id} did not match pattern")
            continue

        fields = match.groupdict()

        # Build unique identifier from configured fields
        unique_id = tuple(fields[f] for f in unique_fields)

        # Get acquisition date for aggregation
        agg_time = datetime.strptime(fields[agg_field], agg_format)
        agg_date = agg_time.date().isoformat()

        # Track by date
        by_date[agg_date]['total'] += 1

        # Check for duplicates
        if unique_id in unique_granules:
            # This is a duplicate
            by_date[agg_date]['duplicates'] += 1

            existing_granule_id, existing_creation_ts = unique_granules[unique_id]

            # If we have creation timestamps, select the latest
            if creation_field:
                current_creation_ts = fields[creation_field]

                # Keep the latest version
                if current_creation_ts > existing_creation_ts:
                    # Current is newer, mark existing as duplicate
                    all_duplicates.append(existing_granule_id)
                    unique_granules[unique_id] = (granule_id, current_creation_ts)
                else:
                    # Existing is newer, mark current as duplicate
                    all_duplicates.append(granule_id)
            else:
                # No creation field, just track first one
                all_duplicates.append(granule_id)
        else:
            # First occurrence of this unique ID
            creation_ts = fields.get(creation_field, '') if creation_field else ''
            unique_granules[unique_id] = (granule_id, creation_ts)
            by_date[agg_date]['unique'] += 1

    # Convert by_date to regular dict and sort
    by_date = dict(sorted(by_date.items()))

    # Calculate unique count (items not in duplicates list)
    total_granules = len(granule_ids)
    duplicate_count = len(all_duplicates)
    unique_count = len(unique_granules)

    if total_granules > 0:
        logger.info(
            f"Found {duplicate_count} duplicates out of {total_granules} granules "
            f"({(duplicate_count / total_granules * 100):.2f}%)"
        )
    else:
        logger.info("No granules to process")

    return {
        'total': total_granules,
        'unique': unique_count,
        'duplicates': duplicate_count,
        'duplicate_list': sorted(all_duplicates),
        'by_date': by_date
    }


def detect_disp_s1_end_conflicts(cmr_granules: list[dict]) -> dict[str, Any]:
    """
    Detect DISP-S1 end conflicts (same frame+end date, different begin date).
    
    Ported from Gerald's detect_cmr_duplicates_for_disp_s1.py.
    
    Args:
        cmr_granules: List of CMR granule records
        
    Returns:
        Dict with end conflict analysis results:
        {
            "total": int,
            "conflict_groups": int,
            "conflicting_products": int,
            "conflicts": {frame_id_end_dt: {begin_dts, products, ...}}
        }
    """
    granule_ids = [g['umm']['GranuleUR'] for g in cmr_granules]
    logger.info(f"Processing {len(granule_ids)} DISP-S1 granules for end conflicts")

    # Group by frame+pol+end datetime to find conflicts
    conflict_groups: dict[str, dict] = {}
    parse_failures = 0
    
    for granule in cmr_granules:
        granule_id = granule['umm']['GranuleUR']
        match = DISP_S1_END_CONFLICT_PATTERN.match(granule_id)
        
        if not match:
            parse_failures += 1
            continue
        
        frame_id = match.group('frame_id')  # Includes 'F' prefix
        pol = match.group('pol')
        begin_dt = match.group('begin_dt')
        end_dt = match.group('end_dt')
        
        # Conflict key: frame_id + pol + end_dt
        conflict_key = f"{frame_id}_{pol}_{end_dt}"
        
        if conflict_key not in conflict_groups:
            conflict_groups[conflict_key] = {
                'frame_id': frame_id,
                'pol': pol,
                'end_dt': end_dt,
                'begin_dts': set(),
                'products': []
            }
        
        conflict_groups[conflict_key]['begin_dts'].add(begin_dt)
        conflict_groups[conflict_key]['products'].append(granule_id)

    # Identify conflicts: groups with >1 begin datetime
    actual_conflicts = {}
    total_conflicting_products = 0

    for key, items in conflict_groups.items():
        if len(items['begin_dts']) > 1:
            # This is a conflict: same frame+pol+end_dt, different begin_dt
            conflict_key = f"{items['frame_id']}_{items['pol']}_{items['end_dt']}"
            actual_conflicts[conflict_key] = {
                'frame_id': items['frame_id'],
                'pol': items['pol'],
                'end_dt': items['end_dt'],
                'begin_dts': sorted(list(items['begin_dts'])),
                'products': items['products'],
                'count': len(items['products'])
            }
            total_conflicting_products += len(items['products'])

    total_granules = len(granule_ids)
    conflict_group_count = len(actual_conflicts)

    if total_granules > 0:
        logger.info(
            f"Found {conflict_group_count} end conflict groups ({total_conflicting_products} products) "
            f"out of {total_granules} granules "
            f"({(total_conflicting_products / total_granules * 100):.2f}%)"
        )
    else:
        logger.info("No granules to process for end conflicts")

    return {
        'total': total_granules,
        'conflict_groups': conflict_group_count,
        'conflicting_products': total_conflicting_products,
        'conflicts': actual_conflicts,
        'parse_failures': parse_failures,
    }


def detect_duplicates_memory_efficient(
    product: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    venue: str = 'PROD',
    chunk_days: int = 30,
    batch_size: int = 100000
) -> dict[str, Any]:
    """
    Detect duplicates using memory-efficient batched processing with time chunking.

    This approach avoids CMR's 1M result / 1000 page limit by:
    - Splitting time range into chunks
    - Processing granules in batches with garbage collection
    - Extracting only GranuleUR strings from CMR responses

    Args:
        product: Product name (e.g., 'DSWX_HLS')
        start_date: Start datetime for CMR query
        end_date: End datetime for CMR query
        venue: 'PROD' or 'UAT'
        chunk_days: Number of days per time chunk (default: 30)
        batch_size: Number of granule IDs to process before garbage collection

    Returns:
        Dict with duplicate analysis results
    """
    product_config = CONFIG['products'][product]
    ccid = product_config['ccid'].get(venue)
    
    if not ccid:
        raise ValueError(f"No CCID configured for {product} in {venue}")

    pattern = re.compile(product_config['pattern'])
    unique_fields = product_config['unique_fields']
    agg_field = product_config['aggregation_field']
    agg_format = product_config['aggregation_format']
    creation_field = product_config.get('creation_field')

    # Generate time chunks
    time_chunks = list(_generate_time_chunks(start_date, end_date, chunk_days))
    logger.info(f"Split query into {len(time_chunks)} time chunks of ~{chunk_days} days each")

    # Use a set to deduplicate granule IDs across chunks
    all_granule_ids_set = set()
    total_products_fetched = 0

    for chunk_start, chunk_end in time_chunks:
        cmr_granules = query_cmr(ccid, chunk_start, chunk_end, venue)
        chunk_count = len(cmr_granules)
        total_products_fetched += chunk_count

        # Extract only GranuleUR strings
        granule_ids = [g['umm']['GranuleUR'] for g in cmr_granules]
        all_granule_ids_set.update(granule_ids)

        # Clear from memory
        del cmr_granules
        del granule_ids
        gc.collect()

        logger.info(f"Chunk {chunk_start.strftime('%Y-%m-%d')}: {chunk_count} fetched, {len(all_granule_ids_set)} unique")

    # Convert to list for processing
    granule_ids = list(all_granule_ids_set)
    del all_granule_ids_set
    gc.collect()

    total_products = len(granule_ids)
    logger.info(f"Retrieved {total_products_fetched} products from CMR ({total_products} unique)")

    # Process in batches
    unique_granules = {}
    all_duplicates = []
    by_date = defaultdict(lambda: {'total': 0, 'unique': 0, 'duplicates': 0})
    parse_failures = 0

    for batch_start in range(0, len(granule_ids), batch_size):
        batch_end = min(batch_start + batch_size, len(granule_ids))
        batch = granule_ids[batch_start:batch_end]

        for granule_id in batch:
            match = pattern.match(granule_id)

            if not match:
                parse_failures += 1
                continue

            fields = match.groupdict()
            unique_id = tuple(fields[f] for f in unique_fields)

            # Get acquisition date
            agg_time = datetime.strptime(fields[agg_field], agg_format)
            agg_date = agg_time.date().isoformat()
            by_date[agg_date]['total'] += 1

            if unique_id in unique_granules:
                by_date[agg_date]['duplicates'] += 1
                existing_granule_id, existing_creation_ts = unique_granules[unique_id]

                if creation_field:
                    current_creation_ts = fields[creation_field]
                    if current_creation_ts > existing_creation_ts:
                        all_duplicates.append(existing_granule_id)
                        unique_granules[unique_id] = (granule_id, current_creation_ts)
                    else:
                        all_duplicates.append(granule_id)
                else:
                    all_duplicates.append(granule_id)
            else:
                creation_ts = fields.get(creation_field, '') if creation_field else ''
                unique_granules[unique_id] = (granule_id, creation_ts)
                by_date[agg_date]['unique'] += 1

        # Garbage collection periodically
        if batch_start > 0 and batch_start % (batch_size * 5) == 0:
            gc.collect()

    del granule_ids
    gc.collect()

    if parse_failures > 0:
        logger.warning(f"Failed to parse {parse_failures} granule IDs")

    by_date = dict(sorted(by_date.items()))
    total_granules = len(unique_granules) + len(all_duplicates)
    duplicate_count = len(all_duplicates)
    unique_count = len(unique_granules)

    return {
        'total': total_granules,
        'unique': unique_count,
        'duplicates': duplicate_count,
        'duplicate_list': sorted(all_duplicates),
        'by_date': by_date,
        'parse_failures': parse_failures
    }


def _generate_time_chunks(start_date: Optional[datetime], end_date: Optional[datetime], chunk_days: int = 30):
    """Generate time chunks for CMR queries to avoid page limits."""
    if not start_date:
        start_date = datetime.now() - timedelta(days=365)
    if not end_date:
        end_date = datetime.now()

    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days), end_date)
        yield (current, chunk_end)
        current = chunk_end
