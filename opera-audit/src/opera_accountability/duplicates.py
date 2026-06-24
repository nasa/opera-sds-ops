"""Duplicate detection logic for OPERA products."""

import re
import logging
import gc
from datetime import datetime, timedelta
from collections import defaultdict
from itertools import chain
from typing import Any, Optional

from . import CONFIG
from .cmr import query_cmr

logger = logging.getLogger(__name__)

# DISP-S1 end conflict detection regex (from Gerald's tool)
# Note: Gerald's original only supports VV|HH (no VH, HV, or compound pols)
DISP_S1_END_CONFLICT_PATTERN = re.compile(
    r'OPERA_L3_DISP-S1_IW_'
    r'F(?P<frame_id>\d{5})'
    r'_(?P<pol>VV|HH)'
    r'_(?P<begin_dt>\d{8}T\d{6}Z)'
    r'_(?P<end_dt>\d{8}T\d{6}Z)'
    r'_v(?P<version>\d+\.\d+)'
    r'_(?P<production_dt>\d{8}T\d{6}Z)'
)


def detect_duplicates(cmr_granules: list[dict], product: str) -> dict[str, Any]:
    """
    Detect duplicate granules based on product configuration.

    Algorithm (from Riley's duplicate_check.py):
    1. Parse granule IDs with regex pattern
    2. Extract unique identifier from configured fields
    3. Group by unique identifier
    4. Select latest by creation timestamp
    5. Aggregate by acquisition date and month

    Args:
        cmr_granules: List of CMR granule dicts (UMM JSON format)
        product: Product name (e.g., 'DSWX_HLS')

    Returns:
        Dict with duplicate analysis results matching Riley's original format:
        {
            "granule_month_map": {month: {n_granules, n_duplicates, percent_duplicates, duplicates}},
            "aqc_date_map": {date: {n_granules, n_duplicates, percent_duplicates, duplicates}}
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

    # Track unique granules: {unique_id_tuple: (granule_id, repr(unique_id_tuple))}
    unique_granules = {}
    
    # Daily and monthly aggregation maps (exactly matching Riley's structure)
    aqc_date_map = {}
    granule_month_map = {}

    for granule_id in granule_ids:
        match = pattern.match(granule_id)

        if match is None:
            raise RuntimeError(f'Failed to parse granule ID {granule_id} with pattern {pattern.pattern}')

        group_dict = match.groupdict()

        # Parse aggregation timestamp
        granule_agg_time = datetime.strptime(group_dict[agg_field], agg_format)
        granule_agg_day = granule_agg_time.replace(hour=0, minute=0, second=0, microsecond=0)
        granule_agg_day = granule_agg_day.strftime('%Y-%m-%d')

        granule_agg_month = granule_agg_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        granule_agg_month = granule_agg_month.strftime('%Y-%m')

        # Initialize daily map entry if needed
        if granule_agg_day not in aqc_date_map:
            aqc_date_map[granule_agg_day] = {
                'n_granules': 0,
                'n_duplicates': 0,
                'percent_duplicates': -1.0,
                'duplicates': {}
            }

        # Initialize monthly map entry if needed
        if granule_agg_month not in granule_month_map:
            granule_month_map[granule_agg_month] = {
                'n_granules': 0,
                'n_duplicates': 0,
                'percent_duplicates': -1.0,
                'duplicates': {}
            }

        # Increment granule counts
        aqc_date_map[granule_agg_day]['n_granules'] += 1
        granule_month_map[granule_agg_month]['n_granules'] += 1

        # Build unique identifier tuple from configured fields
        granule_unique_ids = tuple([group_dict[grp] for grp in unique_fields])

        # Check for duplicates
        if granule_unique_ids in unique_granules:
            granule_month_map[granule_agg_month]['n_duplicates'] += 1
            aqc_date_map[granule_agg_day]['n_duplicates'] += 1
            first_duplicate = unique_granules[granule_unique_ids]

            # Store duplicate under the repr() of unique_ids as key
            if first_duplicate[1] not in granule_month_map[granule_agg_month]['duplicates']:
                granule_month_map[granule_agg_month]['duplicates'][first_duplicate[1]] = [first_duplicate[0]]

            if first_duplicate[1] not in aqc_date_map[granule_agg_day]['duplicates']:
                aqc_date_map[granule_agg_day]['duplicates'][first_duplicate[1]] = [first_duplicate[0]]

            granule_month_map[granule_agg_month]['duplicates'][first_duplicate[1]].append(granule_id)
            aqc_date_map[granule_agg_day]['duplicates'][first_duplicate[1]].append(granule_id)
        else:
            # First occurrence: store (granule_id, repr(unique_id_tuple))
            unique_granules[granule_unique_ids] = (granule_id, repr(granule_unique_ids))

    # Post-process: Sort duplicates by creation timestamp if available
    if creation_field:
        for month in granule_month_map:
            for duplicate in granule_month_map[month]['duplicates']:
                duplicate_granule_ids = granule_month_map[month]['duplicates'][duplicate]
                duplicate_granule_ids.sort(
                    key=lambda x: pattern.match(x).groupdict()[creation_field], reverse=True
                )

                granule_month_map[month]['duplicates'][duplicate] = {
                    'latest_product': duplicate_granule_ids[0],
                    'duplicate_products': duplicate_granule_ids[1:],
                }

        for date in aqc_date_map:
            for duplicate in aqc_date_map[date]['duplicates']:
                duplicate_granule_ids = aqc_date_map[date]['duplicates'][duplicate]
                duplicate_granule_ids.sort(
                    key=lambda x: pattern.match(x).groupdict()[creation_field], reverse=True
                )

                aqc_date_map[date]['duplicates'][duplicate] = {
                    'latest_product': duplicate_granule_ids[0],
                    'duplicate_products': duplicate_granule_ids[1:],
                }

    # Sort maps by date/month
    granule_month_map = dict(sorted(granule_month_map.items()))
    aqc_date_map = dict(sorted(aqc_date_map.items()))

    # Calculate percentage duplicates and add test-compatible aliases
    for month in granule_month_map.keys():
        n_gran = granule_month_map[month]['n_granules']
        granule_month_map[month]['percent_duplicates'] = (
            (granule_month_map[month]['n_duplicates'] / n_gran) * 100 if n_gran > 0 else 0.0
        )
        # Add CLI/test aliases (total, unique)
        granule_month_map[month]['total'] = granule_month_map[month]['n_granules']
        granule_month_map[month]['unique'] = n_gran - granule_month_map[month]['n_duplicates']

    for date in aqc_date_map.keys():
        n_gran = aqc_date_map[date]['n_granules']
        aqc_date_map[date]['percent_duplicates'] = (
            (aqc_date_map[date]['n_duplicates'] / n_gran) * 100 if n_gran > 0 else 0.0
        )
        # Add CLI/test aliases (total, unique)
        aqc_date_map[date]['total'] = aqc_date_map[date]['n_granules']
        aqc_date_map[date]['unique'] = n_gran - aqc_date_map[date]['n_duplicates']

    # Calculate total duplicates and stats (matching Riley's original)
    n_duplicates = sum([month['n_duplicates'] for month in granule_month_map.values()])
    n_granules = len(granule_ids)
    n_unique = len(unique_granules)
    
    if n_granules > 0:
        logger.info(f'Found {n_duplicates} duplicate granule IDs out of {n_granules} granules '
                    f'({(n_duplicates / n_granules) * 100:.1f}%)')
    else:
        logger.info('No granules found in date range')

    # Calculate duplicate counts per granule (for min/max/avg stats)
    if creation_field:
        duplicate_counts = list(chain.from_iterable(
            [len(dup['duplicate_products']) for dup in month['duplicates'].values()]
            for month in granule_month_map.values()
        ))
    else:
        duplicate_counts = list(chain.from_iterable(
            [len(dup) for dup in month['duplicates'].values()]
            for month in granule_month_map.values()
        ))

    # Build flat duplicate list for CLI/reports compatibility
    if creation_field:
        duplicate_list = []
        for month_data in granule_month_map.values():
            for dup_group in month_data['duplicates'].values():
                duplicate_list.extend(dup_group['duplicate_products'])
    else:
        duplicate_list = []
        for month_data in granule_month_map.values():
            for dup_group in month_data['duplicates'].values():
                duplicate_list.extend(dup_group[1:])  # All but first

    # Return format matching Riley's original with CLI compatibility
    return {
        # Riley's original format
        'granule_month_map': granule_month_map,
        'aqc_date_map': aqc_date_map,
        'by_date': aqc_date_map,  # Alias for tests/CLI compatibility
        # CLI compatibility fields
        'total': n_granules,
        'unique': n_unique,
        'duplicates': n_duplicates,
        'duplicate_list': sorted(duplicate_list),
        # Stats for reporting
        'min_duplicates_per_granule': min(duplicate_counts) if len(duplicate_counts) > 0 else None,
        'max_duplicates_per_granule': max(duplicate_counts) if len(duplicate_counts) > 0 else None,
        'avg_duplicates_per_granule': sum(duplicate_counts) / len(duplicate_counts) if len(duplicate_counts) > 0 else None,
    }


def detect_disp_s1_end_conflicts(cmr_granules: list[dict]) -> dict[str, Any]:
    """
    Detect DISP-S1 end conflicts (same frame+end date, different begin date).
    
    Exact port of Gerald's detect_cmr_duplicates_for_disp_s1.py end conflict detection.
    
    Groups by (frame_id, end_dt) only - NOT including polarization.
    Identifies conflicts when multiple different begin_dt values exist for the same frame+end.
    
    Args:
        cmr_granules: List of CMR granule records
        
    Returns:
        Dict with end conflict analysis matching Gerald's output structure:
        {
            "total": int,
            "conflict_groups": int,
            "conflicting_products": int,
            "conflicts": {frame_id_end_dt: {frame_id, end_dt, begin_dts, products, ...}}
        }
    """
    granule_ids = [g['umm']['GranuleUR'] for g in cmr_granules]
    logger.info(f"Processing {len(granule_ids)} DISP-S1 granules for end conflicts")

    # Group by frame+end datetime (Gerald's original: line 380-400)
    # Store (begin_dt, production_dt, version, granule_id) tuples
    end_grouped = defaultdict(list)
    parse_failures = 0
    
    for granule in cmr_granules:
        granule_id = granule['umm']['GranuleUR']
        match = DISP_S1_END_CONFLICT_PATTERN.match(granule_id)
        
        if not match:
            parse_failures += 1
            continue
        
        frame_id = int(match.group('frame_id'))  # Convert to int (Gerald's original: line 103)
        begin_dt = match.group('begin_dt')
        end_dt = match.group('end_dt')
        version = match.group('version')
        production_dt = match.group('production_dt')
        
        # Key is (frame_id, end_dt) - no polarization (Gerald's original: line 396)
        end_key = (frame_id, end_dt)
        
        # Store (begin_dt, production_dt, version, granule_id) (Gerald's original: line 400)
        end_grouped[end_key].append((begin_dt, production_dt, version, granule_id))

    # Find end conflicts (Gerald's original: lines 441-463)
    end_conflicts = {}
    conflicts_total_products = 0

    for key, items in end_grouped.items():
        if len(items) > 1:
            frame_id, end_dt = key
            # Check if there are different begin_dt values
            begin_dts = set(item[0] for item in items)
            if len(begin_dts) > 1:
                # Sort by begin_dt, then production_dt (Gerald's original: line 451)
                items_sorted = sorted(items, key=lambda x: (x[0], x[1]))
                conflict_key = f"F{frame_id:05d}_{end_dt}"
                end_conflicts[conflict_key] = {
                    'frame_id': frame_id,
                    'end_dt': end_dt,
                    'begin_dts': sorted(list(begin_dts)),
                    'count': len(items),
                    'products': [item[3] for item in items_sorted],
                    'production_times': [item[1] for item in items_sorted],
                    'versions': [item[2] for item in items_sorted]
                }
                conflicts_total_products += len(items)

    total_granules = len(granule_ids)
    conflict_group_count = len(end_conflicts)

    if total_granules > 0:
        logger.info(
            f"Found {conflict_group_count} end conflict groups ({conflicts_total_products} products) "
            f"out of {total_granules} granules "
            f"({(conflicts_total_products / total_granules * 100):.2f}%)"
        )
    else:
        logger.info("No granules to process for end conflicts")

    return {
        'total': total_granules,
        'conflict_groups': conflict_group_count,
        'conflicting_products': conflicts_total_products,
        'conflicts': end_conflicts,
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
        Dict with duplicate analysis results matching Riley's original format
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

    # Process in batches using Riley's exact algorithm
    unique_granules = {}
    aqc_date_map = {}
    granule_month_map = {}
    parse_failures = 0

    for batch_start in range(0, len(granule_ids), batch_size):
        batch_end = min(batch_start + batch_size, len(granule_ids))
        batch = granule_ids[batch_start:batch_end]

        for granule_id in batch:
            match = pattern.match(granule_id)

            if not match:
                parse_failures += 1
                continue

            group_dict = match.groupdict()

            # Parse aggregation timestamp
            granule_agg_time = datetime.strptime(group_dict[agg_field], agg_format)
            granule_agg_day = granule_agg_time.replace(hour=0, minute=0, second=0, microsecond=0)
            granule_agg_day = granule_agg_day.strftime('%Y-%m-%d')

            granule_agg_month = granule_agg_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            granule_agg_month = granule_agg_month.strftime('%Y-%m')

            # Initialize daily map entry
            if granule_agg_day not in aqc_date_map:
                aqc_date_map[granule_agg_day] = {
                    'n_granules': 0,
                    'n_duplicates': 0,
                    'percent_duplicates': -1.0,
                    'duplicates': {}
                }

            # Initialize monthly map entry
            if granule_agg_month not in granule_month_map:
                granule_month_map[granule_agg_month] = {
                    'n_granules': 0,
                    'n_duplicates': 0,
                    'percent_duplicates': -1.0,
                    'duplicates': {}
                }

            aqc_date_map[granule_agg_day]['n_granules'] += 1
            granule_month_map[granule_agg_month]['n_granules'] += 1

            granule_unique_ids = tuple([group_dict[grp] for grp in unique_fields])

            if granule_unique_ids in unique_granules:
                granule_month_map[granule_agg_month]['n_duplicates'] += 1
                aqc_date_map[granule_agg_day]['n_duplicates'] += 1
                first_duplicate = unique_granules[granule_unique_ids]

                if first_duplicate[1] not in granule_month_map[granule_agg_month]['duplicates']:
                    granule_month_map[granule_agg_month]['duplicates'][first_duplicate[1]] = [first_duplicate[0]]

                if first_duplicate[1] not in aqc_date_map[granule_agg_day]['duplicates']:
                    aqc_date_map[granule_agg_day]['duplicates'][first_duplicate[1]] = [first_duplicate[0]]

                granule_month_map[granule_agg_month]['duplicates'][first_duplicate[1]].append(granule_id)
                aqc_date_map[granule_agg_day]['duplicates'][first_duplicate[1]].append(granule_id)
            else:
                unique_granules[granule_unique_ids] = (granule_id, repr(granule_unique_ids))

        # Garbage collection periodically
        if batch_start > 0 and batch_start % (batch_size * 5) == 0:
            gc.collect()

    del granule_ids
    gc.collect()

    if parse_failures > 0:
        logger.warning(f"Failed to parse {parse_failures} granule IDs")

    # Post-process: Sort duplicates by creation timestamp
    if creation_field:
        for month in granule_month_map:
            for duplicate in granule_month_map[month]['duplicates']:
                duplicate_granule_ids = granule_month_map[month]['duplicates'][duplicate]
                duplicate_granule_ids.sort(
                    key=lambda x: pattern.match(x).groupdict()[creation_field], reverse=True
                )

                granule_month_map[month]['duplicates'][duplicate] = {
                    'latest_product': duplicate_granule_ids[0],
                    'duplicate_products': duplicate_granule_ids[1:],
                }

        for date in aqc_date_map:
            for duplicate in aqc_date_map[date]['duplicates']:
                duplicate_granule_ids = aqc_date_map[date]['duplicates'][duplicate]
                duplicate_granule_ids.sort(
                    key=lambda x: pattern.match(x).groupdict()[creation_field], reverse=True
                )

                aqc_date_map[date]['duplicates'][duplicate] = {
                    'latest_product': duplicate_granule_ids[0],
                    'duplicate_products': duplicate_granule_ids[1:],
                }

    granule_month_map = dict(sorted(granule_month_map.items()))
    aqc_date_map = dict(sorted(aqc_date_map.items()))

    for month in granule_month_map.keys():
        n_gran = granule_month_map[month]['n_granules']
        granule_month_map[month]['percent_duplicates'] = (
            (granule_month_map[month]['n_duplicates'] / n_gran) * 100 if n_gran > 0 else 0.0
        )

    for date in aqc_date_map.keys():
        n_gran = aqc_date_map[date]['n_granules']
        aqc_date_map[date]['percent_duplicates'] = (
            (aqc_date_map[date]['n_duplicates'] / n_gran) * 100 if n_gran > 0 else 0.0
        )

    # Calculate totals and stats
    n_duplicates = sum([month['n_duplicates'] for month in granule_month_map.values()])
    n_granules = total_products
    n_unique = len(unique_granules)
    
    if n_granules > 0:
        logger.info(f'Found {n_duplicates} duplicate granule IDs out of {n_granules} granules '
                    f'({(n_duplicates / n_granules) * 100:.1f}%)')
    else:
        logger.info('No granules found in date range')

    # Calculate duplicate counts per granule
    if creation_field:
        duplicate_counts = list(chain.from_iterable(
            [len(dup['duplicate_products']) for dup in month['duplicates'].values()]
            for month in granule_month_map.values()
        ))
    else:
        duplicate_counts = list(chain.from_iterable(
            [len(dup) for dup in month['duplicates'].values()]
            for month in granule_month_map.values()
        ))

    # Build flat duplicate list
    if creation_field:
        duplicate_list = []
        for month_data in granule_month_map.values():
            for dup_group in month_data['duplicates'].values():
                duplicate_list.extend(dup_group['duplicate_products'])
    else:
        duplicate_list = []
        for month_data in granule_month_map.values():
            for dup_group in month_data['duplicates'].values():
                duplicate_list.extend(dup_group[1:])

    return {
        # Riley's original format
        'granule_month_map': granule_month_map,
        'aqc_date_map': aqc_date_map,
        'by_date': aqc_date_map,  # Alias for tests/CLI compatibility
        # CLI compatibility fields
        'total': n_granules,
        'unique': n_unique,
        'duplicates': n_duplicates,
        'duplicate_list': sorted(duplicate_list),
        # Stats for reporting
        'min_duplicates_per_granule': min(duplicate_counts) if len(duplicate_counts) > 0 else None,
        'max_duplicates_per_granule': max(duplicate_counts) if len(duplicate_counts) > 0 else None,
        'avg_duplicates_per_granule': sum(duplicate_counts) / len(duplicate_counts) if len(duplicate_counts) > 0 else None,
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
