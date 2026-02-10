"""Duplicate detection logic for OPERA products."""

import re
import logging
from datetime import datetime
from collections import defaultdict
from typing import Any

from . import CONFIG

logger = logging.getLogger(__name__)


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
