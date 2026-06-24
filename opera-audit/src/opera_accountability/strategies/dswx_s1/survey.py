"""Step 1 of the DSWx-S1 accountability pipeline: CMR survey with dedup.

Queries CMR for RTC-S1 and DSWx-S1 granules over a time range, then dedupes
by unique-fields keeping the granule with the latest ``creation_ts``. Port of
``accountability_tools/dswx_s1/survey.py``, refactored to reuse
:func:`opera_accountability.cmr.query_cmr` rather than re-implementing the
CMR client.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional

from ... import CONFIG
from ...cmr import query_cmr
from .rtc_utils import reduce_input_rtc_list

logger = logging.getLogger(__name__)


def _dedupe_by_creation_ts(
    items: list[dict],
    pattern: re.Pattern,
    unique_fields: tuple[str, ...],
) -> list[dict]:
    """Keep the record with the newest ``creation_ts`` for each unique-field tuple.

    Exact port of Riley's survey() deduplication logic:
    - Raises RuntimeError if granule ID does not match pattern
    - Groups by unique-field tuple
    - Sorts by creation_ts (reverse=True) and keeps first
    """
    grouping_products_map = {}

    for item in items:
        granule_id = item['id']
        match = pattern.match(granule_id)

        if match is None:
            raise RuntimeError(f'Failed to parse granule ID {granule_id} with pattern {pattern.pattern}')

        group_dict = match.groupdict()

        id_tuple = tuple([group_dict[grp] for grp in unique_fields])
        item['_timestamp'] = group_dict['creation_ts']

        if id_tuple not in grouping_products_map:
            grouping_products_map[id_tuple] = []
        grouping_products_map[id_tuple].append(item)

    for id_tuple in grouping_products_map:
        grouping_products_map[id_tuple].sort(key=lambda x: x['_timestamp'], reverse=True)
        grouping_products_map[id_tuple] = grouping_products_map[id_tuple][0]
        del grouping_products_map[id_tuple]['_timestamp']

    return list(grouping_products_map.values())


def survey_rtc(
    start: Optional[datetime],
    end: Optional[datetime],
    venue: str = 'PROD',
) -> list[dict]:
    """Query CMR for RTC-S1 granules and dedupe by ``(burst_id, acq_ts, sensor)``.

    Returns a list of ``{"id": <granule_id>, "revision_timestamp": <iso>}``.
    """
    # Use RTC_S1.ccid as the single source of truth — previously a
    # DSWX_S1.accountability.rtc_s1_ccid block duplicated this value and
    # invited silent drift.
    ccid = CONFIG['products']['RTC_S1']['ccid'][venue]
    pattern = re.compile(CONFIG['products']['RTC_S1']['pattern'])
    unique_fields = tuple(CONFIG['products']['RTC_S1']['unique_fields'])

    logger.info("Surveying RTC-S1 granules (ccid=%s, venue=%s)", ccid, venue)
    cmr_records = query_cmr(ccid, start, end, venue)

    # Shape to the intermediate form used by Riley's survey: id + revision_timestamp.
    shaped = [
        {
            'id': r['umm']['GranuleUR'],
            'revision_timestamp': r['meta']['revision-date'],
        }
        for r in cmr_records
    ]
    logger.info("Fetched %d raw RTC-S1 records; deduping by %s", len(shaped), unique_fields)

    deduped = _dedupe_by_creation_ts(shaped, pattern, unique_fields)
    logger.info("RTC-S1 survey complete: %d unique granules", len(deduped))
    return deduped


def survey_dswx(
    start: Optional[datetime],
    end: Optional[datetime],
    venue: str = 'PROD',
) -> list[dict]:
    """Query CMR for DSWx-S1 granules and dedupe by ``(tile_id, acq_ts, sensor)``.

    Returns a list of ``{"id": <granule_id>, "input_rtcs": [<rtc_id>, ...]}``.
    """
    ccid = CONFIG['products']['DSWX_S1']['ccid'][venue]
    pattern = re.compile(CONFIG['products']['DSWX_S1']['pattern'])
    unique_fields = tuple(CONFIG['products']['DSWX_S1']['unique_fields'])

    logger.info("Surveying DSWx-S1 granules (ccid=%s, venue=%s)", ccid, venue)
    cmr_records = query_cmr(ccid, start, end, venue)

    shaped = [
        {
            'id': r['umm']['GranuleUR'],
            'input_rtcs': reduce_input_rtc_list(r['umm'].get('InputGranules', [])),
        }
        for r in cmr_records
    ]
    logger.info("Fetched %d raw DSWx-S1 records; deduping by %s", len(shaped), unique_fields)

    deduped = _dedupe_by_creation_ts(shaped, pattern, unique_fields)
    logger.info("DSWx-S1 survey complete: %d unique granules", len(deduped))
    return deduped
