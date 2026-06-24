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

    Records whose granule ID does not match ``pattern`` are logged at WARNING
    level and skipped. This keeps the survey resilient to unexpected CMR
    records (e.g. new product versions, off-pattern IDs) rather than aborting
    the entire pipeline — mirroring ``duplicates.detect_duplicates``.
    """
    latest: dict[tuple, dict] = {}
    skipped = 0
    for item in items:
        match = pattern.match(item['id'])
        if match is None:
            skipped += 1
            logger.warning(
                "Skipping granule with ID that does not match %s: %s",
                pattern.pattern, item['id'],
            )
            continue
        groups = match.groupdict()
        key = tuple(groups[f] for f in unique_fields)
        incoming_creation = groups['creation_ts']
        existing = latest.get(key)
        if existing is None or incoming_creation > existing['_creation_ts']:
            latest[key] = {**item, '_creation_ts': incoming_creation}
    if skipped:
        logger.warning(
            "Skipped %d / %d records with unparseable granule IDs",
            skipped, len(items),
        )
    # Drop the internal sort key before returning.
    for record in latest.values():
        record.pop('_creation_ts', None)
    return list(latest.values())


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
