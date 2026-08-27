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
from ...checkpoint import CheckpointStore, collect_chunked_records, generate_time_chunks
from ...cmr import query_cmr
from .rtc_utils import reduce_input_rtc_list

logger = logging.getLogger(__name__)


def _dedupe_by_creation_ts(
    items: list[dict],
    pattern: re.Pattern,
    unique_fields: tuple[str, ...],
) -> list[dict]:
    """Keep the record with the newest ``creation_ts`` for each unique-field tuple.

    Based on Riley's survey() deduplication logic:
    - Skips + logs ERROR for granule IDs that do not match pattern
    - Groups by unique-field tuple
    - Sorts by creation_ts (reverse=True) and keeps first
    """
    grouping_products_map = {}
    parse_failures = 0
    total_items = len(items)
    dedup_progress = max(100_000, total_items // 10)

    for idx, item in enumerate(items):
        if idx > 0 and idx % dedup_progress == 0:
            logger.info(
                "  ... dedup progress: %d / %d records (%d unique groups so far)",
                idx, total_items, len(grouping_products_map),
            )
        granule_id = item["id"]
        match = pattern.match(granule_id)

        if match is None:
            parse_failures += 1
            logger.error(
                "Granule ID does not match expected naming spec: %s "
                "(pattern: %s) — this indicates a non-conformant record in CMR",
                granule_id, pattern.pattern,
            )
            continue

        group_dict = match.groupdict()

        id_tuple = tuple([group_dict[grp] for grp in unique_fields])
        item["_timestamp"] = group_dict["creation_ts"]

        if id_tuple not in grouping_products_map:
            grouping_products_map[id_tuple] = []
        grouping_products_map[id_tuple].append(item)

    if parse_failures > 0:
        logger.error(
            "%d of %d granule ID(s) did not match the expected naming pattern — "
            "skipped; these may indicate a collection-level issue in CMR",
            parse_failures, len(items),
        )

    total_groups = len(grouping_products_map)
    dedup_sort_progress = max(100_000, total_groups // 10)
    for idx, id_tuple in enumerate(grouping_products_map):
        grouping_products_map[id_tuple].sort(key=lambda x: x["_timestamp"], reverse=True)
        grouping_products_map[id_tuple] = grouping_products_map[id_tuple][0]
        del grouping_products_map[id_tuple]["_timestamp"]
        if idx > 0 and idx % dedup_sort_progress == 0:
            logger.info("  ... dedup final pass: %d / %d groups", idx, total_groups)
    logger.info("Dedup final pass complete: %d unique groups", total_groups)

    return list(grouping_products_map.values())


def survey_rtc(
    start: Optional[datetime],
    end: Optional[datetime],
    venue: str = "PROD",
    checkpoint: Optional[CheckpointStore] = None,
    chunk_days: Optional[int] = None,
    materialize: bool = True,
) -> Optional[list[dict]]:
    """Query CMR for RTC-S1 granules and dedupe by ``(burst_id, acq_ts, sensor)``.

    Returns a list of ``{"id": <granule_id>, "revision_timestamp": <iso>}``.
    """
    # Use RTC_S1.ccid as the single source of truth — previously a
    # DSWX_S1.accountability.rtc_s1_ccid block duplicated this value and
    # invited silent drift.
    ccid = CONFIG["products"]["RTC_S1"]["ccid"][venue]
    pattern = re.compile(CONFIG["products"]["RTC_S1"]["pattern"])
    unique_fields = tuple(CONFIG["products"]["RTC_S1"]["unique_fields"])

    logger.info("Surveying RTC-S1 granules (ccid=%s, venue=%s)", ccid, venue)
    if checkpoint is not None and start is not None and end is not None:
        def project(record: dict):
            granule_id = record["umm"]["GranuleUR"]
            return granule_id, {
                "id": granule_id,
                "revision_timestamp": record["meta"]["revision-date"],
            }

        collect_chunked_records(
            store=checkpoint,
            namespace="rtc_survey",
            chunks=generate_time_chunks(start, end, chunk_days),
            query=lambda chunk_start, chunk_end: query_cmr(
                ccid, chunk_start, chunk_end, venue
            ),
            project=project,
        )
        if not materialize:
            return None
        shaped = list(checkpoint.iter_payloads("rtc_survey"))
    else:
        cmr_records = query_cmr(ccid, start, end, venue)
        # Shape to the intermediate form used by Riley's survey: id + revision_timestamp.
        shaped = [
            {
                "id": r["umm"]["GranuleUR"],
                "revision_timestamp": r["meta"]["revision-date"],
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
    venue: str = "PROD",
    checkpoint: Optional[CheckpointStore] = None,
    chunk_days: Optional[int] = None,
    materialize: bool = True,
) -> Optional[list[dict]]:
    """Query CMR for DSWx-S1 granules and dedupe by ``(tile_id, acq_ts, sensor)``.

    Returns a list of ``{"id": <granule_id>, "input_rtcs": [<rtc_id>, ...]}``.
    """
    ccid = CONFIG["products"]["DSWX_S1"]["ccid"][venue]
    pattern = re.compile(CONFIG["products"]["DSWX_S1"]["pattern"])
    unique_fields = tuple(CONFIG["products"]["DSWX_S1"]["unique_fields"])

    logger.info("Surveying DSWx-S1 granules (ccid=%s, venue=%s)", ccid, venue)
    if checkpoint is not None and start is not None and end is not None:
        def project(record: dict):
            granule_id = record["umm"]["GranuleUR"]
            return granule_id, {
                "id": granule_id,
                "input_rtcs": reduce_input_rtc_list(
                    record["umm"].get("InputGranules", [])
                ),
            }

        collect_chunked_records(
            store=checkpoint,
            namespace="dswx_survey",
            chunks=generate_time_chunks(start, end, chunk_days),
            query=lambda chunk_start, chunk_end: query_cmr(
                ccid, chunk_start, chunk_end, venue
            ),
            project=project,
        )
        if not materialize:
            return None
        shaped = list(checkpoint.iter_payloads("dswx_survey"))
    else:
        cmr_records = query_cmr(ccid, start, end, venue)
        shaped = [
            {
                "id": r["umm"]["GranuleUR"],
                "input_rtcs": reduce_input_rtc_list(
                    r["umm"].get("InputGranules", [])
                ),
            }
            for r in cmr_records
        ]
    logger.info("Fetched %d raw DSWx-S1 records; deduping by %s", len(shaped), unique_fields)

    deduped = _dedupe_by_creation_ts(shaped, pattern, unique_fields)
    logger.info("DSWx-S1 survey complete: %d unique granules", len(deduped))
    return deduped
