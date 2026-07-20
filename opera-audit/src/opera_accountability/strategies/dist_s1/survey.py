from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

from ... import CONFIG
from ...checkpoint import CheckpointStore, generate_time_chunks
from ...cmr import query_cmr, query_cmr_by_short_name
from ..dswx_s1.rtc_utils import reduce_input_rtc_list
from .iso_xml import extract_dist_input_granules, extract_iso_xml_url, obtain_iso_xml
from .utils import normalize_tile_time_key, parse_dist_s1_native_id

logger = logging.getLogger(__name__)


def _native_id(record: dict) -> Optional[str]:
    return (
        record.get("umm", {}).get("GranuleUR")
        or record.get("meta", {}).get("native-id")
    )


def _dedupe_by_creation_ts(records: list[dict], pattern: re.Pattern, unique_fields: tuple[str, ...]) -> list[dict]:
    latest: dict[tuple, dict] = {}
    total_records = len(records)
    dedup_progress = max(100_000, total_records // 10)
    for idx, record in enumerate(records):
        match = pattern.match(record["id"])
        if match is None:
            logger.warning("Skipping granule with unparseable ID: %s", record["id"])
            continue
        groups = match.groupdict()
        key = tuple(groups[field] for field in unique_fields)
        creation_ts = groups.get("creation_ts", "")
        existing = latest.get(key)
        if existing is None or creation_ts > existing["_creation_ts"]:
            latest[key] = {**record, "_creation_ts": creation_ts}
        if idx > 0 and idx % dedup_progress == 0:
            logger.info("  ... dedup grouping: %d / %d records (%d unique so far)", idx, total_records, len(latest))
    logger.info("Dedup grouping complete: %d records -> %d unique groups", total_records, len(latest))
    for record in latest.values():
        record.pop("_creation_ts", None)
    return list(latest.values())


def survey_rtc(
    start: Optional[datetime],
    end: Optional[datetime],
    venue: str = "PROD",
    checkpoint: Optional[CheckpointStore] = None,
    chunk_days: Optional[int] = None,
) -> list[dict]:
    logger.info("Surveying RTC-S1 products for DIST-S1 (venue=%s)", venue)
    ccid = CONFIG["products"]["RTC_S1"]["ccid"][venue]
    pattern = re.compile(CONFIG["products"]["RTC_S1"]["pattern"])
    unique_fields = tuple(CONFIG["products"]["RTC_S1"]["unique_fields"])

    if checkpoint is not None and start is not None and end is not None:
        chunks_list = list(generate_time_chunks(start, end, chunk_days))
        for ci, chunk in enumerate(chunks_list):
            if checkpoint.is_chunk_complete("rtc_survey", chunk):
                logger.info(
                    "[rtc_survey] chunk %d/%d SKIP (already complete): %s -> %s",
                    ci + 1, len(chunks_list), chunk.start.date(), chunk.end.date(),
                )
                continue
            logger.info(
                "[rtc_survey] chunk %d/%d RUN: %s -> %s (querying CMR...)",
                ci + 1, len(chunks_list), chunk.start.date(), chunk.end.date(),
            )
            cmr_records = query_cmr(ccid, chunk.start, chunk.end, venue)
            shaped_chunk = [
                (
                    granule_id,
                    {
                        "id": granule_id,
                        "revision_timestamp": record.get("meta", {}).get("revision-date"),
                    },
                )
                for record in cmr_records
                if (granule_id := _native_id(record))
            ]
            checkpoint.commit_chunk(
                "rtc_survey", chunk, shaped_chunk, fetched_count=len(cmr_records)
            )
            logger.info(
                "[rtc_survey] chunk %d/%d DONE: %d fetched (cumulative stored: %d)",
                ci + 1, len(chunks_list), len(cmr_records),
                checkpoint.count_records("rtc_survey"),
            )
        shaped = list(checkpoint.iter_payloads("rtc_survey"))
        logger.info("RTC-S1 survey (checkpointed): %d raw records loaded", len(shaped))
    else:
        cmr_records = query_cmr(ccid, start, end, venue)
        logger.info("RTC-S1 survey: fetched %d raw records from CMR", len(cmr_records))
        shaped = [
            {
                "id": granule_id,
                "revision_timestamp": record.get("meta", {}).get("revision-date"),
            }
            for record in cmr_records
            if (granule_id := _native_id(record))
        ]
    deduped = _dedupe_by_creation_ts(shaped, pattern, unique_fields)
    logger.info("RTC-S1 survey complete: %d unique records after dedup", len(deduped))
    return deduped


async def _fetch_dist_product_inputs(
    product: dict,
    semaphore: asyncio.Semaphore,
    max_retries: int,
    prefer_s3: bool,
) -> Optional[dict]:
    async with semaphore:
        native_id = _native_id(product)
        if not native_id:
            logger.warning("Skipping DIST-S1 record with missing native ID")
            return None
        try:
            iso_xml_url = extract_iso_xml_url(product, prefer_s3=prefer_s3)
            root = await asyncio.to_thread(obtain_iso_xml, iso_xml_url, max_retries)
            input_rtcs = reduce_input_rtc_list(extract_dist_input_granules(root))
            return {
                "id": native_id,
                "input_rtcs": sorted(input_rtcs),
                "iso_xml_url": iso_xml_url,
            }
        except Exception as err:
            logger.error("Unable to obtain ISO XML for %s: %s", native_id, err)
            return None


async def survey_dist_async(
    start: Optional[datetime],
    end: Optional[datetime],
    venue: str = "PROD",
    max_concurrent: int = 10,
    max_retries: int = 3,
    prefer_s3: bool = False,
    checkpoint: Optional[CheckpointStore] = None,
    chunk_days: Optional[int] = None,
) -> tuple[list[dict], set[str]]:
    logger.info(
        "Surveying DIST-S1 products (venue=%s, max_concurrent=%d, prefer_s3=%s)",
        venue, max_concurrent, prefer_s3,
    )
    cfg = CONFIG["products"]["DIST_S1"]
    ccid = cfg["ccid"].get(venue)
    def query_range(range_start, range_end):
        if ccid:
            return query_cmr(ccid, range_start, range_end, venue)
        collection = cfg["collection"][venue]
        return query_cmr_by_short_name(
            collection["short_name"],
            provider=collection.get("provider"),
            start_date=range_start,
            end_date=range_end,
            venue=venue,
        )

    if checkpoint is not None and start is not None and end is not None:
        chunks_list = list(generate_time_chunks(start, end, chunk_days))
        for ci, chunk in enumerate(chunks_list):
            if checkpoint.is_chunk_complete("dist_survey", chunk):
                logger.info(
                    "[dist_survey] chunk %d/%d SKIP (already complete): %s -> %s",
                    ci + 1, len(chunks_list), chunk.start.date(), chunk.end.date(),
                )
                continue
            logger.info(
                "[dist_survey] chunk %d/%d RUN: %s -> %s (querying + ISO XML download)",
                ci + 1, len(chunks_list), chunk.start.date(), chunk.end.date(),
            )
            cmr_records = query_range(chunk.start, chunk.end)
            logger.info(
                "[dist_survey] chunk %d/%d: fetched %d CMR records, starting ISO XML download",
                ci + 1, len(chunks_list), len(cmr_records),
            )
            semaphore = asyncio.Semaphore(max_concurrent)
            tasks = [
                _fetch_dist_product_inputs(
                    product, semaphore, max_retries, prefer_s3
                )
                for product in cmr_records
            ]
            loaded = await asyncio.gather(*tasks)
            logger.info(
                "[dist_survey] chunk %d/%d: ISO XML download complete (%d succeeded, %d failed)",
                ci + 1, len(chunks_list),
                sum(1 for r in loaded if r is not None),
                sum(1 for r in loaded if r is None),
            )
            loaded_by_id = {
                result["id"]: result for result in loaded if result is not None
            }
            projected = []
            for product in cmr_records:
                native_id = _native_id(product)
                if not native_id:
                    continue
                tile_id, acq_time = parse_dist_s1_native_id(native_id)
                existing_key = (
                    normalize_tile_time_key(tile_id, acq_time)
                    if tile_id and acq_time
                    else None
                )
                projected.append(
                    (
                        native_id,
                        {
                            "id": native_id,
                            "loaded": native_id in loaded_by_id,
                            "input_rtcs": loaded_by_id.get(native_id, {}).get(
                                "input_rtcs", []
                            ),
                            "iso_xml_url": loaded_by_id.get(native_id, {}).get(
                                "iso_xml_url"
                            ),
                            "existing_tile_time": existing_key,
                        },
                    )
                )
            checkpoint.commit_chunk(
                "dist_survey", chunk, projected, fetched_count=len(cmr_records)
            )

        payloads = list(checkpoint.iter_payloads("dist_survey"))
        existing_tile_times = {
            payload["existing_tile_time"]
            for payload in payloads
            if payload.get("existing_tile_time")
        }
        results = [
            {
                "id": payload["id"],
                "input_rtcs": payload.get("input_rtcs", []),
                "iso_xml_url": payload.get("iso_xml_url"),
            }
            for payload in payloads
            if payload.get("loaded")
        ]
        logger.info(
            "DIST-S1 survey (checkpointed): %d products loaded, %d existing tile-times",
            len(results), len(existing_tile_times),
        )
        return results, existing_tile_times

    cmr_records = query_range(start, end)
    logger.info("DIST-S1 survey (non-checkpointed): %d CMR records, downloading ISO XML", len(cmr_records))

    existing_tile_times = set()
    for product in cmr_records:
        native_id = _native_id(product)
        if not native_id:
            continue
        tile_id, acq_time = parse_dist_s1_native_id(native_id)
        if tile_id and acq_time:
            existing_tile_times.add(normalize_tile_time_key(tile_id, acq_time))

    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [
        _fetch_dist_product_inputs(product, semaphore, max_retries, prefer_s3)
        for product in cmr_records
    ]
    raw_results = await asyncio.gather(*tasks)
    results = [result for result in raw_results if result is not None]
    logger.info(
        "DIST-S1 survey complete: %d products with ISO XML (%d existing tile-times)",
        len(results), len(existing_tile_times),
    )
    return results, existing_tile_times


def survey_dist(
    start: Optional[datetime],
    end: Optional[datetime],
    venue: str = "PROD",
    max_concurrent: int = 10,
    max_retries: int = 3,
    prefer_s3: bool = False,
    checkpoint: Optional[CheckpointStore] = None,
    chunk_days: Optional[int] = None,
) -> tuple[list[dict], set[str]]:
    return asyncio.run(
        survey_dist_async(
            start,
            end,
            venue,
            max_concurrent,
            max_retries,
            prefer_s3,
            checkpoint,
            chunk_days,
        )
    )
