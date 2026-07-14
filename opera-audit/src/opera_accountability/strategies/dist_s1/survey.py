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
    for record in records:
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
    ccid = CONFIG["products"]["RTC_S1"]["ccid"][venue]
    pattern = re.compile(CONFIG["products"]["RTC_S1"]["pattern"])
    unique_fields = tuple(CONFIG["products"]["RTC_S1"]["unique_fields"])

    if checkpoint is not None and start is not None and end is not None:
        for chunk in generate_time_chunks(start, end, chunk_days):
            if checkpoint.is_chunk_complete("rtc_survey", chunk):
                continue
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
        shaped = list(checkpoint.iter_payloads("rtc_survey"))
    else:
        cmr_records = query_cmr(ccid, start, end, venue)
        shaped = [
            {
                "id": granule_id,
                "revision_timestamp": record.get("meta", {}).get("revision-date"),
            }
            for record in cmr_records
            if (granule_id := _native_id(record))
        ]
    return _dedupe_by_creation_ts(shaped, pattern, unique_fields)


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
        for chunk in generate_time_chunks(start, end, chunk_days):
            if checkpoint.is_chunk_complete("dist_survey", chunk):
                continue
            cmr_records = query_range(chunk.start, chunk.end)
            semaphore = asyncio.Semaphore(max_concurrent)
            tasks = [
                _fetch_dist_product_inputs(
                    product, semaphore, max_retries, prefer_s3
                )
                for product in cmr_records
            ]
            loaded = await asyncio.gather(*tasks)
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
        return results, existing_tile_times

    cmr_records = query_range(start, end)

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
    results = await asyncio.gather(*tasks)
    return [result for result in results if result is not None], existing_tile_times


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
