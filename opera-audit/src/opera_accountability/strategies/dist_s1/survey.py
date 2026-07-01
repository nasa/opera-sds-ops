from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

from ... import CONFIG
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


def survey_rtc(start: Optional[datetime], end: Optional[datetime], venue: str = "PROD") -> list[dict]:
    ccid = CONFIG["products"]["RTC_S1"]["ccid"][venue]
    pattern = re.compile(CONFIG["products"]["RTC_S1"]["pattern"])
    unique_fields = tuple(CONFIG["products"]["RTC_S1"]["unique_fields"])

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
) -> tuple[list[dict], set[str]]:
    cfg = CONFIG["products"]["DIST_S1"]
    ccid = cfg["ccid"].get(venue)
    if ccid:
        cmr_records = query_cmr(ccid, start, end, venue)
    else:
        collection = cfg["collection"][venue]
        cmr_records = query_cmr_by_short_name(
            collection["short_name"],
            provider=collection.get("provider"),
            start_date=start,
            end_date=end,
            venue=venue,
        )

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
) -> tuple[list[dict], set[str]]:
    return asyncio.run(
        survey_dist_async(start, end, venue, max_concurrent, max_retries, prefer_s3)
    )
