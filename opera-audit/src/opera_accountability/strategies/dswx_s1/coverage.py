"""Step 5 of DSWx-S1 accountability: validate real RTC burst coverage.

Cycle expansion identifies tile-set/cycle/sensor buckets associated with RTCs
that were not used by a DSWx-S1 product. A bucket is only actionable when the
complete acquisition had enough RTC burst coverage to trigger DSWx-S1. This
module checks that condition and reduces overlapping valid buckets to a small,
deterministic set of representative RTC recovery candidates.
"""

from __future__ import annotations

import ast
import json
import logging
import sqlite3
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

from ... import CONFIG
from ...cmr import query_cmr_by_native_id_patterns
from .rtc_utils import rtc_to_id_tuple

logger = logging.getLogger(__name__)

_BURSTS_QUERY = "SELECT bursts FROM mgrs_burst_db WHERE mgrs_set_id = ?"


def _settings() -> dict:
    return (
        CONFIG["products"]["DSWX_S1"]["accountability"].get("coverage_validation")
        or {}
    )


def empty_result(threshold: Optional[int] = None) -> dict:
    """Return an empty coverage result using the configured threshold."""
    if threshold is None:
        threshold = int(_settings().get("min_rtc_count", 4))
    return {
        "threshold": threshold,
        "total_buckets": 0,
        "valid_count": 0,
        "dropped_count": 0,
        "valid": {},
        "dropped": {},
        "reduced": {},
    }


def _parse_bursts(raw: str, tile_set_id: str) -> list[str]:
    """Parse and normalize an MGRS DB burst list to product-ID tokens."""
    try:
        values = json.loads(raw)
    except json.JSONDecodeError:
        try:
            values = ast.literal_eval(raw)
        except (ValueError, SyntaxError) as err:
            raise ValueError(
                f"Invalid bursts value for MGRS tile set {tile_set_id}"
            ) from err

    if not isinstance(values, list):
        raise ValueError(f"Expected a burst list for MGRS tile set {tile_set_id}")
    return sorted({str(value).replace("_", "-").upper() for value in values})


def _load_tile_bursts(
    mgrs_db_path: str | Path,
    tile_set_ids: set[str],
) -> dict[str, list[str]]:
    """Load expected RTC burst IDs for every requested MGRS tile set."""
    result: dict[str, list[str]] = {}
    total_ids = len(tile_set_ids)
    load_progress = max(1000, total_ids // 10)
    logger.info("Loading burst IDs for %d MGRS tile sets from DB", total_ids)
    with closing(sqlite3.connect(str(mgrs_db_path))) as conn:
        cursor = conn.cursor()
        for idx, tile_set_id in enumerate(sorted(tile_set_ids)):
            cursor.execute(_BURSTS_QUERY, (tile_set_id,))
            row = cursor.fetchone()
            if row is None:
                raise ValueError(
                    f"MGRS tile set {tile_set_id} is missing from {mgrs_db_path}"
                )
            result[tile_set_id] = _parse_bursts(row[0], tile_set_id)
            if idx > 0 and idx % load_progress == 0:
                logger.info("  ... loaded burst IDs for %d / %d tile sets", idx, total_ids)
    logger.info("Loaded burst IDs for all %d tile sets", total_ids)
    return result


def _cmr_native_id(item: dict) -> Optional[str]:
    return (
        item.get("meta", {}).get("native-id")
        or item.get("umm", {}).get("GranuleUR")
        or item.get("id")
    )


def _dedupe_rtc_ids(
    items: list[dict],
    sensor: Optional[str] = None,
) -> list[str]:
    """Return one RTC ID per burst/acquisition/sensor tuple."""
    by_key: dict[tuple[str, str, str], str] = {}
    for item in items:
        rtc_id = _cmr_native_id(item)
        if not rtc_id:
            logger.error("CMR RTC result has no native ID: %s", item)
            continue
        try:
            key = rtc_to_id_tuple(rtc_id)
        except ValueError:
            logger.error(
                "CMR returned a non-conformant RTC ID during coverage validation: %s",
                rtc_id,
            )
            continue
        if sensor is not None and key[2] != sensor:
            continue
        # Prefer a deterministic ID when multiple revisions share the same
        # burst/acquisition/sensor tuple. Coverage only needs the unique count.
        by_key[key] = max(rtc_id, by_key.get(key, rtc_id))
    return sorted(by_key.values())


def _validate_one(
    bucket_key: str,
    candidate_rtc_ids: list[str],
    expected_bursts: list[str],
    *,
    rtc_collection_id: str,
    venue: str,
    threshold: int,
    temporal_window_hours: float,
    query_func: Callable[..., list[dict]],
) -> tuple[str, bool, dict]:
    candidate_rtc_ids = sorted(set(candidate_rtc_ids))
    if not candidate_rtc_ids:
        raise ValueError(f"Coverage bucket {bucket_key} contains no RTC IDs")

    if len(candidate_rtc_ids) >= threshold:
        matching_rtc_ids = candidate_rtc_ids
        source = "identified_missing_rtcs"
    else:
        _, acquisition_ts, sensor = rtc_to_id_tuple(candidate_rtc_ids[0])
        acquisition = datetime.strptime(acquisition_ts, "%Y%m%dT%H%M%SZ")
        delta = timedelta(hours=temporal_window_hours)
        patterns = [f"OPERA_L2_RTC-S1_{burst_id}_*" for burst_id in expected_bursts]
        cmr_items = query_func(
            collection_id=rtc_collection_id,
            native_id_patterns=patterns,
            start_date=acquisition - delta,
            end_date=acquisition + delta,
            venue=venue,
        )
        matching_rtc_ids = _dedupe_rtc_ids(cmr_items, sensor=sensor)
        source = "cmr"

    detail = {
        "coverage": len(matching_rtc_ids),
        "expected_burst_count": len(expected_bursts),
        "candidate_rtc_ids": candidate_rtc_ids,
        "matching_rtc_ids": matching_rtc_ids,
        "representative_rtc_id": candidate_rtc_ids[0],
        "source": source,
    }
    return bucket_key, len(matching_rtc_ids) >= threshold, detail


def reduce_valid_candidates(valid: dict[str, dict]) -> dict[str, list[str]]:
    """Greedily reduce valid buckets to deterministic representative RTC IDs."""
    rtc_to_buckets: dict[str, set[str]] = {}
    for bucket_key, detail in valid.items():
        for rtc_id in detail["candidate_rtc_ids"]:
            rtc_to_buckets.setdefault(rtc_id, set()).add(bucket_key)

    initial_rtc_count = len(rtc_to_buckets)
    logger.info("Reducing %d candidate RTCs across %d valid buckets", initial_rtc_count, len(valid))
    reduce_progress = max(1000, initial_rtc_count // 10)
    reduced: dict[str, list[str]] = {}
    iteration = 0
    while rtc_to_buckets:
        # Pick the RTC covering the most remaining buckets; lexical tie-break
        # makes report output stable across runs and worker completion order.
        rtc_id = min(
            rtc_to_buckets,
            key=lambda key: (-len(rtc_to_buckets[key]), key),
        )
        covered = rtc_to_buckets.pop(rtc_id)
        reduced[rtc_id] = sorted(covered)

        for other_rtc in list(rtc_to_buckets):
            rtc_to_buckets[other_rtc].difference_update(covered)
            if not rtc_to_buckets[other_rtc]:
                del rtc_to_buckets[other_rtc]

        iteration += 1
        if iteration % reduce_progress == 0:
            logger.info(
                "  ... reduction iteration %d: %d RTCs remaining, %d selected so far",
                iteration, len(rtc_to_buckets), len(reduced),
            )
    logger.info("Reduction complete: %d representative RTCs selected from %d candidates", len(reduced), initial_rtc_count)
    return reduced


def validate_cycle_coverage(
    cycle_map: dict[str, list[str]],
    mgrs_db_path: str | Path,
    venue: str = "PROD",
    *,
    threshold: Optional[int] = None,
    temporal_window_hours: Optional[float] = None,
    workers: Optional[int] = None,
    query_func: Callable[..., list[dict]] = query_cmr_by_native_id_patterns,
) -> dict:
    """Classify cycle buckets by real RTC coverage and reduce valid triggers.

    CMR or database failures are intentionally propagated. Treating a failed
    query as zero coverage would silently drop real recovery candidates.
    """
    settings = _settings()
    threshold = int(
        threshold if threshold is not None else settings.get("min_rtc_count", 4)
    )
    temporal_window_hours = float(
        temporal_window_hours
        if temporal_window_hours is not None
        else settings.get("temporal_window_hours", 1)
    )
    workers = int(workers if workers is not None else settings.get("workers", 8))

    if threshold < 1:
        raise ValueError("DSWx-S1 coverage threshold must be at least 1")
    if temporal_window_hours <= 0:
        raise ValueError("DSWx-S1 coverage temporal window must be positive")
    if workers < 1:
        raise ValueError("DSWx-S1 coverage worker count must be at least 1")
    if not cycle_map:
        return empty_result(threshold)

    rtc_collection_id = CONFIG["products"]["RTC_S1"]["ccid"].get(venue)
    if not rtc_collection_id:
        raise ValueError(f"No RTC_S1 CCID configured for venue {venue}")

    tile_set_ids = {key.split("$", 1)[0] for key in cycle_map}
    tile_bursts = _load_tile_bursts(mgrs_db_path, tile_set_ids)
    valid: dict[str, dict] = {}
    dropped: dict[str, dict] = {}

    logger.info(
        "Validating RTC coverage for %d DSWx-S1 cycle buckets "
        "(threshold=%d, workers=%d)",
        len(cycle_map),
        threshold,
        workers,
    )
    validated_count = 0
    total_buckets = len(cycle_map)
    with ThreadPoolExecutor(max_workers=min(workers, len(cycle_map))) as pool:
        items = list(cycle_map.items())
        submit_batch_size = max(100, workers * 8)
        for offset in range(0, len(items), submit_batch_size):
            futures = []
            for bucket_key, candidate_rtc_ids in items[
                offset:offset + submit_batch_size
            ]:
                tile_set_id = bucket_key.split("$", 1)[0]
                futures.append(
                    pool.submit(
                        _validate_one,
                        bucket_key,
                        candidate_rtc_ids,
                        tile_bursts[tile_set_id],
                        rtc_collection_id=rtc_collection_id,
                        venue=venue,
                        threshold=threshold,
                        temporal_window_hours=temporal_window_hours,
                        query_func=query_func,
                    )
                )

            for future in as_completed(futures):
                bucket_key, is_valid, detail = future.result()
                (valid if is_valid else dropped)[bucket_key] = detail
                validated_count += 1

            logger.info(
                "  ... validated %d / %d cycle buckets (valid=%d, dropped=%d)",
                validated_count,
                total_buckets,
                len(valid),
                len(dropped),
            )

    valid = {key: valid[key] for key in sorted(valid)}
    dropped = {key: dropped[key] for key in sorted(dropped)}
    reduced = reduce_valid_candidates(valid)
    logger.info(
        "DSWx-S1 coverage validation retained %d buckets and dropped %d; "
        "%d representative RTC recovery candidates remain",
        len(valid),
        len(dropped),
        len(reduced),
    )
    return {
        "threshold": threshold,
        "total_buckets": len(cycle_map),
        "valid_count": len(valid),
        "dropped_count": len(dropped),
        "valid": valid,
        "dropped": dropped,
        "reduced": reduced,
    }
