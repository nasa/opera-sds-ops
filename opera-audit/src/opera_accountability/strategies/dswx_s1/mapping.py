"""Step 2 of the DSWx-S1 accountability pipeline: RTC → DSWx input mapping.

Given surveyed RTC-S1 and DSWx-S1 granules, identify which RTCs have been
used as DSWx-S1 inputs, then compute the set of *missing* RTCs — surveyed
RTCs that could have produced a DSWx-S1 output but did not.

Port of ``accountability_tools/dswx_s1/accountability.py``. Sensor start
times are sourced from ``config.yaml`` rather than hardcoded.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

from ... import CONFIG
from ...checkpoint import CheckpointStore
from .rtc_utils import rtc_to_id_tuple

logger = logging.getLogger(__name__)

_GRANULE_TIME_FMT = "%Y%m%dT%H%M%SZ"


def _parse_iso(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp (e.g. ``"2024-08-21T00:11:56Z"``)."""
    return datetime.strptime(ts.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)


def _load_sensor_start_dates() -> dict[str, datetime]:
    raw = CONFIG["products"]["DSWX_S1"]["accountability"]["sensor_start_dates"]
    return {sensor: _parse_iso(ts) for sensor, ts in raw.items()}


def should_include_rtc(rtc_id: str, sensor_start_dates: dict[str, datetime]) -> bool:
    """Return True if the RTC's acquisition time is at/after its sensor's start date.

    Sensors not present in ``sensor_start_dates`` are skipped (returns ``False``)
    with a one-time WARNING rather than raising. This keeps the pipeline
    resilient when CMR returns granules from newly activated sensors (e.g. S1D)
    before ``config.yaml`` is updated.
    """
    _, acquisition_ts, sensor = rtc_to_id_tuple(rtc_id)
    if sensor not in sensor_start_dates:
        _warn_unknown_sensor_once(sensor, rtc_id)
        return False
    acq_dt = datetime.strptime(acquisition_ts, _GRANULE_TIME_FMT)
    return acq_dt >= sensor_start_dates[sensor]


_warned_sensors: set[str] = set()


def _warn_unknown_sensor_once(sensor: str, rtc_id: str) -> None:
    """Emit a single WARNING per unknown sensor, regardless of RTC count."""
    if sensor in _warned_sensors:
        return
    _warned_sensors.add(sensor)
    logger.warning(
        "No DSWx-S1 processing start time configured for sensor %s (first seen: %s). "
        "Excluding all %s RTCs from accountability. Add it under "
        "products.DSWX_S1.accountability.sensor_start_dates in config.yaml.",
        sensor, rtc_id, sensor,
    )


def analyze(
    rtc_products: list[dict],
    dswx_products: list[dict],
    sensor_start_dates: dict[str, datetime] | None = None,
) -> dict[str, Any]:
    """Map RTCs to DSWx-S1 outputs and return missing-RTC accountability results.

    Parameters
    ----------
    rtc_products:
        List of ``{"id": <granule_id>, ...}`` from :func:`survey.survey_rtc`.
    dswx_products:
        List of ``{"id": <granule_id>, "input_rtcs": [<rtc_id>, ...]}`` from
        :func:`survey.survey_dswx`.
    sensor_start_dates:
        Sensor → earliest-sensing datetime. Defaults to the values in
        ``config.yaml``.

    Returns
    -------
    dict:
        Keys: ``expected``, ``actual``, ``missing_count``, ``used_rtc_count``,
        ``filtered_rtc_count``, ``missing`` (sorted list of missing RTC IDs),
        ``rtc_to_dswx_map`` (serializable mapping).
    """
    if sensor_start_dates is None:
        sensor_start_dates = _load_sensor_start_dates()

    # Reset the one-shot warning cache so every pipeline invocation within a
    # long-lived process (e.g. the Streamlit dashboard) re-emits the
    # "unknown sensor" warning instead of silently swallowing it after the
    # first run.
    _warned_sensors.clear()

    logger.info("Loaded RTC survey with %d products", len(rtc_products))
    rtc_filtered = [rtc for rtc in rtc_products if should_include_rtc(rtc["id"], sensor_start_dates)]
    logger.info(
        "Filtered RTC products from %d to %d using sensor start dates",
        len(rtc_products), len(rtc_filtered),
    )

    logger.info("Loaded DSWx-S1 survey with %d products", len(dswx_products))
    logger.info("Mapping DSWx-S1 RTC inputs to products")

    # (burst_id, acq_ts, sensor) -> [dswx_granule_id, ...]
    rtc_to_dswx_map: dict[tuple[str, str, str], list[str]] = {}
    total_dswx = len(dswx_products)
    dswx_map_progress = max(50_000, total_dswx // 10)
    for idx, dswx in enumerate(dswx_products):
        if idx > 0 and idx % dswx_map_progress == 0:
            logger.info(
                "  ... DSWx-S1 input mapping: %d / %d products (%d unique RTCs so far)",
                idx, total_dswx, len(rtc_to_dswx_map),
            )
        dswx_id = dswx["id"]
        for rtc_in in dswx["input_rtcs"]:
            try:
                key = rtc_to_id_tuple(rtc_in)
            except ValueError:
                # Non-RTC entry in InputGranules (e.g. DEM tiles); ignore.
                logger.debug("Skipping non-RTC input granule: %s", rtc_in)
                continue
            rtc_to_dswx_map.setdefault(key, []).append(dswx_id)
    logger.info("Mapped %d unique RTCs as DSWx-S1 inputs", len(rtc_to_dswx_map))

    # Build latest-ID lookup from surveyed (filtered) RTCs.
    rtc_id_to_latest: dict[tuple[str, str, str], str] = {}
    total_filtered = len(rtc_filtered)
    latest_progress = max(100_000, total_filtered // 10)
    for idx, rec in enumerate(rtc_filtered):
        rtc_id_to_latest[rtc_to_id_tuple(rec["id"])] = rec["id"]
        if idx > 0 and idx % latest_progress == 0:
            logger.info("  ... built latest-ID lookup: %d / %d RTCs", idx, total_filtered)
    logger.info("Built latest-ID lookup for %d filtered RTCs", total_filtered)

    used_rtc_ids = set(rtc_to_dswx_map.keys())
    avail_rtc_ids = set(rtc_id_to_latest.keys())

    logger.info("RTC count used in DSWx: %d", len(used_rtc_ids))
    logger.info("RTC count from filtered survey: %d", len(avail_rtc_ids))
    if len(avail_rtc_ids) > 0:
        logger.info(
            "Used %% of available: %.4f%%",
            (len(used_rtc_ids) / len(avail_rtc_ids)) * 100,
        )

    missing_keys = avail_rtc_ids - used_rtc_ids
    missing_rtc_products = sorted(
        rtc_id_to_latest[k] for k in missing_keys
    )
    logger.info("Unused (missing) RTC count: %d", len(missing_rtc_products))

    # Serializable form of the mapping (str keys).
    logger.info("Serializing RTC → DSWx-S1 map (%d entries)", len(rtc_to_dswx_map))
    rtc_to_dswx_map_serializable = {}
    total_map_entries = len(rtc_to_dswx_map)
    serialize_progress = max(100_000, total_map_entries // 10)
    for idx, (key, dswx_ids) in enumerate(rtc_to_dswx_map.items()):
        rtc_to_dswx_map_serializable["$".join(key)] = sorted(set(dswx_ids))
        if idx > 0 and idx % serialize_progress == 0:
            logger.info("  ... serialized %d / %d map entries", idx, total_map_entries)
    logger.info("Serialization complete (%d entries)", total_map_entries)

    return {
        "expected": len(avail_rtc_ids),
        "actual": len(used_rtc_ids & avail_rtc_ids),
        "missing_count": len(missing_rtc_products),
        "used_rtc_count": len(used_rtc_ids),
        "filtered_rtc_count": len(avail_rtc_ids),
        "missing": missing_rtc_products,
        "rtc_to_dswx_map": rtc_to_dswx_map_serializable,
    }


def _batches(values, size: int = 10000):
    batch = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def analyze_checkpoint(
    checkpoint: CheckpointStore,
    sensor_start_dates: dict[str, datetime] | None = None,
) -> dict[str, Any]:
    """Run the DSWx-S1 global reducer directly against checkpointed records.

    The full RTC and DSWx surveys never coexist as Python lists. Latest-revision
    deduplication, expected/used membership, and set difference remain in
    SQLite; only the final missing RTC list is materialized for recovery work.
    """
    if sensor_start_dates is None:
        sensor_start_dates = _load_sensor_start_dates()
    _warned_sensors.clear()

    rtc_pattern = re.compile(CONFIG["products"]["RTC_S1"]["pattern"])
    rtc_unique_fields = tuple(CONFIG["products"]["RTC_S1"]["unique_fields"])
    dswx_pattern = re.compile(CONFIG["products"]["DSWX_S1"]["pattern"])
    dswx_unique_fields = tuple(CONFIG["products"]["DSWX_S1"]["unique_fields"])

    logger.info("Checkpoint reducer: clearing derived namespaces")
    for namespace in (
        "rtc_unique",
        "dswx_unique",
        "available_rtcs",
        "used_rtcs",
        "rtc_to_dswx_pairs",
    ):
        checkpoint.clear_namespace(namespace)

    logger.info("Checkpoint reducer: deduplicating RTC-S1 survey records")
    rtc_failures = 0
    rtc_batch_count = 0
    for batch in _batches(checkpoint.iter_payloads("rtc_survey")):
        reduced = []
        for product in batch:
            match = rtc_pattern.match(product["id"])
            if match is None:
                rtc_failures += 1
                continue
            groups = match.groupdict()
            stable_key = "$".join(groups[field] for field in rtc_unique_fields)
            reduced.append(
                (stable_key, groups["creation_ts"], product)
            )
        checkpoint.upsert_reduced_records("rtc_unique", reduced)
        rtc_batch_count += 1
        if rtc_batch_count % 10 == 0:
            logger.info("  ... processed %d RTC-S1 batches", rtc_batch_count)
    logger.info("Checkpoint reducer: RTC-S1 dedup done (%d batches)", rtc_batch_count)

    logger.info("Checkpoint reducer: deduplicating DSWx-S1 survey records")
    dswx_failures = 0
    dswx_batch_count = 0
    for batch in _batches(checkpoint.iter_payloads("dswx_survey")):
        reduced = []
        for product in batch:
            match = dswx_pattern.match(product["id"])
            if match is None:
                dswx_failures += 1
                continue
            groups = match.groupdict()
            stable_key = "$".join(groups[field] for field in dswx_unique_fields)
            reduced.append(
                (stable_key, groups["creation_ts"], product)
            )
        checkpoint.upsert_reduced_records("dswx_unique", reduced)
        dswx_batch_count += 1
        if dswx_batch_count % 10 == 0:
            logger.info("  ... processed %d DSWx-S1 batches", dswx_batch_count)
    logger.info("Checkpoint reducer: DSWx-S1 dedup done (%d batches)", dswx_batch_count)

    if rtc_failures:
        logger.error("Skipped %d non-conformant RTC-S1 records", rtc_failures)
    if dswx_failures:
        logger.error("Skipped %d non-conformant DSWx-S1 records", dswx_failures)

    logger.info("Checkpoint reducer: filtering RTCs by sensor start dates")
    filter_batch_count = 0
    for batch in _batches(checkpoint.iter_reduced_payloads("rtc_unique")):
        available = []
        for product in batch:
            if should_include_rtc(product["id"], sensor_start_dates):
                key = "$".join(rtc_to_id_tuple(product["id"]))
                available.append((key, product["id"], product))
        checkpoint.upsert_reduced_records("available_rtcs", available)
        filter_batch_count += 1
        if filter_batch_count % 10 == 0:
            logger.info("  ... filtered %d RTC-S1 batches by sensor start dates", filter_batch_count)
    logger.info("Checkpoint reducer: sensor-date filtering done (%d batches)", filter_batch_count)

    logger.info("Checkpoint reducer: building RTC → DSWx-S1 usage map")
    usage_batch_count = 0
    for batch in _batches(checkpoint.iter_reduced_payloads("dswx_unique")):
        used = []
        pairs = []
        for product in batch:
            dswx_id = product["id"]
            for rtc_id in product.get("input_rtcs", []):
                try:
                    stable_key = "$".join(rtc_to_id_tuple(rtc_id))
                except ValueError:
                    continue
                used.append((stable_key, rtc_id, {"id": rtc_id}))
                pair_key = f"{stable_key}\u0000{dswx_id}"
                pairs.append(
                    (
                        pair_key,
                        {"rtc_key": stable_key, "dswx_id": dswx_id},
                    )
                )
        checkpoint.upsert_reduced_records("used_rtcs", used)
        checkpoint.upsert_records("rtc_to_dswx_pairs", pairs)
        usage_batch_count += 1
        if usage_batch_count % 10 == 0:
            logger.info("  ... processed %d DSWx-S1 usage-map batches", usage_batch_count)
    logger.info("Checkpoint reducer: usage map done (%d batches)", usage_batch_count)

    logger.info("Checkpoint reducer: computing final set differences")
    expected = checkpoint.count_reduced_records("available_rtcs")
    used_count = checkpoint.count_reduced_records("used_rtcs")
    actual = checkpoint.count_reduced_intersection("available_rtcs", "used_rtcs")
    missing = sorted(
        payload["id"]
        for payload in checkpoint.iter_reduced_difference_payloads(
            "available_rtcs", "used_rtcs"
        )
    )

    return {
        "expected": expected,
        "actual": actual,
        "missing_count": len(missing),
        "used_rtc_count": used_count,
        "filtered_rtc_count": expected,
        "missing": missing,
        "rtc_surveyed": checkpoint.count_reduced_records("rtc_unique"),
        "dswx_surveyed": checkpoint.count_reduced_records("dswx_unique"),
    }
