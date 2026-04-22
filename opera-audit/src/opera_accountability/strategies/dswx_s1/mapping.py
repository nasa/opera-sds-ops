"""Step 2 of the DSWx-S1 accountability pipeline: RTC → DSWx input mapping.

Given surveyed RTC-S1 and DSWx-S1 granules, identify which RTCs have been
used as DSWx-S1 inputs, then compute the set of *missing* RTCs — surveyed
RTCs that could have produced a DSWx-S1 output but did not.

Port of ``accountability_tools/dswx_s1/accountability.py``. Sensor start
times are sourced from ``config.yaml`` rather than hardcoded.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from ... import CONFIG
from .rtc_utils import rtc_to_id_tuple

logger = logging.getLogger(__name__)

_GRANULE_TIME_FMT = '%Y%m%dT%H%M%SZ'


def _parse_iso(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp (e.g. ``"2024-08-21T00:11:56Z"``)."""
    return datetime.strptime(ts.replace('Z', '+0000'), '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=None)


def _load_sensor_start_dates() -> dict[str, datetime]:
    raw = CONFIG['products']['DSWX_S1']['accountability']['sensor_start_dates']
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
    rtc_filtered = [rtc for rtc in rtc_products if should_include_rtc(rtc['id'], sensor_start_dates)]
    logger.info(
        "Filtered RTC products from %d to %d using sensor start dates",
        len(rtc_products), len(rtc_filtered),
    )

    logger.info("Loaded DSWx-S1 survey with %d products", len(dswx_products))
    logger.info("Mapping DSWx-S1 RTC inputs to products")

    # (burst_id, acq_ts, sensor) -> [dswx_granule_id, ...]
    rtc_to_dswx_map: dict[tuple[str, str, str], list[str]] = {}
    for dswx in dswx_products:
        dswx_id = dswx['id']
        for rtc_in in dswx['input_rtcs']:
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
    for rec in rtc_filtered:
        rtc_id_to_latest[rtc_to_id_tuple(rec['id'])] = rec['id']

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
    rtc_to_dswx_map_serializable = {
        '$'.join(key): sorted(set(dswx_ids))
        for key, dswx_ids in rtc_to_dswx_map.items()
    }

    return {
        'expected': len(avail_rtc_ids),
        'actual': len(used_rtc_ids & avail_rtc_ids),
        'missing_count': len(missing_rtc_products),
        'used_rtc_count': len(used_rtc_ids),
        'filtered_rtc_count': len(avail_rtc_ids),
        'missing': missing_rtc_products,
        'rtc_to_dswx_map': rtc_to_dswx_map_serializable,
    }
