from __future__ import annotations

import re
from datetime import datetime, timedelta


DIST_S1_NATIVE_ID_RE = re.compile(
    r'OPERA_L3_DIST(?:-ALERT)?-S1_'
    r'(?P<tile_id>T?\w+)_'
    r'(?P<acq_time>\d{8}T\d{6}Z)_'
    r'(?P<prod_time>\d{8}T\d{6}Z)_'
    r'S1[A-D]?_30_v\d+(?:[.]\d+)?'
)
RTC_ID_RE = re.compile(
    r'OPERA_L2_RTC-S1_'
    r'(?P<burst_id>\w{4}-\w{6}-\w{3})_'
    r'(?P<acquisition_ts>\d{8}T\d{6}Z)_'
    r'(?P<creation_ts>\d{8}T\d{6}Z)_'
    r'(?P<sensor>S1[A-D])_30_v\d+[.]\d+'
)


def parse_dist_s1_native_id(native_id: str) -> tuple[str | None, str | None]:
    match = DIST_S1_NATIVE_ID_RE.match(native_id)
    if not match:
        return None, None
    return match.group("tile_id"), match.group("acq_time")


def normalize_tile_time_key(tile_id: str, timestamp: str) -> str:
    if tile_id.startswith("T"):
        tile_id = tile_id[1:]
    return f"{tile_id},{timestamp}"


def parse_rtc_id(granule_id: str) -> dict[str, str] | None:
    match = RTC_ID_RE.match(granule_id)
    return match.groupdict() if match else None


def rtc_acquisition_timestamp(granule_id: str) -> str | None:
    parsed = parse_rtc_id(granule_id)
    return parsed["acquisition_ts"] if parsed else None


def reduce_product_id_times(product_id_times: list[str], tolerance_minutes: int = 10) -> list[str]:
    rows = []
    for value in product_id_times:
        try:
            group, ts_str = value.split(",", 1)
            ts = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ")
        except ValueError:
            continue
        rows.append((group, ts, ts_str))

    rows.sort(key=lambda item: (item[0], item[1]))
    reduced = []
    current_group = None
    current_ts = None
    tolerance = timedelta(minutes=tolerance_minutes)

    for group, ts, ts_str in rows:
        if current_group != group or current_ts is None or (ts - current_ts) >= tolerance:
            reduced.append(f"{group},{ts_str}")
            current_group = group
            current_ts = ts

    return reduced
