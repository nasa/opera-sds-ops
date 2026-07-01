from __future__ import annotations

from typing import Any, Optional

from ...burst_db import map_rtc_granules_to_product_groups
from .utils import normalize_tile_time_key, reduce_product_id_times, rtc_acquisition_timestamp


def _product_id_times(product_group: str, rtc_granules: list[str]) -> list[str]:
    values = []
    for granule_id in rtc_granules:
        timestamp = rtc_acquisition_timestamp(granule_id)
        if timestamp:
            values.append(normalize_tile_time_key(product_group, timestamp))
    return reduce_product_id_times(values)


def analyze(
    rtc_products: list[dict],
    dist_products: list[dict],
    existing_tile_times: set[str],
    bursts_to_products: Optional[dict[str, list[str]]] = None,
) -> dict[str, Any]:
    rtc_ids = sorted({product["id"] for product in rtc_products})
    used_rtc_to_dist: dict[str, list[str]] = {}

    for dist_product in dist_products:
        dist_id = dist_product["id"]
        for rtc_id in dist_product.get("input_rtcs", []):
            used_rtc_to_dist.setdefault(rtc_id, []).append(dist_id)

    used_rtc_ids = set(used_rtc_to_dist)
    available_rtc_ids = set(rtc_ids)
    missing_rtcs = sorted(available_rtc_ids - used_rtc_ids)

    missing_by_product_group: dict[str, list[str]] = {}
    missing_dist_rows = []
    missing_product_id_times = []
    filtered_existing_count = 0

    if bursts_to_products:
        missing_by_product_group = map_rtc_granules_to_product_groups(
            missing_rtcs,
            bursts_to_products,
        )
        for product_group, granules in missing_by_product_group.items():
            product_id_times = _product_id_times(product_group, granules)
            retained = [value for value in product_id_times if value not in existing_tile_times]
            filtered_existing_count += len(product_id_times) - len(retained)
            if not retained:
                continue
            missing_product_id_times.extend(retained)
            missing_dist_rows.append({
                "mgrs_tile_id_acq_group": product_group,
                "rtc_granules": granules,
                "product_id_time": retained,
            })

    actual = len(available_rtc_ids & used_rtc_ids)
    return {
        "expected": len(available_rtc_ids),
        "actual": actual,
        "missing_count": len(missing_rtcs),
        "missing": missing_rtcs,
        "used_rtc_count": len(used_rtc_ids),
        "rtc_surveyed": len(rtc_products),
        "dist_surveyed": len(dist_products),
        "existing_tile_time_count": len(existing_tile_times),
        "rtc_to_dist_map": {
            rtc_id: sorted(set(dist_ids))
            for rtc_id, dist_ids in sorted(used_rtc_to_dist.items())
        },
        "burst_db_enabled": bool(bursts_to_products),
        "missing_product_group_count": len(missing_by_product_group),
        "filtered_existing_product_time_count": filtered_existing_count,
        "missing_dist_product_count": len(missing_product_id_times),
        "missing_dist_products": sorted(set(missing_product_id_times)),
        "missing_dist_product_rows": missing_dist_rows,
        "missing_rtcs_to_product_groups": missing_by_product_group,
    }
