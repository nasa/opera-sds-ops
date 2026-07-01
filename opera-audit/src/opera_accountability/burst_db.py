from __future__ import annotations

import json
import logging
import os
import pickle
import re
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


_RTC_BURST_RE = re.compile(r"T\d{3}-\d{6}-IW\d")


def extract_rtc_burst_id(granule_id: str) -> Optional[str]:
    """Extract burst ID from RTC granule native ID.
    
    Exact port of Kevin's extract_rtc_burst from cmr_audit_dist_s1.py:360-363
    """
    match = _RTC_BURST_RE.search(granule_id)
    return match.group() if match else None


def normalize_burst_id(burst_id: str) -> str:
    return burst_id.upper()


def _coerce_bursts_to_products(data: Any) -> dict[str, list[str]]:
    if isinstance(data, tuple) and len(data) >= 2:
        data = data[1]
    elif isinstance(data, dict) and "bursts_to_products" in data:
        data = data["bursts_to_products"]

    if not isinstance(data, dict):
        raise ValueError("Could not locate bursts_to_products mapping in burst DB data")

    coerced = {}
    for burst_id, product_groups in data.items():
        if product_groups is None:
            values = []
        elif isinstance(product_groups, (set, tuple, list)):
            values = sorted(str(v) for v in product_groups)
        else:
            values = [str(product_groups)]
        coerced[normalize_burst_id(str(burst_id))] = values
    return coerced


def load_dist_s1_bursts_to_products(db_file: Optional[str] = None) -> Optional[dict[str, list[str]]]:
    candidate = db_file or os.environ.get("OPERA_DIST_S1_BURST_DB")

    if candidate:
        path = Path(candidate).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"DIST-S1 burst DB path does not exist: {path}")

        if path.suffix.lower() == ".json":
            with open(path) as f:
                return _coerce_bursts_to_products(json.load(f))

        if path.suffix.lower() in {".pickle", ".pkl"}:
            with open(path, "rb") as f:
                return _coerce_bursts_to_products(pickle.load(f))

        try:
            from data_subscriber.dist_s1_utils import parse_local_burst_db_pickle
        except ImportError as err:
            raise ImportError(
                "Reading DIST-S1 burst DB formats other than JSON/pickle requires "
                "data_subscriber.dist_s1_utils from opera-sds-pcm."
            ) from err

        _, bursts_to_products, _, _ = parse_local_burst_db_pickle(str(path), f"{path}.pickle")
        return _coerce_bursts_to_products(bursts_to_products)

    try:
        from data_subscriber.dist_s1_utils import localize_dist_burst_db
    except ImportError:
        logger.info("DIST-S1 burst DB utilities are unavailable; running in CMR-only mode.")
        return None

    _, bursts_to_products, _, _ = localize_dist_burst_db()
    return _coerce_bursts_to_products(bursts_to_products)


def map_rtc_granules_to_product_groups(
    rtc_granules: list[str],
    bursts_to_products: dict[str, list[str]],
) -> dict[str, list[str]]:
    mapped: dict[str, list[str]] = {}
    for granule_id in rtc_granules:
        burst_id = extract_rtc_burst_id(granule_id)
        if not burst_id:
            continue
        product_groups = bursts_to_products.get(normalize_burst_id(burst_id), [])
        for product_group in product_groups:
            mapped.setdefault(product_group, []).append(granule_id)
    return {group: sorted(set(granules)) for group, granules in sorted(mapped.items())}
