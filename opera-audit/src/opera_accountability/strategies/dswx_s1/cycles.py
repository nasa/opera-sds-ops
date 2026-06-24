"""Step 4 of the DSWx-S1 accountability pipeline: cycle + sensor expansion.

Takes the ``{mgrs_set_id: [rtc_id, ...]}`` mapping from Step 3 and expands
keys to ``<mgrs_set_id>$<acquisition_cycle>$<sensor>``, sorting by tile set
numeric components, then cycle, then sensor. Port of
``accountability_tools/dswx_s1/add_cycle_indices.py``.
"""

from __future__ import annotations

import logging
import re
from typing import Iterable

from .rtc_utils import RTC_GRANULE_REGEX, determine_acquisition_cycle_for_rtc_granule

logger = logging.getLogger(__name__)

_RTC_GRANULE_PATTERN = re.compile(RTC_GRANULE_REGEX)


def _tile_set_sort_key(key: str) -> tuple:
    """Sort key matching Riley's add_cycle_indices.py ordering.

    Expects ``<mgrs_set_id>$<cycle>$<sensor>`` where ``mgrs_set_id`` has the
    shape ``MS_<n1>_<n2>``.
    """
    tile_set, cycle, sensor = key.split('$')
    tile_set_parts = tile_set.split('_')
    # mgrs_set_id like "MS_1_3" → (1, 3). Fallback to (0, 0) if unparseable.
    try:
        n1, n2 = int(tile_set_parts[1]), int(tile_set_parts[2])
    except (IndexError, ValueError):
        n1, n2 = 0, 0
    return (n1, n2, int(cycle), sensor)


def expand_with_cycle_indices(
    mgrs_set_to_rtc: dict[str, Iterable[str]],
) -> dict[str, list[str]]:
    """Expand each tile-set bucket to ``<tile_set>$<cycle>$<sensor>`` keys.

    Each RTC is re-keyed under the 12-day acquisition cycle it falls in (from
    :func:`rtc_utils.determine_acquisition_cycle_for_rtc_granule`) and its
    sensor code. The returned mapping is sorted for stable diffs.
    """
    expanded: dict[str, list[str]] = {}

    for tile_set, rtc_ids in mgrs_set_to_rtc.items():
        for rtc in rtc_ids:
            match = _RTC_GRANULE_PATTERN.match(rtc)
            if match is None:
                raise ValueError(f"Failed to parse RTC granule ID: {rtc!r}")
            sensor = match.groupdict()['sensor']
            cycle = determine_acquisition_cycle_for_rtc_granule(rtc)

            key = f'{tile_set}${cycle}${sensor}'
            expanded.setdefault(key, []).append(rtc)

    # Sort inner lists and outer keys deterministically.
    sorted_map = {
        k: sorted(expanded[k])
        for k in sorted(expanded.keys(), key=_tile_set_sort_key)
    }
    logger.info(
        "Expanded %d MGRS tile sets to %d tile-set/cycle/sensor buckets",
        len(mgrs_set_to_rtc), len(sorted_map),
    )
    return sorted_map
