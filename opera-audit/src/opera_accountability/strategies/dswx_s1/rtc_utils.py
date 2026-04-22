"""RTC granule helpers ported from ``accountability_tools/dswx_s1/rtc_utils.py``.

Provides the RTC granule regex, Sentinel-1 cycle-index calculation, and a
``reduce_input_rtc_list`` helper that normalizes DSWx-S1 ``InputGranules``
values (per-band / per-polarization file names) down to bare RTC granule IDs.
"""

from __future__ import annotations

import logging
import re
from datetime import timedelta
from functools import lru_cache
from typing import Iterable

from dateutil.parser import isoparse

logger = logging.getLogger(__name__)


# EPOCH dates for the Sentinel-1 missions. The exact date does not matter - it
# just needs to be aligned with the 12-day cycle.
_EPOCH_S1A = "20140101T000000Z"
_EPOCH_S1B = "20140107T000000Z"  # Offset by 6 days from S1A.
_EPOCH_S1C = "20140107T000000Z"  # Same as S1B (S1C replaced S1B).
_EPOCH_S1D = "NOT_YET_DETERMINED"  # TODO: update when S1D begins production.

_EPOCH_MAP = {
    "S1A": _EPOCH_S1A,
    "S1B": _EPOCH_S1B,
    "S1C": _EPOCH_S1C,
    "S1D": _EPOCH_S1D,
}

_EPOCH_TODO_SENTINEL = "NOT_YET_DETERMINED"


def has_known_epoch(sensor: str) -> bool:
    """Return True iff ``sensor`` has a concrete 12-day cycle epoch defined.

    Used by the pipeline to detect mis-configuration before attempting cycle
    expansion (which would otherwise raise ``NotImplementedError`` mid-run).
    """
    return _EPOCH_MAP.get(sensor, _EPOCH_TODO_SENTINEL) != _EPOCH_TODO_SENTINEL


RTC_GRANULE_REGEX = (
    r'(?P<id>'
    r'(?P<project>OPERA)_'
    r'(?P<level>L2)_'
    r'(?P<product_type>RTC)-'
    r'(?P<source>S1)_'
    r'(?P<burst_id>\w{4}-\w{6}-\w{3})_'
    r'(?P<acquisition_ts>(?P<acq_year>\d{4})(?P<acq_month>\d{2})(?P<acq_day>\d{2})'
    r'T(?P<acq_hour>\d{2})(?P<acq_minute>\d{2})(?P<acq_second>\d{2})Z)_'
    r'(?P<creation_ts>(?P<cre_year>\d{4})(?P<cre_month>\d{2})(?P<cre_day>\d{2})'
    r'T(?P<cre_hour>\d{2})(?P<cre_minute>\d{2})(?P<cre_second>\d{2})Z)_'
    r'(?P<sensor>S1A|S1B|S1C|S1D)_'
    r'(?P<spacing>30)_'
    r'(?P<product_version>v\d+[.]\d+)'
    r')'
)
"""Full-granule RTC-S1 ID regex.

Example::

    OPERA_L2_RTC-S1_T118-252624-IW1_20250512T193408Z_20250513T011557Z_S1A_30_v1.0
"""

_RTC_GRANULE_PATTERN = re.compile(RTC_GRANULE_REGEX)


# Suffixes stripped from ``InputGranules`` file names when normalizing back to
# bare RTC granule IDs. Extensions come first (stripped once) followed by
# polarization / product tags (also stripped once each). Combined pol tags
# like ``_VV+VH`` are listed before their single-pol components so we don't
# leave a trailing ``+VH`` behind.
_INPUT_EXT_SUFFIXES = ('.h5', '.tif', '.tiff')
_INPUT_TAG_SUFFIXES = (
    '_VV+VH', '_HH+HV',
    '_VH', '_HV', '_VV', '_HH',
    '_mask',
)


def reduce_input_rtc_list(input_files: Iterable[str]) -> list[str]:
    """Normalize DSWx-S1 ``InputGranules`` entries to unique RTC granule IDs.

    DSWx-S1 CMR records list their inputs as per-band / per-polarization files
    (e.g. ``...S1A_30_v1.0_VV.tif``). This helper:

    1. Strips one known file extension (``.tif`` / ``.tiff`` / ``.h5``).
    2. Strips one known product/polarization tag (``_VV``, ``_HH+HV``,
       ``_mask``, etc.).
    3. Validates the result against the RTC granule regex; unrecognized
       entries (e.g. DEM tiles) are preserved as-is so that downstream code
       (``mapping.analyze``'s ``rtc_to_id_tuple`` try/except) can filter them.
    4. Dedupes so there is one entry per input RTC granule.

    Unrecognized suffixes are logged at DEBUG level to aid future debugging.
    """
    reduced: set[str] = set()
    for raw in input_files:
        name = raw
        for suffix in _INPUT_EXT_SUFFIXES:
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break
        for suffix in _INPUT_TAG_SUFFIXES:
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break
        if _RTC_GRANULE_PATTERN.match(name) is None and name != raw:
            logger.debug(
                "reduce_input_rtc_list: stripped form %r (from %r) does not "
                "match RTC regex; keeping raw entry for downstream filtering.",
                name, raw,
            )
            name = raw
        reduced.add(name)
    return list(reduced)


@lru_cache(maxsize=1_000_000)
def rtc_to_id_tuple(rtc_id: str) -> tuple[str, str, str]:
    """Return ``(burst_id, acquisition_ts, sensor)`` for an RTC granule ID."""
    match = _RTC_GRANULE_PATTERN.match(rtc_id)
    if match is None:
        raise ValueError(f"Failed to parse RTC granule ID: {rtc_id!r}")
    groups = match.groupdict()
    return groups['burst_id'], groups['acquisition_ts'], groups['sensor']


def determine_acquisition_cycle(burst_id: str, acquisition_dts: str, sensor: str) -> int:
    """Return the 12-day acquisition cycle index for an RTC granule.

    Parameters
    ----------
    burst_id:
        e.g. ``"T118-252624-IW1"``
    acquisition_dts:
        ISO compact timestamp, e.g. ``"20250512T193408Z"``
    sensor:
        ``"S1A" | "S1B" | "S1C" | "S1D"``
    """
    cycle_days = 12
    MAX_BURST_IDENTIFICATION_NUMBER = 375887  # from MGRS burst DB.
    ACQUISITION_CYCLE_DURATION_SECS = timedelta(days=cycle_days).total_seconds()

    epoch = _EPOCH_MAP[sensor]
    if epoch == _EPOCH_TODO_SENTINEL:
        raise NotImplementedError(f"Acquisition cycle epoch not yet defined for sensor {sensor}")

    instrument_epoch = isoparse(epoch)

    burst_identification_number = int(burst_id.split(sep='-')[1])
    seconds_after_mission_epoch = (isoparse(acquisition_dts) - instrument_epoch).total_seconds()

    acquisition_index = (
        seconds_after_mission_epoch
        - (ACQUISITION_CYCLE_DURATION_SECS
           * (burst_identification_number / MAX_BURST_IDENTIFICATION_NUMBER))
    ) / ACQUISITION_CYCLE_DURATION_SECS

    cycle = round(acquisition_index)
    if cycle < 0:
        raise ValueError(f"Acquisition cycle is negative: cycle={cycle}")
    return cycle


def determine_acquisition_cycle_for_rtc_granule(granule_id: str) -> int:
    """Convenience wrapper: parse an RTC granule ID and return its cycle index."""
    match = _RTC_GRANULE_PATTERN.match(granule_id)
    if match is None:
        raise ValueError(f"Failed to parse RTC granule ID: {granule_id!r}")
    groups = match.groupdict()
    return determine_acquisition_cycle(
        groups['burst_id'], groups['acquisition_ts'], groups['sensor']
    )
