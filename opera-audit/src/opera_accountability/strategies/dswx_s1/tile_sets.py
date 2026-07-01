"""Step 3 of the DSWx-S1 accountability pipeline: map missing RTCs → MGRS tile sets.

Uses an externally supplied MGRS tile-collection SQLite database to resolve
each missing RTC's burst ID to the MGRS tile set(s) it belongs to, dropping
any tile sets whose ``land_ocean_flag`` is ``'water'``. Obtain the DB from
JPL Artifactory or the ADT package repository and point the tool at it via
``--mgrs-db <path>`` or the ``OPERA_MGRS_DB`` environment variable.
Port of ``accountability_tools/dswx_s1/missing_rtcs_to_tile_sets.py``.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from ... import CONFIG

logger = logging.getLogger(__name__)


# Single query reused by all worker threads. Each burst_id lookup asks the DB
# "which mgrs_set_ids contain this burst?".
_QUERY = """
    SELECT mgrs_set_id, land_ocean_flag
    FROM mgrs_burst_db
    WHERE (
        SELECT 1
        FROM json_each(bursts)
        WHERE value = ?
    )
"""


def resolve_mgrs_tile_db(override: Optional[str] = None) -> Path:
    """Resolve the MGRS tile-collection SQLite path.

    The database is not bundled with this package. Resolution order:

    1. Explicit ``override`` (treated as an absolute/relative filesystem path).
    2. ``OPERA_MGRS_DB`` environment variable.

    Raises :class:`FileNotFoundError` with guidance if neither is set or the
    resolved path does not exist.
    """
    candidate = override or os.environ.get("OPERA_MGRS_DB")
    if not candidate:
        raise FileNotFoundError(
            "MGRS tile-collection SQLite path is required. Pass --mgrs-db "
            "<path> or set the OPERA_MGRS_DB environment variable. The DB "
            "is available from JPL Artifactory or the ADT package repo "
            "(it is no longer bundled with opera-accountability)."
        )

    p = Path(candidate).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"MGRS tile DB path does not exist: {p}")
    return p


def _burst_id_to_db_key(rtc_id: str) -> str:
    """Turn ``OPERA_L2_RTC-S1_T118-252624-IW1_...`` → ``t118_252624_iw1``."""
    return rtc_id.split("_")[3].lower().replace("-", "_")


def _worker_init(state: threading.local, db_path: str,
                 conns: list[sqlite3.Connection], conns_lock: threading.Lock) -> None:
    # ``check_same_thread=False`` lets the main thread .close() this handle
    # after the worker threads exit. It is still single-threaded in practice:
    # each connection is only accessed from the worker that created it.
    state.conn = sqlite3.connect(db_path, check_same_thread=False)
    with conns_lock:
        conns.append(state.conn)


def _lookup_one(
    rtc_id: str,
    state: threading.local,
) -> tuple[str, list[str], list[str]]:
    cursor = state.conn.cursor()
    cursor.execute(_QUERY, (_burst_id_to_db_key(rtc_id),))
    mgrs_sets: list[str] = []
    flags: list[str] = []
    for mgrs_set_id, lof in cursor.fetchall():
        mgrs_sets.append(mgrs_set_id)
        flags.append(lof)
    return rtc_id, mgrs_sets, flags


def map_missing_rtcs_to_tile_sets(
    missing_rtcs: list[str],
    mgrs_db_path: str | Path,
    workers: Optional[int] = None,
) -> dict[str, list[str]]:
    """Return ``{mgrs_set_id: [rtc_id, ...]}`` for the given missing RTC list.

    ``land_ocean_flag == 'water'`` tile sets are dropped (cannot be triggered
    as DSWx-S1 outputs). RTCs whose burst IDs are not present in the MGRS DB
    are logged and counted separately from water-set drops.
    """
    if workers is None:
        workers = CONFIG["products"]["DSWX_S1"]["accountability"].get("tile_set_workers", 8)

    mgrs_db_path = str(mgrs_db_path)
    local = threading.local()
    conns: list[sqlite3.Connection] = []
    conns_lock = threading.Lock()
    mgrs_set_to_rtc: dict[str, list[str]] = {}
    dropped_water = 0
    unmatched_bursts = 0

    logger.info(
        "Resolving %d missing RTC burst IDs to MGRS tile sets "
        "(workers=%d, db=%s)",
        len(missing_rtcs), workers, mgrs_db_path,
    )

    try:
        with ThreadPoolExecutor(
            max_workers=workers,
            initializer=_worker_init,
            initargs=(local, mgrs_db_path, conns, conns_lock),
        ) as pool:
            futures = [pool.submit(_lookup_one, rtc_id, local) for rtc_id in missing_rtcs]
            completed = 0
            for fut in as_completed(futures):
                rtc_id, mgrs_sets, flags = fut.result()
                if not mgrs_sets:
                    unmatched_bursts += 1
                    logger.debug(
                        "No MGRS tile set found for burst in %s (DB lookup empty)",
                        rtc_id,
                    )
                for mgrs_set_id, lof in zip(mgrs_sets, flags):
                    if lof == "water":
                        dropped_water += 1
                        continue
                    mgrs_set_to_rtc.setdefault(mgrs_set_id, []).append(rtc_id)
                completed += 1
                if completed % 10000 == 0:
                    logger.info("  ... processed %d / %d missing RTCs", completed, len(missing_rtcs))
    finally:
        # Close every per-worker sqlite connection; the executor has joined
        # its threads by the time we reach here so this is race-free.
        for conn in conns:
            try:
                conn.close()
            except sqlite3.Error as err:
                logger.debug("Error closing sqlite connection: %s", err)

    logger.info(
        "Mapped missing RTCs to %d MGRS tile sets "
        "(dropped %d water sets, %d RTCs had no burst match in DB)",
        len(mgrs_set_to_rtc), dropped_water, unmatched_bursts,
    )
    if unmatched_bursts:
        logger.warning(
            "%d / %d missing RTCs had no burst match in %s — consider "
            "pointing --mgrs-db / OPERA_MGRS_DB at a newer MGRS tile DB.",
            unmatched_bursts, len(missing_rtcs), mgrs_db_path,
        )
    return mgrs_set_to_rtc
