"""Step 3 of the DSWx-S1 accountability pipeline: map missing RTCs → MGRS tile sets.

Uses the bundled MGRS tile-collection SQLite database to resolve each missing
RTC's burst ID to the MGRS tile set(s) it belongs to, dropping any tile sets
whose ``land_ocean_flag`` is ``'water'``. Port of
``accountability_tools/dswx_s1/missing_rtcs_to_tile_sets.py``.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from importlib.resources import files as _pkg_files
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

    Resolution order:

    1. Explicit ``override`` (treated as an absolute/relative filesystem path).
    2. ``products.DSWX_S1.accountability.mgrs_tile_db`` in ``config.yaml``,
       looked up inside the packaged ``opera_accountability/data/`` directory
       via :mod:`importlib.resources`.
    """
    if override:
        p = Path(override).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"--mgrs-db path does not exist: {p}")
        return p

    db_name = CONFIG['products']['DSWX_S1']['accountability']['mgrs_tile_db']
    pkg_db = _pkg_files('opera_accountability').joinpath('data', db_name)
    db_path = Path(str(pkg_db))
    if not db_path.exists():
        raise FileNotFoundError(
            f"Bundled MGRS tile DB not found at {db_path}. Expected "
            f"opera_accountability/data/{db_name} (configure via "
            f"config.yaml's products.DSWX_S1.accountability.mgrs_tile_db)."
        )
    return db_path


def _burst_id_to_db_key(rtc_id: str) -> str:
    """Turn ``OPERA_L2_RTC-S1_T118-252624-IW1_...`` → ``t118_252624_iw1``."""
    return rtc_id.split('_')[3].lower().replace('-', '_')


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
        workers = CONFIG['products']['DSWX_S1']['accountability'].get('tile_set_workers', 8)

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
                    if lof == 'water':
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
            "updating products.DSWX_S1.accountability.mgrs_tile_db.",
            unmatched_bursts, len(missing_rtcs), mgrs_db_path,
        )
    return mgrs_set_to_rtc
