#!/usr/bin/env python3
"""Combine daily DSWx-S1 accountability results into a single report.

All intermediate data is stored in a temporary SQLite database on disk
so that peak RAM stays at ~100-200 MB regardless of dataset size.
Each phase reads one JSON file at a time, writes parsed rows to SQLite,
then frees all Python objects before the next phase begins.

Usage
-----
    .venv/bin/python scripts/combine_dswx_s1.py \\
        scripts/output/opera_dswx_s1_daily_20260709_203245

    .venv/bin/python scripts/combine_dswx_s1.py \\
        scripts/output/opera_dswx_s1_daily_20260709_203245 \\
        --start 2024-08-28 --end 2026-07-01 \\
        --venue PROD \\
        --mgrs-db /path/to/MGRS_tile_collection_v0.3.sqlite
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import os
import re
import sqlite3
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

# Ensure the repo's src/ is importable when run from scripts/
_SCRIPT_DIR = Path(__file__).resolve().parent
_SRC_DIR = _SCRIPT_DIR.parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from opera_accountability import CONFIG
from opera_accountability.strategies.dswx_s1 import tile_sets
from opera_accountability.strategies.dswx_s1.rtc_utils import (
    RTC_GRANULE_REGEX,
    determine_acquisition_cycle_for_rtc_granule,
)

_RTC_RE = re.compile(RTC_GRANULE_REGEX)
_BATCH = 10_000

# Configure logging so tile_sets.py progress messages are visible
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)


def _log(msg: str) -> None:
    """Print with flush so output is immediately visible."""
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_rtc_id(rtc_id: str) -> tuple[str, str, str] | None:
    m = _RTC_RE.match(rtc_id)
    if m is None:
        return None
    g = m.groupdict()
    return g["burst_id"], g["acquisition_ts"], g["sensor"]


def _load_sensor_start_dates() -> dict[str, datetime]:
    raw = CONFIG["products"]["DSWX_S1"]["accountability"]["sensor_start_dates"]
    result = {}
    for sensor, ts in raw.items():
        dt = datetime.strptime(
            ts.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z"
        ).replace(tzinfo=None)
        result[sensor] = dt
    return result


def _build_pattern(product_key: str):
    cfg = CONFIG["products"][product_key]
    return re.compile(cfg["pattern"]), tuple(cfg["unique_fields"])


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def _stream_write_json_list(path: Path, cursor, *, transform=None) -> int:
    """Stream-write a JSON array from a DB cursor without loading all into RAM."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as f:
        f.write("[\n")
        for row in cursor:
            item = transform(row) if transform else row
            if count > 0:
                f.write(",\n")
            f.write("  " + json.dumps(item, sort_keys=True))
            count += 1
        f.write("\n]\n")
    return count


def _stream_write_json_map(path: Path, cursor) -> int:
    """Stream-write a JSON object {key: [val,...]} from grouped cursor rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as f:
        f.write("{\n")
        for key, vals_json in cursor:
            vals = json.loads(vals_json)
            if count > 0:
                f.write(",\n")
            f.write(f"  {json.dumps(key)}: {json.dumps(sorted(vals))}")
            count += 1
        f.write("\n}\n")
    return count


def write_summary_txt(path: Path, r: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    expected = r["expected"]
    actual = r["actual"]
    pct = (actual / expected * 100) if expected else 0.0
    with path.open("w", encoding="utf-8") as f:
        f.write("OPERA DSWx-S1 Accountability Report (combined)\n")
        f.write("=" * 55 + "\n")
        f.write(f"{'Venue':<26}{r['metadata']['venue']}\n")
        f.write(f"{'Start date':<26}{r['metadata']['start_date']}\n")
        f.write(f"{'End date':<26}{r['metadata']['end_date']}\n")
        f.write(f"{'Generated':<26}{r['metadata']['generated_at']}\n")
        f.write(f"{'Combined from chunks':<26}{r['metadata']['combined_day_count']}\n")
        f.write("\n")
        f.write("SURVEY\n" + "-" * 55 + "\n")
        f.write(f"RTC-S1  after global dedupe : {r['rtc_surveyed']:>10,}\n")
        f.write(f"DSWx-S1 after global dedupe : {r['dswx_surveyed']:>10,}\n")
        f.write(f"Raw daily RTC-S1 records    : {r['metadata']['raw_rtc_records']:>10,}\n")
        f.write(f"Raw daily DSWx-S1 records   : {r['metadata']['raw_dswx_records']:>10,}\n")
        f.write("\n")
        f.write("MAPPING\n" + "-" * 55 + "\n")
        f.write(f"RTCs after sensor-date filter : {r['filtered_rtc_count']:>10,}\n")
        f.write(f"RTCs used in DSWx-S1         : {r['used_rtc_count']:>10,}\n")
        f.write(f"Missing RTCs                 : {r['missing_count']:>10,}\n")
        f.write(f"Accountability rate          : {pct:>9.2f}%\n")
        f.write("\n")
        f.write("TILE SETS\n" + "-" * 55 + "\n")
        f.write(f"MGRS tile sets affected      : {r['tile_set_count']:>10,}\n")
        f.write(f"Tile-set/cycle/sensor buckets : {r['cycle_bucket_count']:>10,}\n")


def _tile_set_sort_key(key: str) -> tuple:
    tile_set, cycle, sensor = key.split("$")
    parts = tile_set.split("_")
    try:
        n1, n2 = int(parts[1]), int(parts[2])
    except (IndexError, ValueError):
        n1, n2 = 0, 0
    return (n1, n2, int(cycle), sensor)


# ---------------------------------------------------------------------------
# SQLite schema
# ---------------------------------------------------------------------------

def _init_db(db: sqlite3.Connection) -> None:
    db.executescript("""
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous  = NORMAL;
        PRAGMA cache_size   = -64000;   -- 64 MB page cache
        PRAGMA temp_store   = MEMORY;

        CREATE TABLE IF NOT EXISTS rtc_deduped (
            burst_id     TEXT NOT NULL,
            acq_ts       TEXT NOT NULL,
            sensor       TEXT NOT NULL,
            creation_ts  TEXT NOT NULL,
            id           TEXT NOT NULL,
            PRIMARY KEY (burst_id, acq_ts, sensor)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS dswx_deduped (
            tile_id      TEXT NOT NULL,
            acq_ts       TEXT NOT NULL,
            sensor       TEXT NOT NULL,
            creation_ts  TEXT NOT NULL,
            id           TEXT NOT NULL,
            input_rtcs   TEXT NOT NULL,   -- JSON array
            PRIMARY KEY (tile_id, acq_ts, sensor)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS rtc_filtered (
            burst_id  TEXT NOT NULL,
            acq_ts    TEXT NOT NULL,
            sensor    TEXT NOT NULL,
            id        TEXT NOT NULL,
            PRIMARY KEY (burst_id, acq_ts, sensor)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS rtc_dswx_map (
            burst_id  TEXT NOT NULL,
            acq_ts    TEXT NOT NULL,
            sensor    TEXT NOT NULL,
            dswx_id   TEXT NOT NULL
        );
        -- NOTE: index on rtc_dswx_map is created AFTER bulk inserts (phase3b)
        -- to avoid per-row index maintenance overhead on 150M+ rows.

        CREATE TABLE IF NOT EXISTS missing_rtcs (
            id TEXT NOT NULL PRIMARY KEY
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS counters (
            name  TEXT PRIMARY KEY,
            value INTEGER NOT NULL DEFAULT 0
        ) WITHOUT ROWID;
    """)


# ---------------------------------------------------------------------------
# Phase 1: dedupe RTC-S1 into SQLite
# ---------------------------------------------------------------------------

def phase1_rtc_dedupe(db: sqlite3.Connection, files: list[Path]) -> tuple[int, int]:
    pattern, unique_fields = _build_pattern("RTC_S1")
    raw_count = 0
    parse_failures = 0

    for file_idx, path in enumerate(files, 1):
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            continue

        batch: list[tuple] = []
        for item in data:
            raw_count += 1
            gid = item.get("id")
            if not gid:
                parse_failures += 1
                continue
            m = pattern.match(gid)
            if m is None:
                parse_failures += 1
                continue
            gd = m.groupdict()
            key_vals = tuple(gd[f] for f in unique_fields)
            creation_ts = gd.get("creation_ts", "")
            # (burst_id, acq_ts, sensor, creation_ts, id)
            batch.append((*key_vals, creation_ts, gid))

        if batch:
            db.executemany(
                "INSERT INTO rtc_deduped (burst_id, acq_ts, sensor, creation_ts, id) "
                "VALUES (?,?,?,?,?) "
                "ON CONFLICT(burst_id, acq_ts, sensor) DO UPDATE SET "
                "  id = excluded.id, creation_ts = excluded.creation_ts "
                "WHERE excluded.creation_ts > rtc_deduped.creation_ts",
                batch,
            )
            db.commit()

        if file_idx % 25 == 0 or file_idx == len(files):
            count = db.execute("SELECT COUNT(*) FROM rtc_deduped").fetchone()[0]
            _log(f"  [RTC_S1] {file_idx}/{len(files)} files, "
                 f"{raw_count:,} raw, {count:,} unique")

    return raw_count, parse_failures


# ---------------------------------------------------------------------------
# Phase 2: dedupe DSWx-S1 into SQLite
# ---------------------------------------------------------------------------

def phase2_dswx_dedupe(db: sqlite3.Connection, files: list[Path]) -> tuple[int, int]:
    pattern, unique_fields = _build_pattern("DSWX_S1")
    raw_count = 0
    parse_failures = 0

    for file_idx, path in enumerate(files, 1):
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            continue

        batch: list[tuple] = []
        for item in data:
            raw_count += 1
            gid = item.get("id")
            if not gid:
                parse_failures += 1
                continue
            m = pattern.match(gid)
            if m is None:
                parse_failures += 1
                continue
            gd = m.groupdict()
            key_vals = tuple(gd[f] for f in unique_fields)
            creation_ts = gd.get("creation_ts", "")
            input_rtcs_json = json.dumps(item.get("input_rtcs", []))
            batch.append((*key_vals, creation_ts, gid, input_rtcs_json))

        if batch:
            db.executemany(
                "INSERT INTO dswx_deduped (tile_id, acq_ts, sensor, creation_ts, id, input_rtcs) "
                "VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(tile_id, acq_ts, sensor) DO UPDATE SET "
                "  id = excluded.id, creation_ts = excluded.creation_ts, "
                "  input_rtcs = excluded.input_rtcs "
                "WHERE excluded.creation_ts > dswx_deduped.creation_ts",
                batch,
            )
            db.commit()

        if file_idx % 25 == 0 or file_idx == len(files):
            count = db.execute("SELECT COUNT(*) FROM dswx_deduped").fetchone()[0]
            _log(f"  [DSWX_S1] {file_idx}/{len(files)} files, "
                 f"{raw_count:,} raw, {count:,} unique")

    return raw_count, parse_failures


# ---------------------------------------------------------------------------
# Phase 3a: filter RTCs by sensor start date
# ---------------------------------------------------------------------------

def phase3a_filter_rtcs(db: sqlite3.Connection) -> int:
    sensor_start_dates = _load_sensor_start_dates()
    _FMT = "%Y%m%dT%H%M%SZ"

    total = db.execute("SELECT COUNT(*) FROM rtc_deduped").fetchone()[0]
    _log(f"Phase 3a/4: Filtering {total:,} RTCs by sensor start dates...")

    cursor = db.execute("SELECT burst_id, acq_ts, sensor, id FROM rtc_deduped")
    batch: list[tuple] = []
    processed = 0
    skipped = 0
    inserted = 0
    warned: set[str] = set()

    for burst_id, acq_ts, sensor, rtc_id in cursor:
        processed += 1
        if sensor not in sensor_start_dates:
            if sensor not in warned:
                warned.add(sensor)
                _log(f"  WARNING: unknown sensor {sensor} (first: {rtc_id}), excluding")
            skipped += 1
            continue
        acq_dt = datetime.strptime(acq_ts, _FMT)
        if acq_dt >= sensor_start_dates[sensor]:
            batch.append((burst_id, acq_ts, sensor, rtc_id))
            inserted += 1
        if len(batch) >= _BATCH:
            db.executemany(
                "INSERT INTO rtc_filtered VALUES (?,?,?,?)", batch
            )
            db.commit()
            batch.clear()
        if processed % 100_000 == 0:
            _log(f"  ... {processed:,}/{total:,} ({inserted:,} passed)")

    if batch:
        db.executemany("INSERT INTO rtc_filtered VALUES (?,?,?,?)", batch)
        db.commit()

    _log(f"  Filtered: {total:,} -> {inserted:,} (skipped {skipped:,} unknown-sensor)")
    return inserted


# ---------------------------------------------------------------------------
# Phase 3b: build RTC->DSWx map from DSWx input_rtcs
# ---------------------------------------------------------------------------

def phase3b_build_map(db: sqlite3.Connection) -> int:
    total = db.execute("SELECT COUNT(*) FROM dswx_deduped").fetchone()[0]
    _log(f"Phase 3b/4: Building RTC->DSWx map from {total:,} DSWx products...")

    cursor = db.execute("SELECT id, input_rtcs FROM dswx_deduped")
    batch: list[tuple] = []
    processed = 0
    input_count = 0
    t_start = time.monotonic()

    for dswx_id, input_rtcs_json in cursor:
        processed += 1
        input_rtcs = json.loads(input_rtcs_json)
        for rtc_in in input_rtcs:
            input_count += 1
            parsed = _parse_rtc_id(rtc_in)
            if parsed is None:
                continue
            batch.append((*parsed, dswx_id))
            if len(batch) >= 100_000:
                db.executemany(
                    "INSERT INTO rtc_dswx_map VALUES (?,?,?,?)", batch
                )
                db.commit()
                batch.clear()
        if processed % 100_000 == 0:
            elapsed = time.monotonic() - t_start
            _log(f"  ... {processed:,}/{total:,} DSWx products "
                 f"({input_count:,} input RTCs, {elapsed:.0f}s)")

    if batch:
        db.executemany("INSERT INTO rtc_dswx_map VALUES (?,?,?,?)", batch)
        db.commit()

    elapsed = time.monotonic() - t_start
    _log(f"  Inserted {input_count:,} map rows in {elapsed:.0f}s.  Building index...")
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_map_key "
        "ON rtc_dswx_map(burst_id, acq_ts, sensor)"
    )
    db.commit()
    _log(f"  Index built.  {input_count:,} input RTC entries total.")
    return input_count


# ---------------------------------------------------------------------------
# Phase 3c: compute missing RTCs via SQL set difference
# ---------------------------------------------------------------------------

def phase3c_missing(db: sqlite3.Connection) -> dict[str, int]:
    _log("Phase 3c/4: Computing missing RTCs (SQL set difference)...")
    _log("  Running NOT EXISTS query (this may take a few minutes)...")
    t0 = time.monotonic()

    db.execute("""
        INSERT INTO missing_rtcs (id)
        SELECT f.id
        FROM rtc_filtered f
        WHERE NOT EXISTS (
            SELECT 1 FROM rtc_dswx_map m
            WHERE m.burst_id = f.burst_id
              AND m.acq_ts   = f.acq_ts
              AND m.sensor   = f.sensor
        )
        ORDER BY f.id
    """)
    db.commit()
    _log(f"  Query done in {time.monotonic() - t0:.1f}s")

    filtered_count = db.execute("SELECT COUNT(*) FROM rtc_filtered").fetchone()[0]
    missing_count = db.execute("SELECT COUNT(*) FROM missing_rtcs").fetchone()[0]
    used_count = filtered_count - missing_count

    pct = (used_count / filtered_count * 100) if filtered_count else 0.0
    _log(f"  Expected: {filtered_count:,}  Actual: {used_count:,}  "
         f"Missing: {missing_count:,}  ({pct:.2f}%)")

    return {
        "expected": filtered_count,
        "actual": used_count,
        "missing_count": missing_count,
        "used_rtc_count": used_count,
        "filtered_rtc_count": filtered_count,
    }


# ---------------------------------------------------------------------------
# Phase 4: tile sets + cycles  (missing list is small enough for memory)
# ---------------------------------------------------------------------------

def phase4_tile_sets_and_cycles(
    db: sqlite3.Connection,
    mgrs_db_override: str | None,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    missing_count = db.execute("SELECT COUNT(*) FROM missing_rtcs").fetchone()[0]
    if missing_count == 0:
        _log("Phase 4/4: No missing RTCs -- skipping tile set / cycle expansion.")
        return {}, {}

    _log(f"Phase 4a/4: Loading {missing_count:,} missing RTC IDs...")
    missing_list = [
        row[0] for row in db.execute("SELECT id FROM missing_rtcs ORDER BY id")
    ]
    _log(f"  Loaded {len(missing_list):,} IDs into memory.")

    # --- Resolve to MGRS tile sets (inline with progress) ---
    _log(f"Phase 4a/4: Resolving to MGRS tile sets...")
    mgrs_db_path = str(tile_sets.resolve_mgrs_tile_db(mgrs_db_override))
    _log(f"  MGRS DB: {mgrs_db_path}")

    # Parallelize with 8 worker threads, each with its own sqlite connection.
    workers = 8
    _local = threading.local()
    conns_lock = threading.Lock()
    all_conns: list[sqlite3.Connection] = []

    def _init_worker() -> None:
        _local.conn = sqlite3.connect(mgrs_db_path, check_same_thread=False)
        with conns_lock:
            all_conns.append(_local.conn)

    _QUERY = (
        "SELECT mgrs_set_id, land_ocean_flag FROM mgrs_burst_db "
        "WHERE (SELECT 1 FROM json_each(bursts) WHERE value = ?)"
    )

    def _lookup(rtc_id: str) -> tuple[str, list[tuple[str, str]]]:
        burst_key = rtc_id.split("_")[3].lower().replace("-", "_")
        rows = _local.conn.execute(_QUERY, (burst_key,)).fetchall()
        return rtc_id, rows

    mgrs_set_to_rtc: dict[str, list[str]] = {}
    dropped_water = 0
    unmatched_bursts = 0
    t_start = time.monotonic()
    _log(f"  Starting {workers}-thread MGRS DB lookups for {missing_count:,} RTCs...")

    with ThreadPoolExecutor(max_workers=workers, initializer=_init_worker) as pool:
        futures = [pool.submit(_lookup, rtc_id) for rtc_id in missing_list]
        _log(f"  Submitted {len(futures):,} lookup futures, waiting for results...")
        completed = 0
        for fut in as_completed(futures):
            rtc_id, rows = fut.result()
            if not rows:
                unmatched_bursts += 1
            for mgrs_set_id, lof in rows:
                if lof == "water":
                    dropped_water += 1
                    continue
                mgrs_set_to_rtc.setdefault(mgrs_set_id, []).append(rtc_id)
            completed += 1
            # heartbeats: quick early feedback, then regular progress every 5K
            if completed == 1 or completed == 100 or completed == 1_000 or completed % 5_000 == 0:
                elapsed = time.monotonic() - t_start
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (missing_count - completed) / rate if rate > 0 else 0
                _log(f"  ... {completed:,}/{missing_count:,} RTCs resolved "
                     f"({len(mgrs_set_to_rtc):,} tile sets, {elapsed:.1f}s, "
                     f"{rate:.0f} RTC/s, ETA {eta/60:.1f}min)")

    for c in all_conns:
        try:
            c.close()
        except sqlite3.Error:
            pass
    elapsed = time.monotonic() - t_start
    _log(f"  Done: {len(mgrs_set_to_rtc):,} tile sets, "
         f"{dropped_water:,} water-dropped, {unmatched_bursts:,} unmatched "
         f"({elapsed:.0f}s)")

    tile_set_map = mgrs_set_to_rtc
    del missing_list
    gc.collect()

    # --- Expand with cycle indices ---
    _log("Phase 4b/4: Expanding tile sets with cycle indices...")
    expanded: dict[str, list[str]] = {}
    total_rtcs = sum(len(v) for v in tile_set_map.values())
    processed = 0
    t_start = time.monotonic()
    for ts_id, rtc_ids in tile_set_map.items():
        for rtc in rtc_ids:
            m = _RTC_RE.match(rtc)
            if m is None:
                raise ValueError(f"Failed to parse RTC granule ID: {rtc!r}")
            sensor = m.groupdict()["sensor"]
            cycle = determine_acquisition_cycle_for_rtc_granule(rtc)
            key = f"{ts_id}${cycle}${sensor}"
            expanded.setdefault(key, []).append(rtc)
            processed += 1
            if processed % 25_000 == 0:
                elapsed = time.monotonic() - t_start
                _log(f"  ... {processed:,}/{total_rtcs:,} RTCs "
                     f"({len(expanded):,} buckets, {elapsed:.0f}s)")
    elapsed = time.monotonic() - t_start
    _log(f"  Expanded {processed:,} RTCs into {len(expanded):,} buckets "
         f"({elapsed:.0f}s)")

    cycle_map = {
        k: sorted(expanded[k])
        for k in sorted(expanded.keys(), key=_tile_set_sort_key)
    }
    return tile_set_map, cycle_map


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Combine daily DSWx-S1 accountability results (disk-backed, low RAM).",
    )
    parser.add_argument("base_dir", type=Path,
                        help="Base output directory containing day_* subdirectories")
    parser.add_argument("--combined-dir", type=Path, default=None,
                        help="Where to write combined report (default: <base_dir>/combined)")
    parser.add_argument("--start", default="2024-08-28", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-07-01", help="End date (YYYY-MM-DD)")
    parser.add_argument("--venue", default="PROD", help="Venue (PROD or UAT)")
    parser.add_argument("--mgrs-db", default=None,
                        help="Path to MGRS tile DB (default: OPERA_MGRS_DB env var)")
    args = parser.parse_args()

    base_dir = args.base_dir.resolve()
    combined_dir = (args.combined_dir or base_dir / "combined").resolve()

    if not base_dir.exists():
        print(f"ERROR: base_dir does not exist: {base_dir}", file=sys.stderr)
        sys.exit(1)

    t0 = time.monotonic()
    generated_at = datetime.now()
    date_str = generated_at.strftime("%Y-%m-%d")
    report_dir = combined_dir / "reports" / "accountability" / "DSWX_S1" / date_str
    report_dir.mkdir(parents=True, exist_ok=True)

    # Discover daily output directories.
    day_dirs = sorted(
        p for p in base_dir.iterdir()
        if p.is_dir() and p.name.startswith("day_")
    )
    _log(f"Found {len(day_dirs)} daily chunk directories under {base_dir}")

    rtc_files: list[Path] = []
    dswx_files: list[Path] = []
    for d in day_dirs:
        rtc_files.extend(sorted(d.glob("reports/accountability/DSWX_S1/*/rtc_survey.json")))
        dswx_files.extend(sorted(d.glob("reports/accountability/DSWX_S1/*/dswx_survey.json")))

    if not rtc_files:
        print(f"ERROR: no rtc_survey.json found under {base_dir}/day_*", file=sys.stderr)
        sys.exit(1)
    if not dswx_files:
        print(f"ERROR: no dswx_survey.json found under {base_dir}/day_*", file=sys.stderr)
        sys.exit(1)

    _log(f"RTC survey files  : {len(rtc_files)}")
    _log(f"DSWx survey files : {len(dswx_files)}")

    # ---- Open temp SQLite DB ----
    tmp_fd, tmp_db_path = tempfile.mkstemp(suffix=".sqlite", prefix="combine_dswx_")
    os.close(tmp_fd)
    _log(f"Temp SQLite DB    : {tmp_db_path}")
    _log("")

    db = sqlite3.connect(tmp_db_path)
    try:
        _init_db(db)

        # ---- Phase 1 ----
        _log("Phase 1/4: Deduping RTC-S1 into SQLite...")
        raw_rtc, rtc_fails = phase1_rtc_dedupe(db, rtc_files)
        rtc_count = db.execute("SELECT COUNT(*) FROM rtc_deduped").fetchone()[0]
        _log(f"  => {raw_rtc:,} raw -> {rtc_count:,} unique (failures: {rtc_fails})")
        gc.collect()
        _log("")

        # ---- Phase 2 ----
        _log("Phase 2/4: Deduping DSWx-S1 into SQLite...")
        raw_dswx, dswx_fails = phase2_dswx_dedupe(db, dswx_files)
        dswx_count = db.execute("SELECT COUNT(*) FROM dswx_deduped").fetchone()[0]
        _log(f"  => {raw_dswx:,} raw -> {dswx_count:,} unique (failures: {dswx_fails})")
        gc.collect()
        _log("")

        # ---- Phase 3a ----
        filtered_count = phase3a_filter_rtcs(db)
        gc.collect()
        _log("")

        # ---- Phase 3b ----
        input_count = phase3b_build_map(db)
        gc.collect()
        _log("")

        # ---- Phase 3c ----
        stats = phase3c_missing(db)
        gc.collect()
        _log("")

        # ---- Phase 4 ----
        tile_set_map, cycle_map = phase4_tile_sets_and_cycles(db, args.mgrs_db)
        _log("")

        # ---- Write outputs ----
        _log("Writing combined report...")

        files = {
            "missing_rtc_products": report_dir / "missing_rtc_products.json",
            "rtc_to_dswx_map": report_dir / "rtc_to_dswx_map.json",
            "missing_rtcs_to_tile_sets": report_dir / "missing_rtcs_to_tile_sets.json",
            "missing_mgrs_set_cycle_indices": report_dir / "missing_mgrs_set_cycle_indices.json",
            "summary_json": report_dir / "summary.json",
            "summary_txt": report_dir / "summary.txt",
            "combine_manifest": report_dir / "combine_manifest.json",
        }

        # Stream-write missing RTCs
        _log("  Writing missing_rtc_products.json...")
        cursor = db.execute("SELECT id FROM missing_rtcs ORDER BY id")
        _stream_write_json_list(
            files["missing_rtc_products"], cursor,
            transform=lambda row: row[0],
        )

        # Stream-write rtc_to_dswx_map
        _log("  Writing rtc_to_dswx_map.json (GROUP BY query, may take a minute)...")
        cursor = db.execute("""
            SELECT burst_id || '$' || acq_ts || '$' || sensor AS key,
                   json_group_array(DISTINCT dswx_id)
            FROM rtc_dswx_map
            GROUP BY burst_id, acq_ts, sensor
            ORDER BY key
        """)
        map_entries = _stream_write_json_map(files["rtc_to_dswx_map"], cursor)
        _log(f"    {map_entries:,} map entries written")

        _log("  Writing tile set files...")
        write_json(files["missing_rtcs_to_tile_sets"], tile_set_map)
        write_json(files["missing_mgrs_set_cycle_indices"], cycle_map)

        results = {
            "metadata": {
                "product": "DSWX_S1",
                "strategy": "dswx_s1",
                "venue": args.venue,
                "start_date": datetime.strptime(args.start, "%Y-%m-%d").isoformat(),
                "end_date": datetime.strptime(args.end, "%Y-%m-%d").isoformat(),
                "generated_at": generated_at.isoformat(),
                "combined": True,
                "combined_day_count": len(day_dirs),
                "raw_rtc_records": raw_rtc,
                "raw_dswx_records": raw_dswx,
                "rtc_parse_failures": rtc_fails,
                "dswx_parse_failures": dswx_fails,
                "rtc_files_combined": len(rtc_files),
                "dswx_files_combined": len(dswx_files),
            },
            "rtc_surveyed": rtc_count,
            "dswx_surveyed": dswx_count,
            "filtered_rtc_count": stats["filtered_rtc_count"],
            "used_rtc_count": stats["used_rtc_count"],
            "missing_count": stats["missing_count"],
            "expected": stats["expected"],
            "actual": stats["actual"],
            "tile_set_count": len(tile_set_map),
            "cycle_bucket_count": len(cycle_map),
            "files": {k: str(v) for k, v in files.items()},
        }

        manifest = {
            "combined": True,
            "base_dir": str(base_dir),
            "combined_dir": str(combined_dir),
            "report_dir": str(report_dir),
            "day_count": len(day_dirs),
            "rtc_survey_files": len(rtc_files),
            "dswx_survey_files": len(dswx_files),
            "raw_rtc_records": raw_rtc,
            "raw_dswx_records": raw_dswx,
            "deduped_rtc_records": rtc_count,
            "deduped_dswx_records": dswx_count,
            "expected": stats["expected"],
            "actual": stats["actual"],
            "missing_count": stats["missing_count"],
        }

        _log("  Writing summary...")
        write_json(files["summary_json"], results)
        write_json(files["combine_manifest"], manifest)
        write_summary_txt(files["summary_txt"], results)

    finally:
        db.close()
        try:
            os.unlink(tmp_db_path)
            # WAL and SHM files
            for ext in ("-wal", "-shm"):
                p = tmp_db_path + ext
                if os.path.exists(p):
                    os.unlink(p)
        except OSError:
            pass

    elapsed = time.monotonic() - t0
    _log("")
    _log("=" * 60)
    _log(f"Done in {elapsed:.1f}s")
    _log(f"Report : {report_dir}")
    _log(f"Summary: {files['summary_txt']}")
    _log("=" * 60)


if __name__ == "__main__":
    main()
