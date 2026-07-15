"""Shared time chunking and resumable SQLite checkpoints.

Temporal CMR searches use intersecting, inclusive ranges.  Adjacent queries can
therefore return the same granule at a boundary.  This module makes chunking
safe by storing records under a stable product key in SQLite; repeated records
are upserted rather than counted twice.

The checkpoint is intentionally generic.  Product strategies store only the
small projection they need for their final reducer, not the full CMR response.
Each chunk is committed in one transaction, so an interrupted chunk is either
fully present and marked complete or is rerun on resume.
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimeChunk:
    """One half-open ownership interval used for a temporal query."""

    index: int
    start: datetime
    end: datetime

    @property
    def key(self) -> str:
        return f"{self.start.isoformat()}__{self.end.isoformat()}"


def generate_time_chunks(
    start: datetime,
    end: datetime,
    chunk_days: Optional[int] = 30,
) -> Iterator[TimeChunk]:
    """Yield contiguous chunks covering ``[start, end)``.

    CMR may return a boundary granule in both adjacent queries.  Ownership is
    resolved later by stable-key upserts in :class:`CheckpointStore`.
    ``chunk_days=None`` yields one chunk, which is useful for static products
    and explicit ``--no-chunking`` operation.
    """
    if end < start:
        raise ValueError("end date must be on or after start date")
    if chunk_days is not None and chunk_days < 1:
        raise ValueError("chunk_days must be at least 1")
    if start == end:
        yield TimeChunk(0, start, end)
        return

    if chunk_days is None:
        yield TimeChunk(0, start, end)
        return

    current = start
    index = 0
    delta = timedelta(days=chunk_days)
    while current < end:
        chunk_end = min(current + delta, end)
        yield TimeChunk(index, current, chunk_end)
        current = chunk_end
        index += 1


def _date_token(value: datetime) -> str:
    return value.strftime("%Y%m%dT%H%M%SZ")


class CheckpointStore:
    """SQLite-backed intermediate state for one command/product/date range."""

    def __init__(
        self,
        *,
        command: str,
        product: str,
        venue: str,
        start: datetime,
        end: datetime,
        chunk_days: Optional[int],
        output_dir: str | Path = "./output",
        checkpoint_dir: str | Path | None = None,
        resume: bool = True,
        keep: bool = False,
        extra_identity: Optional[dict[str, Any]] = None,
    ) -> None:
        self.command = command
        self.product = product
        self.venue = venue
        self.start = start
        self.end = end
        self.chunk_days = chunk_days
        self.resume = resume
        self.keep = keep
        self._successful = False

        identity = {
            "schema_version": 1,
            "command": command,
            "product": product,
            "venue": venue,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "chunk_days": chunk_days,
            "extra": extra_identity or {},
        }
        encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:12]
        run_name = f"{_date_token(start)}_{_date_token(end)}_{digest}"

        root = (
            Path(checkpoint_dir).expanduser()
            if checkpoint_dir is not None
            else Path(output_dir).expanduser() / "checkpoints"
        )
        self.path = root / command / product / venue / run_name
        self.db_path = self.path / "state.sqlite"
        self.manifest_path = self.path / "manifest.json"

        path_existed_before = self.path.exists()
        if path_existed_before and not resume:
            logger.info(
                "Discarding prior checkpoint at %s (--no-resume)", self.path
            )
            shutil.rmtree(self.path)
            path_existed_before = False
        self.path.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Checkpoint %s: %s (%s, chunk_days=%s, resume=%s, keep=%s) [%s]",
            command,
            product,
            venue,
            chunk_days,
            resume,
            keep,
            "resuming existing state" if path_existed_before else "fresh run",
        )
        logger.info("  path: %s", self.path)

        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                namespace TEXT NOT NULL,
                record_key TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (namespace, record_key)
            );
            CREATE TABLE IF NOT EXISTS chunks (
                namespace TEXT NOT NULL,
                chunk_key TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                fetched_count INTEGER NOT NULL,
                stored_count INTEGER NOT NULL,
                completed_at TEXT NOT NULL,
                PRIMARY KEY (namespace, chunk_key)
            );
            CREATE TABLE IF NOT EXISTS values_store (
                namespace TEXT NOT NULL,
                value_key TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (namespace, value_key)
            );
            CREATE TABLE IF NOT EXISTS reduced_records (
                namespace TEXT NOT NULL,
                record_key TEXT NOT NULL,
                sort_value TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (namespace, record_key)
            );
            """
        )
        self._conn.commit()

        if self.manifest_path.exists():
            existing = json.loads(self.manifest_path.read_text())
            if existing.get("identity") != identity:
                raise ValueError(
                    f"Checkpoint identity mismatch at {self.path}; choose a "
                    "different --checkpoint-dir or run with --no-resume"
                )
        else:
            self._write_manifest(identity)

    def _write_manifest(self, identity: dict[str, Any]) -> None:
        manifest = {
            "identity": identity,
            "created_at": datetime.now().isoformat(),
            "status": "running",
            "database": self.db_path.name,
        }
        self.manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))

    def is_chunk_complete(self, namespace: str, chunk: TimeChunk) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM chunks WHERE namespace = ? AND chunk_key = ?",
            (namespace, chunk.key),
        ).fetchone()
        return row is not None

    def commit_chunk(
        self,
        namespace: str,
        chunk: TimeChunk,
        records: Iterable[tuple[str, Any]],
        *,
        fetched_count: int,
    ) -> int:
        """Atomically upsert a chunk's records and mark the chunk complete."""
        stored_count = 0
        with self._conn:
            for record_key, payload in records:
                self._conn.execute(
                    """
                    INSERT INTO records(namespace, record_key, payload)
                    VALUES (?, ?, ?)
                    ON CONFLICT(namespace, record_key)
                    DO UPDATE SET payload = excluded.payload
                    """,
                    (
                        namespace,
                        str(record_key),
                        json.dumps(payload, separators=(",", ":"), default=str),
                    ),
                )
                stored_count += 1
            self._conn.execute(
                """
                INSERT OR REPLACE INTO chunks(
                    namespace, chunk_key, chunk_index, start_time, end_time,
                    fetched_count, stored_count, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    namespace,
                    chunk.key,
                    chunk.index,
                    chunk.start.isoformat(),
                    chunk.end.isoformat(),
                    fetched_count,
                    stored_count,
                    datetime.now().isoformat(),
                ),
            )
        return stored_count

    def mark_chunk_complete(
        self,
        namespace: str,
        chunk: TimeChunk,
        *,
        fetched_count: int,
        stored_count: int,
    ) -> None:
        """Mark a chunk complete after its records were persisted incrementally."""
        with self._conn:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO chunks(
                    namespace, chunk_key, chunk_index, start_time, end_time,
                    fetched_count, stored_count, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    namespace,
                    chunk.key,
                    chunk.index,
                    chunk.start.isoformat(),
                    chunk.end.isoformat(),
                    fetched_count,
                    stored_count,
                    datetime.now().isoformat(),
                ),
            )

    def iter_records(self, namespace: str) -> Iterator[tuple[str, Any]]:
        cursor = self._conn.execute(
            "SELECT record_key, payload FROM records WHERE namespace = ? ORDER BY record_key",
            (namespace,),
        )
        for record_key, payload in cursor:
            yield record_key, json.loads(payload)

    def upsert_records(
        self,
        namespace: str,
        records: Iterable[tuple[str, Any]],
    ) -> int:
        """Upsert derived records without changing chunk completion state."""
        count = 0
        with self._conn:
            for record_key, payload in records:
                self._conn.execute(
                    """
                    INSERT INTO records(namespace, record_key, payload)
                    VALUES (?, ?, ?)
                    ON CONFLICT(namespace, record_key)
                    DO UPDATE SET payload = excluded.payload
                    """,
                    (
                        namespace,
                        str(record_key),
                        json.dumps(payload, separators=(",", ":"), default=str),
                    ),
                )
                count += 1
        return count

    def iter_payloads(self, namespace: str) -> Iterator[Any]:
        for _, payload in self.iter_records(namespace):
            yield payload

    def count_records(self, namespace: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM records WHERE namespace = ?", (namespace,)
        ).fetchone()
        return int(row[0])

    def clear_namespace(self, namespace: str) -> None:
        """Clear derived state while leaving fetched chunk markers intact."""
        with self._conn:
            self._conn.execute(
                "DELETE FROM records WHERE namespace = ?", (namespace,)
            )
            self._conn.execute(
                "DELETE FROM reduced_records WHERE namespace = ?", (namespace,)
            )
            self._conn.execute(
                "DELETE FROM values_store WHERE namespace = ?", (namespace,)
            )

    def set_value(self, namespace: str, key: str, value: Any) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO values_store(namespace, value_key, payload)
                VALUES (?, ?, ?)
                ON CONFLICT(namespace, value_key)
                DO UPDATE SET payload = excluded.payload
                """,
                (namespace, key, json.dumps(value, separators=(",", ":"), default=str)),
            )

    def get_value(self, namespace: str, key: str, default: Any = None) -> Any:
        row = self._conn.execute(
            "SELECT payload FROM values_store WHERE namespace = ? AND value_key = ?",
            (namespace, key),
        ).fetchone()
        return default if row is None else json.loads(row[0])

    def upsert_reduced_records(
        self,
        namespace: str,
        records: Iterable[tuple[str, str, Any]],
    ) -> int:
        """Keep the payload with the greatest sort value for each stable key."""
        count = 0
        with self._conn:
            for record_key, sort_value, payload in records:
                self._conn.execute(
                    """
                    INSERT INTO reduced_records(namespace, record_key, sort_value, payload)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(namespace, record_key) DO UPDATE SET
                        sort_value = excluded.sort_value,
                        payload = excluded.payload
                    WHERE excluded.sort_value > reduced_records.sort_value
                    """,
                    (
                        namespace,
                        str(record_key),
                        str(sort_value),
                        json.dumps(payload, separators=(",", ":"), default=str),
                    ),
                )
                count += 1
        return count

    def iter_reduced_records(self, namespace: str) -> Iterator[tuple[str, Any]]:
        cursor = self._conn.execute(
            """
            SELECT record_key, payload FROM reduced_records
            WHERE namespace = ? ORDER BY record_key
            """,
            (namespace,),
        )
        for record_key, payload in cursor:
            yield record_key, json.loads(payload)

    def iter_reduced_payloads(self, namespace: str) -> Iterator[Any]:
        for _, payload in self.iter_reduced_records(namespace):
            yield payload

    def count_reduced_records(self, namespace: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM reduced_records WHERE namespace = ?",
            (namespace,),
        ).fetchone()
        return int(row[0])

    def count_reduced_intersection(self, left: str, right: str) -> int:
        row = self._conn.execute(
            """
            SELECT COUNT(*) FROM reduced_records AS lhs
            INNER JOIN reduced_records AS rhs
                ON rhs.record_key = lhs.record_key AND rhs.namespace = ?
            WHERE lhs.namespace = ?
            """,
            (right, left),
        ).fetchone()
        return int(row[0])

    def iter_reduced_difference_payloads(
        self,
        left: str,
        right: str,
    ) -> Iterator[Any]:
        cursor = self._conn.execute(
            """
            SELECT lhs.payload FROM reduced_records AS lhs
            LEFT JOIN reduced_records AS rhs
                ON rhs.record_key = lhs.record_key AND rhs.namespace = ?
            WHERE lhs.namespace = ? AND rhs.record_key IS NULL
            ORDER BY lhs.record_key
            """,
            (right, left),
        )
        for (payload,) in cursor:
            yield json.loads(payload)

    def chunk_status(self, namespace: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT chunk_index, start_time, end_time, fetched_count, stored_count
            FROM chunks WHERE namespace = ? ORDER BY chunk_index
            """,
            (namespace,),
        )
        return [
            {
                "index": row[0],
                "start": row[1],
                "end": row[2],
                "fetched": row[3],
                "stored": row[4],
            }
            for row in rows
        ]

    def mark_successful(self) -> None:
        self._successful = True
        manifest = json.loads(self.manifest_path.read_text())
        manifest["status"] = "complete"
        manifest["completed_at"] = datetime.now().isoformat()
        self.manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))

    def close(self) -> None:
        if getattr(self, "_conn", None) is not None:
            self._conn.close()
            self._conn = None
        if self._successful and not self.keep and self.path.exists():
            shutil.rmtree(self.path)

    def __enter__(self) -> "CheckpointStore":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if exc_type is None:
            self.mark_successful()
        self.close()

    def __del__(self) -> None:
        """Close SQLite during exception unwinding without deleting state."""
        conn = getattr(self, "_conn", None)
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass
            self._conn = None


def collect_chunked_records(
    *,
    store: CheckpointStore,
    namespace: str,
    chunks: Iterable[TimeChunk],
    query,
    project,
) -> int:
    """Query missing chunks, project records, and commit each chunk.

    ``query`` receives ``(chunk.start, chunk.end)`` and returns the raw page-
    aggregated response. ``project`` receives one raw record and returns either
    ``(stable_key, compact_payload)`` or ``None``.
    """
    # Materialize the chunk plan so operators can see N/M progress and a
    # resume-vs-new summary before any CMR traffic starts.
    chunk_list = list(chunks)
    total = len(chunk_list)
    already_complete = sum(
        1 for c in chunk_list if store.is_chunk_complete(namespace, c)
    )
    pending = total - already_complete
    if total > 0:
        logger.info(
            "[%s] plan: %d chunk(s) total, %d already complete (resume), %d pending "
            "(%s .. %s)",
            namespace,
            total,
            already_complete,
            pending,
            chunk_list[0].start.date(),
            chunk_list[-1].end.date(),
        )

    for chunk in chunk_list:
        if store.is_chunk_complete(namespace, chunk):
            logger.info(
                "[%s] chunk %d/%d SKIP (already complete): %s -> %s",
                namespace,
                chunk.index + 1,
                total,
                chunk.start.date(),
                chunk.end.date(),
            )
            continue

        logger.info(
            "[%s] chunk %d/%d RUN: %s -> %s (querying CMR...)",
            namespace,
            chunk.index + 1,
            total,
            chunk.start.date(),
            chunk.end.date(),
        )
        raw_records = query(chunk.start, chunk.end)
        projected: list[tuple[str, Any]] = []
        for record in raw_records:
            value = project(record)
            if value is not None:
                projected.append(value)
        store.commit_chunk(
            namespace,
            chunk,
            projected,
            fetched_count=len(raw_records),
        )
        logger.info(
            "[%s] chunk %d/%d DONE: %d fetched, %d projected "
            "(cumulative stored: %d)",
            namespace,
            chunk.index + 1,
            total,
            len(raw_records),
            len(projected),
            store.count_records(namespace),
        )
        del raw_records
        del projected

    return store.count_records(namespace)


def collect_paged_records(
    *,
    store: CheckpointStore,
    namespace: str,
    chunk: TimeChunk,
    pages: Iterable[Iterable[Any]],
    project,
) -> int:
    """Project and persist each remote page before requesting the next one.

    Page writes are intentionally durable before the chunk completion marker.
    If a run is interrupted, retrying the incomplete chunk safely upserts the
    same stable keys and cannot duplicate boundary/page records.
    """
    if store.is_chunk_complete(namespace, chunk):
        logger.info(
            "Resuming %s: chunk %d already complete (%s to %s)",
            namespace,
            chunk.index + 1,
            chunk.start,
            chunk.end,
        )
        return store.count_records(namespace)

    fetched_count = 0
    projected_count = 0
    page_num = 0
    for page in pages:
        projected: list[tuple[str, Any]] = []
        page_count = 0
        for record in page:
            page_count += 1
            value = project(record)
            if value is not None:
                projected.append(value)
        fetched_count += page_count
        projected_count += store.upsert_records(namespace, projected)
        page_num += 1
        if page_num <= 3 or page_num % 10 == 0:
            logger.info(
                "[%s] page %d: %d fetched, %d projected so far",
                namespace, page_num, fetched_count, projected_count,
            )
        del projected

    store.mark_chunk_complete(
        namespace,
        chunk,
        fetched_count=fetched_count,
        stored_count=projected_count,
    )
    logger.info(
        "Checkpointed %s chunk %d page-by-page: %d fetched, %d projected (%d pages)",
        namespace,
        chunk.index + 1,
        fetched_count,
        projected_count,
        page_num,
    )
    return store.count_records(namespace)
