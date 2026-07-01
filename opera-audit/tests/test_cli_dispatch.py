"""Regression tests for the ``opera-audit accountability`` CLI dispatcher.

These tests cover the dispatch arity / argument-order contract between
:func:`opera_accountability.cli.accountability` and its strategy helpers
(``_run_dswx_s1_accountability`` / ``_run_dist_s1_accountability``).

Without these tests the dispatcher silently passed positional arguments in
the wrong order (e.g. ``product`` ended up in the ``start_date`` slot), and
referenced a CLI option (``--mgrs-db``) that wasn't declared on the
``accountability`` command at all. Both bugs only surfaced at runtime when
a user actually selected the ``dswx_s1`` / ``dist_s1`` strategies, which
none of the unit tests for the underlying pipelines could catch.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from typer.testing import CliRunner

from opera_accountability import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _capture(monkeypatch, target_name: str) -> dict[str, Any]:
    """Replace ``cli.<target_name>`` with a capturing stub.

    Returns a dict the test can inspect after invoking the CLI.
    """
    captured: dict[str, Any] = {}

    def _stub(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(cli, target_name, _stub)
    return captured


def test_accountability_dswx_s1_passes_args_in_helper_order(monkeypatch, runner):
    """Regression: dispatcher must call _run_dswx_s1_accountability with the
    helper's positional contract (start_date, end_date, venue, save,
    output_dir, mgrs_db, quiet) — not prepend ``product`` or swap ``mgrs_db``
    and ``quiet``.
    """
    captured = _capture(monkeypatch, "_run_dswx_s1_accountability")

    result = runner.invoke(
        cli.app,
        [
            "accountability", "DSWX_S1",
            "--start", "2025-01-01",
            "--end", "2025-01-02",
            "--mgrs-db", "/tmp/mgrs.sqlite",
            "--quiet",
        ],
    )

    assert result.exit_code == 0, result.output
    args = captured["args"]
    assert args[0] == datetime(2025, 1, 1)
    assert args[1] == datetime(2025, 1, 2)
    assert args[2] == "PROD"
    assert args[3] is False  # save
    assert args[4] == "./output"  # output_dir default
    assert args[5] == "/tmp/mgrs.sqlite"  # mgrs_db
    assert args[6] is True  # quiet


def test_accountability_dist_s1_passes_args_in_helper_order(monkeypatch, runner):
    """Regression: dispatcher must call _run_dist_s1_accountability with the
    helper's positional contract (start_date, end_date, venue, save,
    output_dir, burst_db, max_concurrent, max_retries, prefer_s3, quiet).
    """
    captured = _capture(monkeypatch, "_run_dist_s1_accountability")

    result = runner.invoke(
        cli.app,
        [
            "accountability", "DIST_S1",
            "--start", "2025-01-01",
            "--end", "2025-01-02",
            "--burst-db", "/tmp/bursts.json",
            "--max-concurrent", "5",
            "--max-retries", "2",
            "--prefer-s3",
            "--quiet",
        ],
    )

    assert result.exit_code == 0, result.output
    args = captured["args"]
    assert args[0] == datetime(2025, 1, 1)
    assert args[1] == datetime(2025, 1, 2)
    assert args[2] == "PROD"
    assert args[3] is False  # save
    assert args[4] == "./output"  # output_dir default
    assert args[5] == "/tmp/bursts.json"  # burst_db
    assert args[6] == 5  # max_concurrent
    assert args[7] == 2  # max_retries
    assert args[8] is True  # prefer_s3
    assert args[9] is True  # quiet


def test_accountability_dswx_s1_without_mgrs_db_passes_none(monkeypatch, runner):
    """``--mgrs-db`` is optional; omitting it must pass ``None`` to the helper
    so that ``resolve_mgrs_tile_db`` can fall back to ``OPERA_MGRS_DB``.
    """
    captured = _capture(monkeypatch, "_run_dswx_s1_accountability")

    result = runner.invoke(
        cli.app,
        [
            "accountability", "DSWX_S1",
            "--start", "2025-01-01",
            "--end", "2025-01-02",
            "--quiet",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["args"][5] is None  # mgrs_db
