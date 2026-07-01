"""Unit tests for the dashboard's ``load_reports`` and helper functions.

Streamlit's UI is exercised separately via manual smoke-testing; these tests
cover the pure I/O + normalization layer so regressions in layout handling
fail fast.
"""

from __future__ import annotations

import json
from pathlib import Path

from opera_accountability.dashboard import (
    _extract_generated_at,
    _format_age,
    _is_dswx_s1_report,
    _is_dist_s1_report,
    _status_for_accountability_rate,
    _status_for_duplicate_rate,
    _unwrap_accountability_results,
    load_reports,
)


# ---------------------------------------------------------------------------
# load_reports
# ---------------------------------------------------------------------------


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def test_load_reports_empty_when_no_reports_dir(tmp_path: Path):
    reports = load_reports(tmp_path)
    assert reports == {"duplicates": {}, "accountability": {}, "burst_coverage": {}}


def test_load_reports_flat_dswx_hls_layout(tmp_path: Path):
    """DSWX_HLS-style: ``reports/accountability/DSWX_HLS/<date>.json`` flat file."""
    base = tmp_path / "reports" / "accountability" / "DSWX_HLS"
    older = {"report_metadata": {"generated_at": "2026-01-01"},
             "results": {"expected": 10, "actual": 9, "missing_count": 1, "missing": ["a"]}}
    newer = {"report_metadata": {"generated_at": "2026-01-02"},
             "results": {"expected": 20, "actual": 18, "missing_count": 2, "missing": ["b", "c"]}}
    _write_json(base / "2026-01-01.json", older)
    _write_json(base / "2026-01-02.json", newer)

    reports = load_reports(tmp_path)
    assert "DSWX_HLS" in reports["accountability"]
    # Latest by filename sort should win.
    assert reports["accountability"]["DSWX_HLS"]["results"]["expected"] == 20


def test_load_reports_nested_dswx_s1_layout(tmp_path: Path):
    """DSWX_S1-style: ``reports/accountability/DSWX_S1/<date>/summary.json``."""
    older_dir = tmp_path / "reports" / "accountability" / "DSWX_S1" / "2026-01-01"
    newer_dir = tmp_path / "reports" / "accountability" / "DSWX_S1" / "2026-01-02"
    _write_json(older_dir / "summary.json", {
        "metadata": {"generated_at": "2026-01-01T00:00:00"},
        "rtc_surveyed": 5, "dswx_surveyed": 2,
        "filtered_rtc_count": 5, "used_rtc_count": 4,
        "missing_count": 1, "missing": ["a"],
        "tile_set_count": 0, "cycle_bucket_count": 0,
        "expected": 5, "actual": 4,
    })
    _write_json(newer_dir / "summary.json", {
        "metadata": {"generated_at": "2026-01-02T00:00:00"},
        "rtc_surveyed": 10, "dswx_surveyed": 5,
        "filtered_rtc_count": 10, "used_rtc_count": 8,
        "missing_count": 2, "missing": ["b", "c"],
        "tile_set_count": 1, "cycle_bucket_count": 1,
        "expected": 10, "actual": 8,
    })

    reports = load_reports(tmp_path)
    assert "DSWX_S1" in reports["accountability"]
    loaded = reports["accountability"]["DSWX_S1"]
    # Latest date dir wins.
    assert loaded["rtc_surveyed"] == 10
    assert loaded["tile_set_count"] == 1
    # The loader stamps the source dir so the UI can find sibling artifacts.
    assert loaded["_report_dir"] == str(newer_dir)


def test_load_reports_nested_missing_summary_is_skipped(tmp_path: Path):
    """A date-dir without summary.json shouldn't produce an accountability entry."""
    partial = tmp_path / "reports" / "accountability" / "DSWX_S1" / "2026-01-02"
    partial.mkdir(parents=True)
    (partial / "rtc_survey.json").write_text("[]")  # sibling but no summary.json

    reports = load_reports(tmp_path)
    assert "DSWX_S1" not in reports["accountability"]


def test_load_reports_dist_s1_layout(tmp_path: Path):
    """DIST-S1-style: ``reports/accountability/DIST_S1/<date>/summary.json``."""
    newer_dir = tmp_path / "reports" / "accountability" / "DIST_S1" / "2026-01-02"
    _write_json(newer_dir / "summary.json", {
        "metadata": {"generated_at": "2026-01-02T00:00:00", "strategy": "dist_s1"},
        "rtc_surveyed": 10, "dist_surveyed": 2,
        "expected": 10, "actual": 8, "missing_count": 2,
        "burst_db_enabled": True,
    })

    reports = load_reports(tmp_path)
    assert "DIST_S1" in reports["accountability"]
    loaded = reports["accountability"]["DIST_S1"]
    assert loaded["rtc_surveyed"] == 10
    assert loaded["dist_surveyed"] == 2
    assert loaded["burst_db_enabled"] is True


def test_load_reports_duplicates_layout(tmp_path: Path):
    base = tmp_path / "reports" / "duplicates" / "DSWX_HLS"
    _write_json(base / "2026-01-01.json", {
        "report_metadata": {"generated_at": "2026-01-01"},
        "results": {"total": 3, "unique": 2, "duplicates": 1,
                    "duplicate_list": ["x"], "by_date": {}},
    })

    reports = load_reports(tmp_path)
    assert reports["duplicates"]["DSWX_HLS"]["results"]["duplicates"] == 1


# ---------------------------------------------------------------------------
# _unwrap_accountability_results / _is_dswx_s1_report
# ---------------------------------------------------------------------------


def test_unwrap_handles_dswx_hls_shape():
    hls = {"report_metadata": {}, "results": {"expected": 1, "actual": 1, "missing_count": 0}}
    assert _unwrap_accountability_results(hls)["expected"] == 1


def test_unwrap_handles_dswx_s1_shape():
    ds1 = {
        "metadata": {},
        "rtc_surveyed": 1, "dswx_surveyed": 1,
        "missing_count": 0, "missing": [],
        "tile_set_count": 0, "cycle_bucket_count": 0,
        "expected": 1, "actual": 1,
    }
    # DSWX_S1 has no `results` wrapper; unwrap should return the dict as-is.
    assert _unwrap_accountability_results(ds1) is ds1


def test_is_dswx_s1_report():
    ds1 = {"rtc_surveyed": 1, "tile_set_count": 0}
    hls = {"results": {"expected": 1}}
    assert _is_dswx_s1_report(ds1) is True
    assert _is_dswx_s1_report(hls) is False


def test_is_dist_s1_report():
    dist = {"metadata": {"strategy": "dist_s1"}, "dist_surveyed": 1}
    ds1 = {"rtc_surveyed": 1, "tile_set_count": 0}
    hls = {"results": {"expected": 1}}
    assert _is_dist_s1_report(dist) is True
    assert _is_dist_s1_report(ds1) is False
    assert _is_dist_s1_report(hls) is False


# ---------------------------------------------------------------------------
# Overview helpers (_extract_generated_at, _format_age, _status_*)
# ---------------------------------------------------------------------------


def test_extract_generated_at_from_flat_hls_report():
    report = {"report_metadata": {"generated_at": "2026-04-20T17:05:00"},
              "results": {}}
    assert _extract_generated_at(report) == "2026-04-20T17:05:00"


def test_extract_generated_at_from_dswx_s1_report():
    report = {"metadata": {"generated_at": "2026-04-20T17:05:00"},
              "rtc_surveyed": 1, "tile_set_count": 0}
    assert _extract_generated_at(report) == "2026-04-20T17:05:00"


def test_extract_generated_at_returns_none_for_unknown_shape():
    assert _extract_generated_at({}) is None
    assert _extract_generated_at({"foo": "bar"}) is None


def test_format_age_unknown_on_missing_input():
    assert _format_age(None) == "unknown"
    assert _format_age("not-a-date") == "unknown"


def test_format_age_renders_absolute_local_timestamp():
    """Operators want the absolute wall-clock time in the 'Generated' column,
    not a relative label like 'Today 17:05' / '3d ago'."""
    from datetime import datetime as _dt
    sample = _dt(2026, 4, 21, 9, 5)
    result = _format_age(sample.isoformat())
    # Now includes timezone abbreviation (e.g., PDT, PST)
    assert result.startswith("2026-04-21 09:05")
    assert len(result.split()) == 3  # YYYY-MM-DD HH:MM TZ


def test_format_age_strips_trailing_z_and_converts_to_local_time():
    """ISO strings ending in 'Z' (UTC) are accepted and converted to local
    time, same as :func:`_format_meta_timestamp`."""
    from datetime import datetime as _dt, timezone as _tz
    label = _format_age("2026-04-21T16:05:00Z")
    # Round-trip the UTC instant through astimezone to compute the local
    # expected value so the test passes regardless of CI timezone.
    local_dt = _dt(2026, 4, 21, 16, 5, tzinfo=_tz.utc).astimezone()
    local_base = local_dt.strftime("%Y-%m-%d %H:%M")
    local_tz = local_dt.strftime("%Z")
    # Now includes timezone abbreviation
    assert label == f"{local_base} {local_tz}"


def test_status_for_duplicate_rate_buckets():
    """Rates < healthy → pill-healthy; < warning → pill-warning; else critical."""
    assert "opera-pill-healthy" in _status_for_duplicate_rate(0.5)
    assert "0.50%" in _status_for_duplicate_rate(0.5)
    assert "opera-pill-warning" in _status_for_duplicate_rate(2.0)
    assert "opera-pill-critical" in _status_for_duplicate_rate(10.0)
    # Boundary: exactly at the healthy/warning cutoff → warning.
    assert "opera-pill-warning" in _status_for_duplicate_rate(1.0)
    # Boundary: exactly at the warning/critical cutoff → critical.
    assert "opera-pill-critical" in _status_for_duplicate_rate(5.0)


def test_status_for_accountability_rate_buckets():
    assert "opera-pill-healthy" in _status_for_accountability_rate(99.5)
    assert "opera-pill-warning" in _status_for_accountability_rate(95.0)
    assert "opera-pill-critical" in _status_for_accountability_rate(85.0)
    # Boundary: exactly 98% → healthy (inclusive).
    assert "opera-pill-healthy" in _status_for_accountability_rate(98.0)
    # Boundary: exactly 90% → warning (inclusive).
    assert "opera-pill-warning" in _status_for_accountability_rate(90.0)


# ---------------------------------------------------------------------------
# Overview rendering regressions
# ---------------------------------------------------------------------------


def _make_streamlit_stub(monkeypatch):
    """Patch ``st`` and ``sui`` in the dashboard module with no-op stubs.

    The Overview/strategy panels make many ``streamlit`` and
    ``streamlit_shadcn_ui`` calls; for these regressions we only care about
    the data-shape arithmetic and pure-Python control flow, so the stubs
    just need to be call-tolerant. ``st.info`` in particular is *not* a
    context manager, which is precisely the bug we are guarding against —
    the stub mirrors that contract by being a plain callable.
    """
    from opera_accountability import dashboard as dash

    class _Tolerant:
        """Callable + attribute-access + no-op context manager."""

        def __call__(self, *args, **kwargs):
            return self

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def __getattr__(self, name):
            return _Tolerant()

        def __iter__(self):
            return iter([])

    class _Streamlit:
        """``st`` shim: every attr resolves to a tolerant callable except
        ``info``, which mirrors real Streamlit by being a plain callable
        (NOT a context manager)."""

        session_state: dict = {}

        def __init__(self):
            self.session_state = {}

        def __getattr__(self, name):
            if name == "info":
                # Real ``st.info`` requires a positional ``body`` arg and is
                # NOT a context manager. Surface that by raising the same
                # error Streamlit would: ``with st.info():`` should fail.
                def _info(body, *args, **kwargs):
                    return None  # plain callable; no __enter__/__exit__
                return _info
            return _Tolerant()

        def columns(self, n):
            return [_Tolerant() for _ in range(n)]

    fake_st = _Streamlit()
    monkeypatch.setattr(dash, "st", fake_st)
    monkeypatch.setattr(dash, "sui", _Tolerant())
    monkeypatch.setattr(dash, "alt", _Tolerant())
    monkeypatch.setattr(dash, "pd", _Tolerant())
    return fake_st


def test_overview_topline_handles_disp_s1_end_conflict_reports(monkeypatch):
    """Regression: a DISP_S1 end-conflict report mixed with ordinary
    duplicate reports must not crash the Overview tab.

    Before the fix the topline summed ``r['results']['duplicates']`` over
    every duplicate report, which raised ``KeyError`` because end-conflict
    reports use ``conflicting_products`` instead.
    """
    from opera_accountability import dashboard as dash

    _make_streamlit_stub(monkeypatch)

    reports = {
        "duplicates": {
            "DSWX_HLS": {
                "report_metadata": {"generated_at": "2026-04-20T17:05:00"},
                "results": {"total": 100, "unique": 95, "duplicates": 5,
                            "duplicate_list": [], "by_date": {}},
            },
            "DISP_S1": {
                "report_metadata": {"generated_at": "2026-04-20T17:05:00"},
                "results": {
                    "total": 50, "conflict_groups": 2,
                    "conflicting_products": 7, "conflicts": {},
                    "parse_failures": 0,
                },
            },
        },
        "accountability": {},
        "burst_coverage": {},
    }

    # Must not raise.
    dash._render_overview(reports)


def test_overview_chart_reads_results_wrapper(monkeypatch):
    """Regression: the duplicate-rate chart used to read ``report['total']``
    instead of ``report['results']['total']``, so every bar plotted at 0%.
    """
    from opera_accountability import dashboard as dash

    _make_streamlit_stub(monkeypatch)

    captured: dict = {}
    real_dataframe = dash.pd.DataFrame if hasattr(dash.pd, "DataFrame") else None

    import pandas as real_pd

    class _CapturingPd:
        def __getattr__(self, name):
            return getattr(real_pd, name)

        def DataFrame(self, rows, *args, **kwargs):
            captured["rows"] = list(rows)
            return real_pd.DataFrame(rows, *args, **kwargs)

    monkeypatch.setattr(dash, "pd", _CapturingPd())

    reports = {
        "duplicates": {
            "DSWX_HLS": {
                "report_metadata": {"generated_at": "2026-04-20T17:05:00"},
                "results": {"total": 100, "unique": 90, "duplicates": 10,
                            "duplicate_list": [], "by_date": {}},
            },
        },
        "accountability": {},
        "burst_coverage": {},
    }

    dash._render_overview(reports)

    rows_by_product = {row["Product"]: row for row in captured["rows"]}
    assert rows_by_product["DSWX_HLS"]["Duplicates"] == 10
    assert rows_by_product["DSWX_HLS"]["Rate (%)"] == 10.0


def test_overview_chart_reads_end_conflicts_for_disp_s1(monkeypatch):
    """End-conflict reports: chart should plot ``conflicting_products`` /
    ``total`` instead of falling through to a 0% bar."""
    from opera_accountability import dashboard as dash

    _make_streamlit_stub(monkeypatch)

    captured: dict = {}
    import pandas as real_pd

    class _CapturingPd:
        def __getattr__(self, name):
            return getattr(real_pd, name)

        def DataFrame(self, rows, *args, **kwargs):
            captured["rows"] = list(rows)
            return real_pd.DataFrame(rows, *args, **kwargs)

    monkeypatch.setattr(dash, "pd", _CapturingPd())

    reports = {
        "duplicates": {
            "DISP_S1": {
                "report_metadata": {"generated_at": "2026-04-20T17:05:00"},
                "results": {
                    "total": 50, "conflict_groups": 2,
                    "conflicting_products": 7, "conflicts": {},
                    "parse_failures": 0,
                },
            },
        },
        "accountability": {},
        "burst_coverage": {},
    }

    dash._render_overview(reports)

    rows_by_product = {row["Product"]: row for row in captured["rows"]}
    assert rows_by_product["DISP_S1"]["Duplicates"] == 7
    assert rows_by_product["DISP_S1"]["Rate (%)"] == 14.0


def test_delegated_validator_panel_does_not_use_st_info_as_context_manager(monkeypatch):
    """Regression: ``with st.info():`` raised AttributeError because
    ``st.info`` is a plain callable, not a context manager. The panel must
    render without entering a context."""
    from opera_accountability import dashboard as dash

    fake_st = _make_streamlit_stub(monkeypatch)

    info_calls: list = []
    real_info = fake_st.info

    def _spy_info(body, *args, **kwargs):
        info_calls.append(body)
        return real_info(body, *args, **kwargs)

    # Replace the bound ``info`` with a spy. Use object.__setattr__ to bypass
    # the ``__getattr__``-based shim.
    object.__setattr__(fake_st, "info", _spy_info)

    report = {
        "report_metadata": {"generated_at": "2026-04-20T17:05:00"},
        "results": {
            "strategy": "delegated_validator",
            "expected": 10, "actual": 10, "missing_count": 0,
            "delegated": True, "missing": [],
        },
    }

    # Must not raise — and must call ``st.info`` as a plain function.
    dash._render_generic_strategy_panel("DISP_S1", report, "delegated_validator")
    assert len(info_calls) == 1
    assert "delegated" in info_calls[0].lower()


def test_load_reports_burst_coverage_layout(tmp_path: Path):
    """Burst-coverage: ``reports/burst_coverage/<timestamp>.json`` flat files."""
    bc_dir = tmp_path / "reports" / "burst_coverage"
    report_data = {
        "metadata": {
            "start_datetime": "2026-01-01T00:00:00Z",
            "end_datetime": "2026-01-07T23:59:59Z",
            "geojson": "test.geojson",
            "slc_count": 42,
            "total_bursts_raw": 200,
            "unique_bursts": 150,
            "polarizations": ["VV"],
        },
        "products": {
            "CSLC-S1": {
                "expected_count": 150,
                "found_count": 145,
                "missing_count": 5,
                "coverage_percent": 96.67,
            },
            "RTC-S1": {
                "expected_count": 150,
                "found_count": 148,
                "missing_count": 2,
                "coverage_percent": 98.67,
            },
        },
    }
    _write_json(bc_dir / "2026-01-07_12-00-00.json", report_data)

    reports = load_reports(tmp_path)
    assert "2026-01-07_12-00-00" in reports["burst_coverage"]
    loaded = reports["burst_coverage"]["2026-01-07_12-00-00"]
    assert loaded["metadata"]["slc_count"] == 42
    assert loaded["products"]["CSLC-S1"]["coverage_percent"] == 96.67


def test_overview_with_burst_coverage_reports(monkeypatch):
    """Overview tab should render without errors when burst_coverage reports
    are present alongside duplicates and accountability."""
    from opera_accountability import dashboard as dash

    _make_streamlit_stub(monkeypatch)

    reports = {
        "duplicates": {},
        "accountability": {},
        "burst_coverage": {
            "2026-01-07_12-00-00": {
                "metadata": {"slc_count": 10, "unique_bursts": 50},
                "products": {
                    "CSLC-S1": {
                        "expected_count": 50,
                        "found_count": 48,
                        "missing_count": 2,
                        "coverage_percent": 96.0,
                    },
                },
            },
        },
    }

    # Must not raise.
    dash._render_overview(reports)


def test_status_pill_uses_material_symbol_icon_names():
    """Rendered HTML should embed the Material Symbol name, not an emoji."""
    from opera_accountability.dashboard import STATUS_HEALTHY, STATUS_WARNING, STATUS_CRITICAL
    assert STATUS_HEALTHY == "check_circle"
    assert STATUS_WARNING == "warning"
    assert STATUS_CRITICAL == "cancel"

    healthy_html = _status_for_duplicate_rate(0.1)
    assert 'class="material-symbols-rounded"' in healthy_html
    assert ">check_circle<" in healthy_html
