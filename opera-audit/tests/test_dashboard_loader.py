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
    assert reports == {'duplicates': {}, 'accountability': {}}


def test_load_reports_flat_dswx_hls_layout(tmp_path: Path):
    """DSWX_HLS-style: ``reports/accountability/DSWX_HLS/<date>.json`` flat file."""
    base = tmp_path / 'reports' / 'accountability' / 'DSWX_HLS'
    older = {'report_metadata': {'generated_at': '2026-01-01'},
             'results': {'expected': 10, 'actual': 9, 'missing_count': 1, 'missing': ['a']}}
    newer = {'report_metadata': {'generated_at': '2026-01-02'},
             'results': {'expected': 20, 'actual': 18, 'missing_count': 2, 'missing': ['b', 'c']}}
    _write_json(base / '2026-01-01.json', older)
    _write_json(base / '2026-01-02.json', newer)

    reports = load_reports(tmp_path)
    assert 'DSWX_HLS' in reports['accountability']
    # Latest by filename sort should win.
    assert reports['accountability']['DSWX_HLS']['results']['expected'] == 20


def test_load_reports_nested_dswx_s1_layout(tmp_path: Path):
    """DSWX_S1-style: ``reports/accountability/DSWX_S1/<date>/summary.json``."""
    older_dir = tmp_path / 'reports' / 'accountability' / 'DSWX_S1' / '2026-01-01'
    newer_dir = tmp_path / 'reports' / 'accountability' / 'DSWX_S1' / '2026-01-02'
    _write_json(older_dir / 'summary.json', {
        'metadata': {'generated_at': '2026-01-01T00:00:00'},
        'rtc_surveyed': 5, 'dswx_surveyed': 2,
        'filtered_rtc_count': 5, 'used_rtc_count': 4,
        'missing_count': 1, 'missing': ['a'],
        'tile_set_count': 0, 'cycle_bucket_count': 0,
        'expected': 5, 'actual': 4,
    })
    _write_json(newer_dir / 'summary.json', {
        'metadata': {'generated_at': '2026-01-02T00:00:00'},
        'rtc_surveyed': 10, 'dswx_surveyed': 5,
        'filtered_rtc_count': 10, 'used_rtc_count': 8,
        'missing_count': 2, 'missing': ['b', 'c'],
        'tile_set_count': 1, 'cycle_bucket_count': 1,
        'expected': 10, 'actual': 8,
    })

    reports = load_reports(tmp_path)
    assert 'DSWX_S1' in reports['accountability']
    loaded = reports['accountability']['DSWX_S1']
    # Latest date dir wins.
    assert loaded['rtc_surveyed'] == 10
    assert loaded['tile_set_count'] == 1
    # The loader stamps the source dir so the UI can find sibling artifacts.
    assert loaded['_report_dir'] == str(newer_dir)


def test_load_reports_nested_missing_summary_is_skipped(tmp_path: Path):
    """A date-dir without summary.json shouldn't produce an accountability entry."""
    partial = tmp_path / 'reports' / 'accountability' / 'DSWX_S1' / '2026-01-02'
    partial.mkdir(parents=True)
    (partial / 'rtc_survey.json').write_text('[]')  # sibling but no summary.json

    reports = load_reports(tmp_path)
    assert 'DSWX_S1' not in reports['accountability']


def test_load_reports_duplicates_layout(tmp_path: Path):
    base = tmp_path / 'reports' / 'duplicates' / 'DSWX_HLS'
    _write_json(base / '2026-01-01.json', {
        'report_metadata': {'generated_at': '2026-01-01'},
        'results': {'total': 3, 'unique': 2, 'duplicates': 1,
                    'duplicate_list': ['x'], 'by_date': {}},
    })

    reports = load_reports(tmp_path)
    assert reports['duplicates']['DSWX_HLS']['results']['duplicates'] == 1


# ---------------------------------------------------------------------------
# _unwrap_accountability_results / _is_dswx_s1_report
# ---------------------------------------------------------------------------


def test_unwrap_handles_dswx_hls_shape():
    hls = {'report_metadata': {}, 'results': {'expected': 1, 'actual': 1, 'missing_count': 0}}
    assert _unwrap_accountability_results(hls)['expected'] == 1


def test_unwrap_handles_dswx_s1_shape():
    ds1 = {
        'metadata': {},
        'rtc_surveyed': 1, 'dswx_surveyed': 1,
        'missing_count': 0, 'missing': [],
        'tile_set_count': 0, 'cycle_bucket_count': 0,
        'expected': 1, 'actual': 1,
    }
    # DSWX_S1 has no `results` wrapper; unwrap should return the dict as-is.
    assert _unwrap_accountability_results(ds1) is ds1


def test_is_dswx_s1_report():
    ds1 = {'rtc_surveyed': 1, 'tile_set_count': 0}
    hls = {'results': {'expected': 1}}
    assert _is_dswx_s1_report(ds1) is True
    assert _is_dswx_s1_report(hls) is False


# ---------------------------------------------------------------------------
# Overview helpers (_extract_generated_at, _format_age, _status_*)
# ---------------------------------------------------------------------------


def test_extract_generated_at_from_flat_hls_report():
    report = {'report_metadata': {'generated_at': '2026-04-20T17:05:00'},
              'results': {}}
    assert _extract_generated_at(report) == '2026-04-20T17:05:00'


def test_extract_generated_at_from_dswx_s1_report():
    report = {'metadata': {'generated_at': '2026-04-20T17:05:00'},
              'rtc_surveyed': 1, 'tile_set_count': 0}
    assert _extract_generated_at(report) == '2026-04-20T17:05:00'


def test_extract_generated_at_returns_none_for_unknown_shape():
    assert _extract_generated_at({}) is None
    assert _extract_generated_at({'foo': 'bar'}) is None


def test_format_age_unknown_on_missing_input():
    assert _format_age(None) == "unknown"
    assert _format_age("not-a-date") == "unknown"


def test_format_age_renders_absolute_local_timestamp():
    """Operators want the absolute wall-clock time in the 'Generated' column,
    not a relative label like 'Today 17:05' / '3d ago'."""
    from datetime import datetime as _dt
    sample = _dt(2026, 4, 21, 9, 5)
    assert _format_age(sample.isoformat()) == "2026-04-21 09:05"


def test_format_age_strips_trailing_z_and_converts_to_local_time():
    """ISO strings ending in 'Z' (UTC) are accepted and converted to local
    time, same as :func:`_format_meta_timestamp`."""
    from datetime import datetime as _dt, timezone as _tz
    label = _format_age("2026-04-21T16:05:00Z")
    # Round-trip the UTC instant through astimezone to compute the local
    # expected value so the test passes regardless of CI timezone.
    local = _dt(2026, 4, 21, 16, 5, tzinfo=_tz.utc).astimezone().strftime('%Y-%m-%d %H:%M')
    assert label == local


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


def test_status_pill_uses_material_symbol_icon_names():
    """Rendered HTML should embed the Material Symbol name, not an emoji."""
    from opera_accountability.dashboard import STATUS_HEALTHY, STATUS_WARNING, STATUS_CRITICAL
    assert STATUS_HEALTHY == "check_circle"
    assert STATUS_WARNING == "warning"
    assert STATUS_CRITICAL == "cancel"

    healthy_html = _status_for_duplicate_rate(0.1)
    assert 'class="material-symbols-rounded"' in healthy_html
    assert '>check_circle<' in healthy_html
