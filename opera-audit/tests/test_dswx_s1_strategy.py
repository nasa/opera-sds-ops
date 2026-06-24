"""Unit tests for the DSWx-S1 accountability strategy (Phase 1C).

Exercises each pipeline module in isolation using small synthetic fixtures and
an ephemeral SQLite database — no CMR traffic. Integration tests that hit CMR
live in ``tests/test_cmr_integration.py``.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from opera_accountability.strategies.dswx_s1 import rtc_utils, mapping, tile_sets, cycles, survey
from opera_accountability.strategies.dswx_s1 import pipeline as ds1_pipeline


# ---------------------------------------------------------------------------
# rtc_utils
# ---------------------------------------------------------------------------


RTC_A_S1A = 'OPERA_L2_RTC-S1_T001-000001-IW1_20250101T000831Z_20250101T050419Z_S1A_30_v1.0'
RTC_B_S1A = 'OPERA_L2_RTC-S1_T001-000002-IW1_20250101T000832Z_20250101T050419Z_S1A_30_v1.0'
RTC_C_S1A = 'OPERA_L2_RTC-S1_T001-000003-IW1_20240101T000833Z_20240101T050419Z_S1A_30_v1.0'  # pre-start
RTC_D_S1C = 'OPERA_L2_RTC-S1_T001-000004-IW1_20260101T000834Z_20260101T050419Z_S1C_30_v1.0'


def test_rtc_to_id_tuple():
    assert rtc_utils.rtc_to_id_tuple(RTC_A_S1A) == (
        'T001-000001-IW1', '20250101T000831Z', 'S1A'
    )


def test_rtc_to_id_tuple_raises_on_bad_id():
    with pytest.raises(ValueError):
        rtc_utils.rtc_to_id_tuple('not-a-real-granule')


def test_reduce_input_rtc_list_strips_suffixes_and_dedupes():
    files = [
        RTC_A_S1A + '_VV.tif',
        RTC_A_S1A + '_VH.tif',
        RTC_A_S1A + '_mask.tif',
        RTC_A_S1A + '.h5',
        RTC_B_S1A + '_VV.tif',
    ]
    reduced = sorted(rtc_utils.reduce_input_rtc_list(files))
    assert reduced == sorted([RTC_A_S1A, RTC_B_S1A])


def test_determine_acquisition_cycle_is_nonnegative_and_stable():
    cycle_a = rtc_utils.determine_acquisition_cycle_for_rtc_granule(RTC_A_S1A)
    cycle_b = rtc_utils.determine_acquisition_cycle_for_rtc_granule(RTC_B_S1A)
    assert cycle_a >= 0
    assert cycle_b >= 0
    # Same acquisition ~day, adjacent bursts → cycle index should be close.
    assert abs(cycle_a - cycle_b) <= 1


# ---------------------------------------------------------------------------
# mapping.analyze
# ---------------------------------------------------------------------------


SENSOR_STARTS = {
    'S1A': datetime(2024, 8, 21, 0, 11, 56),
    'S1B': datetime(2024, 8, 21, 0, 11, 56),
    'S1C': datetime(2025, 5, 20, 0, 0, 0),
}


def test_should_include_rtc_filters_by_sensor_start():
    assert mapping.should_include_rtc(RTC_A_S1A, SENSOR_STARTS) is True
    # RTC_C_S1A has acquisition 2024-01 — before the 2024-08-21 S1A cutoff.
    assert mapping.should_include_rtc(RTC_C_S1A, SENSOR_STARTS) is False


def test_should_include_rtc_skips_unknown_sensor_with_warning(caplog):
    fake_s1d = 'OPERA_L2_RTC-S1_T001-000005-IW1_20260101T000835Z_20260101T050419Z_S1D_30_v1.0'
    # Reset the one-shot warn cache so this test is order-independent.
    mapping._warned_sensors.clear()
    with caplog.at_level('WARNING', logger=mapping.logger.name):
        assert mapping.should_include_rtc(fake_s1d, SENSOR_STARTS) is False
    assert any('S1D' in rec.message for rec in caplog.records)
    # Subsequent calls with the same sensor should not log again.
    caplog.clear()
    assert mapping.should_include_rtc(fake_s1d, SENSOR_STARTS) is False
    assert not caplog.records


def test_analyze_flags_unused_rtcs_and_filters_pre_start():
    rtc_products = [
        {'id': RTC_A_S1A},
        {'id': RTC_B_S1A},
        {'id': RTC_C_S1A},  # pre-start; should be filtered out
    ]
    dswx_products = [
        {
            'id': 'OPERA_L3_DSWx-S1_T45SYD_20250101T000838Z_20250101T111826Z_S1A_30_v1.0',
            'input_rtcs': [RTC_A_S1A],
        }
    ]

    results = mapping.analyze(rtc_products, dswx_products, SENSOR_STARTS)

    # C is filtered out by sensor-start; A and B remain. A is used, B is missing.
    assert results['filtered_rtc_count'] == 2
    assert results['used_rtc_count'] == 1
    assert results['missing_count'] == 1
    assert results['missing'] == [RTC_B_S1A]


def test_analyze_ignores_non_rtc_input_granules():
    rtc_products = [{'id': RTC_A_S1A}]
    dswx_products = [
        {
            'id': 'OPERA_L3_DSWx-S1_T45SYD_20250101T000838Z_20250101T111826Z_S1A_30_v1.0',
            'input_rtcs': [RTC_A_S1A, 'DEM-tile-some-identifier'],
        }
    ]

    results = mapping.analyze(rtc_products, dswx_products, SENSOR_STARTS)

    assert results['used_rtc_count'] == 1
    assert results['missing_count'] == 0


# ---------------------------------------------------------------------------
# tile_sets
# ---------------------------------------------------------------------------


def _make_tile_db(path: Path) -> None:
    """Create a minimal SQLite DB mirroring the shape used by tile_sets._QUERY."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE mgrs_burst_db (mgrs_set_id TEXT, land_ocean_flag TEXT, bursts TEXT)"
        )
        # RTC_A's burst → t001_000001_iw1 belongs to land tile set MS_1_1.
        conn.execute(
            "INSERT INTO mgrs_burst_db VALUES (?, ?, ?)",
            ('MS_1_1', 'land', json.dumps(['t001_000001_iw1', 't001_000002_iw1'])),
        )
        # Water tile set containing same burst — should be dropped.
        conn.execute(
            "INSERT INTO mgrs_burst_db VALUES (?, ?, ?)",
            ('MS_1_2', 'water', json.dumps(['t001_000001_iw1'])),
        )
        # Unrelated tile set.
        conn.execute(
            "INSERT INTO mgrs_burst_db VALUES (?, ?, ?)",
            ('MS_2_1', 'land', json.dumps(['t999_999999_iw1'])),
        )
        conn.commit()
    finally:
        conn.close()


def test_tile_sets_drops_water_and_groups_by_set(tmp_path: Path):
    db = tmp_path / "mgrs.sqlite"
    _make_tile_db(db)

    result = tile_sets.map_missing_rtcs_to_tile_sets(
        [RTC_A_S1A, RTC_B_S1A],
        mgrs_db_path=db,
        workers=2,
    )

    # MS_1_1 contains both bursts; MS_1_2 was water → dropped.
    assert 'MS_1_2' not in result
    assert set(result.keys()) == {'MS_1_1'}
    assert sorted(result['MS_1_1']) == sorted([RTC_A_S1A, RTC_B_S1A])


def test_resolve_mgrs_tile_db_explicit_override(tmp_path: Path):
    db = tmp_path / "mgrs.sqlite"
    _make_tile_db(db)

    resolved = tile_sets.resolve_mgrs_tile_db(str(db))
    assert resolved == db.resolve()


def test_resolve_mgrs_tile_db_override_missing_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        tile_sets.resolve_mgrs_tile_db(str(tmp_path / "does_not_exist.sqlite"))


def test_resolve_mgrs_tile_db_no_source_raises(monkeypatch):
    """Without --mgrs-db or OPERA_MGRS_DB, resolution must error clearly."""
    monkeypatch.delenv('OPERA_MGRS_DB', raising=False)
    with pytest.raises(FileNotFoundError, match="OPERA_MGRS_DB"):
        tile_sets.resolve_mgrs_tile_db(None)


def test_resolve_mgrs_tile_db_env_var(tmp_path: Path, monkeypatch):
    """OPERA_MGRS_DB env var is honored when no override is given."""
    db = tmp_path / "mgrs.sqlite"
    _make_tile_db(db)
    monkeypatch.setenv('OPERA_MGRS_DB', str(db))

    resolved = tile_sets.resolve_mgrs_tile_db(None)
    assert resolved == db.resolve()


# ---------------------------------------------------------------------------
# cycles
# ---------------------------------------------------------------------------


def test_expand_with_cycle_indices_groups_by_tile_cycle_sensor():
    base = {
        'MS_1_1': [RTC_A_S1A, RTC_B_S1A],
    }
    expanded = cycles.expand_with_cycle_indices(base)

    # Every produced key must have the form <tile>$<cycle>$<sensor>.
    for key in expanded:
        parts = key.split('$')
        assert len(parts) == 3
        tile, cycle, sensor = parts
        assert tile == 'MS_1_1'
        assert int(cycle) >= 0
        assert sensor == 'S1A'

    # All input RTCs must appear somewhere in the expanded output.
    flattened = [rtc for lst in expanded.values() for rtc in lst]
    assert sorted(flattened) == sorted([RTC_A_S1A, RTC_B_S1A])


# ---------------------------------------------------------------------------
# survey._dedupe_by_creation_ts
# ---------------------------------------------------------------------------


def test_dedupe_raises_on_unparseable_ids():
    """Phase 1: Riley's original raises RuntimeError on un-matchable granule IDs."""
    import re
    from opera_accountability import CONFIG
    import pytest

    pattern = re.compile(CONFIG['products']['RTC_S1']['pattern'])
    unique_fields = tuple(CONFIG['products']['RTC_S1']['unique_fields'])

    records = [
        {'id': RTC_A_S1A},
        {'id': 'totally-bogus-granule-id'},  # should raise RuntimeError
        {'id': RTC_B_S1A},
    ]

    with pytest.raises(RuntimeError, match="Failed to parse granule ID totally-bogus-granule-id"):
        survey._dedupe_by_creation_ts(records, pattern, unique_fields)


def test_dedupe_keeps_latest_creation_ts():
    import re
    from opera_accountability import CONFIG

    pattern = re.compile(CONFIG['products']['RTC_S1']['pattern'])
    unique_fields = tuple(CONFIG['products']['RTC_S1']['unique_fields'])

    records = [
        # Same (burst, acq, sensor) but two different creation_ts values.
        {'id': 'OPERA_L2_RTC-S1_T001-000001-IW1_20250101T000831Z_20250101T050419Z_S1A_30_v1.0'},
        {'id': 'OPERA_L2_RTC-S1_T001-000001-IW1_20250101T000831Z_20260101T050419Z_S1A_30_v1.0'},
        # Unique record.
        {'id': 'OPERA_L2_RTC-S1_T001-000002-IW1_20250101T000832Z_20250101T050419Z_S1A_30_v1.0'},
    ]

    deduped = survey._dedupe_by_creation_ts(records, pattern, unique_fields)
    assert len(deduped) == 2
    ids = {r['id'] for r in deduped}
    # The 2026 creation_ts should win for the burst-1 duplicate.
    assert 'OPERA_L2_RTC-S1_T001-000001-IW1_20250101T000831Z_20260101T050419Z_S1A_30_v1.0' in ids


# ---------------------------------------------------------------------------
# pipeline.run (no-save, no-CMR — inject surveyed data directly)
# ---------------------------------------------------------------------------


def test_pipeline_validates_sensor_epoch_coupling(tmp_path: Path, monkeypatch):
    """Regression: pipeline must fail fast when a configured sensor has no epoch."""
    import opera_accountability
    from opera_accountability.strategies.dswx_s1 import pipeline as ds1_pipeline

    # Inject a bad config where S1D is referenced but its epoch is still
    # the NOT_YET_DETERMINED sentinel (the default for S1D in _EPOCH_MAP).
    bad_config = {
        'products': {
            'DSWX_S1': {
                'accountability': {
                    'sensor_start_dates': {
                        'S1A': '2024-08-21T00:11:56Z',
                        'S1D': '2027-01-01T00:00:00Z',
                    },
                },
            },
        },
    }
    monkeypatch.setattr(opera_accountability, 'CONFIG', bad_config)
    monkeypatch.setattr(ds1_pipeline, 'CONFIG', bad_config)

    with pytest.raises(ValueError, match='S1D'):
        ds1_pipeline._validate_sensor_config()


def test_pipeline_run_with_zero_missing_rtcs_short_circuits(tmp_path: Path, monkeypatch):
    """If every surveyed RTC is used by DSWx, steps 3 & 4 should no-op."""

    def fake_survey_rtc(start, end, venue):
        return [{'id': RTC_A_S1A}]

    def fake_survey_dswx(start, end, venue):
        return [{
            'id': 'OPERA_L3_DSWx-S1_T45SYD_20250101T000838Z_20250101T111826Z_S1A_30_v1.0',
            'input_rtcs': [RTC_A_S1A],
        }]

    monkeypatch.setattr(ds1_pipeline.survey, 'survey_rtc', fake_survey_rtc)
    monkeypatch.setattr(ds1_pipeline.survey, 'survey_dswx', fake_survey_dswx)

    results = ds1_pipeline.run(
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 1, 2),
        output_dir=tmp_path,
        venue='PROD',
        save=True,
    )

    assert results['rtc_surveyed'] == 1
    assert results['dswx_surveyed'] == 1
    assert results['missing_count'] == 0
    assert results['tile_set_count'] == 0
    assert results['cycle_bucket_count'] == 0
    # Summary artifacts always written on save=True.
    assert Path(results['files']['summary_txt']).exists()
    assert Path(results['files']['summary_json']).exists()


def test_pipeline_run_with_missing_rtcs_uses_override_db(tmp_path: Path, monkeypatch):
    """End-to-end smoke with tiny fixture DB and one missing RTC."""
    db = tmp_path / "mgrs.sqlite"
    _make_tile_db(db)

    def fake_survey_rtc(start, end, venue):
        return [{'id': RTC_A_S1A}, {'id': RTC_B_S1A}]

    def fake_survey_dswx(start, end, venue):
        # Only RTC_A is used by a DSWx output → RTC_B is missing.
        return [{
            'id': 'OPERA_L3_DSWx-S1_T45SYD_20250101T000838Z_20250101T111826Z_S1A_30_v1.0',
            'input_rtcs': [RTC_A_S1A],
        }]

    monkeypatch.setattr(ds1_pipeline.survey, 'survey_rtc', fake_survey_rtc)
    monkeypatch.setattr(ds1_pipeline.survey, 'survey_dswx', fake_survey_dswx)

    results = ds1_pipeline.run(
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 1, 2),
        output_dir=tmp_path,
        venue='PROD',
        save=True,
        mgrs_db_override=str(db),
    )

    assert results['missing_count'] == 1
    assert results['missing'] == [RTC_B_S1A]
    # Missing RTC was in MS_1_1 (land) only; MS_1_2 was water → dropped.
    assert results['tile_set_count'] == 1
    assert results['cycle_bucket_count'] >= 1

    # JSON artifacts are well-formed and contain our missing RTC.
    missing_json = json.loads(Path(results['files']['missing_rtc_products']).read_text())
    assert missing_json == [RTC_B_S1A]

    tile_sets_json = json.loads(Path(results['files']['missing_rtcs_to_tile_sets']).read_text())
    assert 'MS_1_1' in tile_sets_json
    assert RTC_B_S1A in tile_sets_json['MS_1_1']
