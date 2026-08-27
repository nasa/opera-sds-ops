"""Shared chunk/checkpoint behavior, including inclusive CMR boundaries."""

from datetime import datetime
import json
import asyncio
from datetime import timezone

from opera_accountability.checkpoint import (
    CheckpointStore,
    collect_chunked_records,
    collect_paged_records,
    generate_time_chunks,
)
from opera_accountability.strategies.dswx_s1 import mapping
from opera_accountability.strategies.dswx_s1 import pipeline as dswx_pipeline
from opera_accountability.strategies.dswx_s1 import survey as dswx_survey
from opera_accountability import burst_coverage


def _store(tmp_path, *, keep=True):
    return CheckpointStore(
        command="test",
        product="RTC_S1",
        venue="PROD",
        start=datetime(2026, 2, 1),
        end=datetime(2026, 2, 3),
        chunk_days=1,
        output_dir=tmp_path,
        resume=True,
        keep=keep,
    )


def test_generate_time_chunks_are_contiguous():
    chunks = list(
        generate_time_chunks(
            datetime(2026, 2, 1), datetime(2026, 2, 3), chunk_days=1
        )
    )
    assert len(chunks) == 2
    assert chunks[0].end == chunks[1].start


def test_checkpoint_resume_and_boundary_upsert(tmp_path):
    chunks = list(
        generate_time_chunks(
            datetime(2026, 2, 1), datetime(2026, 2, 3), chunk_days=1
        )
    )
    store = _store(tmp_path)
    store.commit_chunk(
        "products",
        chunks[0],
        [("boundary", {"revision": 1})],
        fetched_count=1,
    )
    path = store.path
    store.close()  # interrupted/unfinished runs retain their state

    resumed = _store(tmp_path)
    assert resumed.path == path
    assert resumed.is_chunk_complete("products", chunks[0])
    resumed.commit_chunk(
        "products",
        chunks[1],
        [
            ("boundary", {"revision": 2}),
            ("new", {"revision": 1}),
        ],
        fetched_count=2,
    )

    assert resumed.count_records("products") == 2
    assert dict(resumed.iter_records("products"))["boundary"] == {"revision": 2}
    resumed.mark_successful()
    resumed.close()


def test_collect_chunked_records_skips_completed_chunks(tmp_path):
    chunks = list(
        generate_time_chunks(
            datetime(2026, 2, 1), datetime(2026, 2, 3), chunk_days=1
        )
    )
    calls = []

    def query(start, end):
        calls.append((start, end))
        return [{"id": f"record-{len(calls)}"}]

    store = _store(tmp_path)
    collect_chunked_records(
        store=store,
        namespace="products",
        chunks=chunks,
        query=query,
        project=lambda record: (record["id"], record),
    )
    assert len(calls) == 2

    collect_chunked_records(
        store=store,
        namespace="products",
        chunks=chunks,
        query=query,
        project=lambda record: (record["id"], record),
    )
    assert len(calls) == 2
    store.mark_successful()
    store.close()


def test_collect_paged_records_persists_each_page_and_marks_complete(tmp_path):
    chunk = list(
        generate_time_chunks(
            datetime(2026, 2, 1), datetime(2026, 2, 2), chunk_days=None
        )
    )[0]
    store = _store(tmp_path)
    yielded = []

    def pages():
        for index in range(3):
            yielded.append(index)
            yield [{"id": f"record-{index}"}]
            assert store.count_records("products") == index + 1

    collect_paged_records(
        store=store,
        namespace="products",
        chunk=chunk,
        pages=pages(),
        project=lambda record: (record["id"], record),
    )

    assert yielded == [0, 1, 2]
    assert store.count_records("products") == 3
    assert store.is_chunk_complete("products", chunk)
    assert store.chunk_status("products")[0]["fetched"] == 3
    store.mark_successful()
    store.close()


def test_successful_checkpoint_is_removed_by_default(tmp_path):
    store = _store(tmp_path, keep=False)
    path = store.path
    store.mark_successful()
    store.close()
    assert not path.exists()


def test_dswx_s1_checkpoint_reducer_keeps_latest_and_global_difference(tmp_path):
    store = _store(tmp_path)
    rtc_old = (
        "OPERA_L2_RTC-S1_T001-000001-IW1_20250101T000000Z_"
        "20250101T010000Z_S1A_30_v1.0"
    )
    rtc_new = (
        "OPERA_L2_RTC-S1_T001-000001-IW1_20250101T000000Z_"
        "20250101T020000Z_S1A_30_v1.0"
    )
    rtc_missing = (
        "OPERA_L2_RTC-S1_T001-000002-IW1_20250101T000000Z_"
        "20250101T010000Z_S1A_30_v1.0"
    )
    dswx = (
        "OPERA_L3_DSWx-S1_T10ABC_20250101T000000Z_"
        "20250101T030000Z_S1A_30_v1.0"
    )
    store.upsert_records(
        "rtc_survey",
        [
            (rtc_old, {"id": rtc_old, "revision_timestamp": "old"}),
            (rtc_new, {"id": rtc_new, "revision_timestamp": "new"}),
            (rtc_missing, {"id": rtc_missing, "revision_timestamp": "new"}),
        ],
    )
    store.upsert_records(
        "dswx_survey",
        [(dswx, {"id": dswx, "input_rtcs": [rtc_new]})],
    )

    result = mapping.analyze_checkpoint(store)

    assert result["rtc_surveyed"] == 2
    assert result["dswx_surveyed"] == 1
    assert result["expected"] == 2
    assert result["actual"] == 1
    assert result["missing"] == [rtc_missing]
    assert store.count_records("rtc_to_dswx_pairs") == 1
    store.mark_successful()
    store.close()


def test_dswx_s1_pipeline_uses_streaming_checkpoint_reducer(tmp_path, monkeypatch):
    rtc = (
        "OPERA_L2_RTC-S1_T001-000001-IW1_20250101T000000Z_"
        "20250101T020000Z_S1A_30_v1.0"
    )
    dswx = (
        "OPERA_L3_DSWx-S1_T10ABC_20250101T000000Z_"
        "20250101T030000Z_S1A_30_v1.0"
    )

    def fake_query(ccid, start, end, venue):
        if "RTC" in ccid or ccid.endswith("-ASF"):
            return [{
                "meta": {"revision-date": "2025-01-01T02:00:00Z"},
                "umm": {"GranuleUR": rtc},
            }]
        return [{
            "meta": {"revision-date": "2025-01-01T03:00:00Z"},
            "umm": {"GranuleUR": dswx, "InputGranules": [rtc]},
        }]

    monkeypatch.setattr(dswx_survey, "query_cmr", fake_query)
    result = dswx_pipeline.run(
        start_date=datetime(2026, 2, 1),
        end_date=datetime(2026, 2, 2),
        output_dir=tmp_path,
        save=True,
        validate_coverage=False,
        chunk_days=1,
    )

    assert result["expected"] == 1
    assert result["actual"] == 1
    assert result["missing"] == []
    assert json.loads((tmp_path / result["files"]["rtc_survey"]).read_text())
    assert json.loads((tmp_path / result["files"]["rtc_to_dswx_map"]).read_text())


def test_burst_coverage_checkpoint_dedupes_adjacent_chunks(tmp_path, monkeypatch):
    class Geometry:
        def buffer(self, value):
            return self

    slc_id = "S1A_IW_SLC__1SDV_20260201T000000_20260201T000100_000001_A_B-SLC"
    expected = burst_coverage.ExpectedBurst(
        burst=burst_coverage.BurstInfo(1, 1, "IW1"),
        acquisition_time=datetime(2026, 2, 1, tzinfo=timezone.utc),
        platform="S1A",
        polarization="VV",
        slc_native_id=slc_id,
    )

    monkeypatch.setattr(burst_coverage, "load_geojson", lambda path: {})
    monkeypatch.setattr(
        burst_coverage, "geojson_to_bbox", lambda value: (0.0, 0.0, 1.0, 1.0)
    )
    monkeypatch.setattr(burst_coverage, "geojson_to_shapely", lambda value: Geometry())
    monkeypatch.setattr(
        burst_coverage, "polygon_intersects_geojson", lambda points, geom: True
    )

    async def fake_slcs(start, end, bbox):
        return {slc_id}, {
            slc_id: {
                "umm": {
                    "SpatialExtent": {
                        "HorizontalSpatialDomain": {
                            "Geometry": {
                                "GPolygons": [{
                                    "Boundary": {"Points": [{"Longitude": 0, "Latitude": 0}]}
                                }]
                            }
                        }
                    }
                }
            }
        }

    async def fake_expected(slcs, polarizations):
        return 1, [expected]

    async def fake_coverage(expected_bursts, product_type):
        return [
            {**expected_bursts[0].to_dict(), "opera_product_id": "RTC-1"}
        ], []

    monkeypatch.setattr(burst_coverage, "fetch_slc_granules", fake_slcs)
    monkeypatch.setattr(
        burst_coverage, "process_slcs_to_expected_bursts", fake_expected
    )
    monkeypatch.setattr(
        burst_coverage, "check_coverage_for_bursts", fake_coverage
    )

    result = asyncio.run(
        burst_coverage.audit_burst_coverage(
            start_datetime=datetime(2026, 2, 1, tzinfo=timezone.utc),
            end_datetime=datetime(2026, 2, 3, tzinfo=timezone.utc),
            geojson_path="fake.geojson",
            product_types=["RTC-S1"],
            polarizations=["VV"],
            chunk_days=1,
            checkpoint_output_dir=str(tmp_path),
        )
    )

    assert result["metadata"]["slc_count"] == 1
    assert result["products"]["RTC-S1"]["expected_count"] == 1
    assert result["products"]["RTC-S1"]["found_count"] == 1
