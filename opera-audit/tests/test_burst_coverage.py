"""Tests for burst_coverage and slc_annotations modules."""

import asyncio
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from opera_accountability.slc_annotations import (
    parse_burst_count,
    parse_burst_anx_times,
    derive_burst_ids,
    compute_esa_burst_id,
    parse_relative_orbit_numbers,
    parse_ascending_node_time,
    parse_burst_sensing_times,
    derive_burst_ids_from_metadata,
    analyze_annotations,
)
from opera_accountability.burst_coverage import (
    BurstInfo,
    SLCGranule,
    ExpectedBurst,
    RequestCache,
    geojson_to_bbox,
    generate_time_chunks,
    fetch_bursts_for_slc,
)


# =============================================================================
# BurstInfo tests
# =============================================================================

class TestBurstInfo:
    def test_from_asf_id(self):
        burst = BurstInfo.from_asf_id("035_073254_IW1")
        assert burst.track == 35
        assert burst.burst_num == 73254
        assert burst.subswath == "IW1"

    def test_asf_id(self):
        burst = BurstInfo(track=35, burst_num=73254, subswath="IW1")
        assert burst.asf_id == "035_073254_IW1"

    def test_opera_id(self):
        burst = BurstInfo(track=35, burst_num=73254, subswath="IW1")
        assert burst.opera_id == "T035_073254_IW1"

    def test_filename_pattern(self):
        burst = BurstInfo(track=35, burst_num=73254, subswath="IW1")
        assert burst.filename_pattern == "T035-073254-IW1"

    def test_roundtrip(self):
        original = BurstInfo(track=173, burst_num=370215, subswath="IW3")
        parsed = BurstInfo.from_asf_id(original.asf_id)
        assert parsed == original


# =============================================================================
# SLCGranule tests
# =============================================================================

class TestSLCGranule:
    def test_from_native_id_valid(self):
        native_id = "S1A_IW_SLC__1SDV_20240101T120000_20240101T120030_015470_019672_103F-SLC"
        slc = SLCGranule.from_native_id(native_id)
        assert slc is not None
        assert slc.platform == "S1A"
        assert slc.absolute_orbit == 15470
        assert slc.start_time == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert slc.end_time == datetime(2024, 1, 1, 12, 0, 30, tzinfo=timezone.utc)

    def test_from_native_id_invalid(self):
        assert SLCGranule.from_native_id("not-a-valid-slc-id") is None


# =============================================================================
# ExpectedBurst tests
# =============================================================================

class TestExpectedBurst:
    def test_to_dict(self):
        burst = BurstInfo(track=35, burst_num=73254, subswath="IW1")
        exp = ExpectedBurst(
            burst=burst,
            acquisition_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            platform="S1A",
            polarization="VV",
            slc_native_id="test-slc",
        )
        d = exp.to_dict()
        assert d["burst_id"] == "T035_073254_IW1"
        assert d["burst_pattern"] == "T035-073254-IW1"
        assert d["platform"] == "S1A"
        assert d["polarization"] == "VV"


# =============================================================================
# RequestCache tests
# =============================================================================

class TestRequestCache:
    def test_disabled_cache(self):
        cache = RequestCache(enabled=False)
        cache.set("test", {"key": "value"}, "data")
        assert cache.get("test", {"key": "value"}) is None

    def test_cache_roundtrip(self, tmp_path):
        cache = RequestCache(cache_dir=tmp_path / "cache", enabled=True)
        cache.set("test", {"key": "value"}, {"result": 42})
        result = cache.get("test", {"key": "value"})
        assert result == {"result": 42}
        assert cache.hits == 1

    def test_cache_miss(self, tmp_path):
        cache = RequestCache(cache_dir=tmp_path / "cache", enabled=True)
        result = cache.get("test", {"key": "missing"})
        assert result is None
        assert cache.misses == 1

    def test_cache_clear(self, tmp_path):
        cache = RequestCache(cache_dir=tmp_path / "cache", enabled=True)
        cache.set("test", {"key": "1"}, "data1")
        cache.set("test", {"key": "2"}, "data2")
        deleted = cache.clear()
        assert deleted >= 2
        assert cache.get("test", {"key": "1"}) is None

    def test_recheck_dates_bypass(self, tmp_path):
        cache = RequestCache(
            cache_dir=tmp_path / "cache", enabled=True,
            recheck_dates={"2024-01-01"},
        )
        cache.set("cmr_opera", {"date": "2024-01-01"}, "stale")
        # Should bypass for recheck date
        result = cache.get("cmr_opera", {"date": "2024-01-01"})
        assert result is None
        # Should NOT bypass for other dates
        cache.set("cmr_opera", {"date": "2024-01-02"}, "fresh")
        result = cache.get("cmr_opera", {"date": "2024-01-02"})
        assert result == "fresh"


# =============================================================================
# GeoJSON utility tests
# =============================================================================

class TestGeoJSON:
    def test_geojson_to_bbox(self):
        geojson = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-120, 30], [-110, 30], [-110, 40], [-120, 40], [-120, 30]
                    ]]
                }
            }]
        }
        bbox = geojson_to_bbox(geojson)
        assert bbox == (-120, 30, -110, 40)

    def test_geojson_to_bbox_empty(self):
        with pytest.raises(ValueError, match="No coordinates"):
            geojson_to_bbox({"type": "FeatureCollection", "features": []})


# =============================================================================
# Time utility tests
# =============================================================================

class TestTimeUtils:
    def test_generate_time_chunks(self):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 15, tzinfo=timezone.utc)
        chunks = list(generate_time_chunks(start, end, days=7))
        assert len(chunks) == 2
        assert chunks[0][0] == start
        assert chunks[1][1] == end


# =============================================================================
# SLC Annotation tests
# =============================================================================

class TestAnnotationParsing:
    def test_parse_burst_count(self):
        xml = b"""<?xml version="1.0"?>
        <product>
            <swathTiming>
                <burstList count="9">
                    <burst><azimuthAnxTime>100.0</azimuthAnxTime></burst>
                </burstList>
            </swathTiming>
        </product>"""
        assert parse_burst_count(xml) == 9

    def test_parse_burst_count_zero(self):
        xml = b"""<?xml version="1.0"?>
        <product><swathTiming></swathTiming></product>"""
        assert parse_burst_count(xml) == 0

    def test_derive_burst_ids_basic(self):
        anx_times = {
            "IW1": [100.0, 102.76, 105.52],
            "IW2": [100.9, 103.66, 106.42],
        }
        result = derive_burst_ids(
            anx_times, track=35, reference_burst_num=1000,
            reference_anx_time=100.0, reference_subswath="IW1",
        )
        assert len(result) == 6  # 3 IW1 + 3 IW2
        assert "035_001000_IW1" in result
        assert "035_001001_IW1" in result
        assert "035_001002_IW1" in result

    def test_derive_burst_ids_requires_two_bursts(self):
        with pytest.raises(ValueError, match="Need at least 2 bursts"):
            derive_burst_ids(
                {"IW1": [100.0]}, track=35,
                reference_burst_num=1000, reference_anx_time=100.0,
                reference_subswath="IW1",
            )

    def test_parse_metadata_and_derive_ids(self):
        manifest = b"""<xfdu><metadataSection><relativeOrbitNumber type="start">35</relativeOrbitNumber><relativeOrbitNumber type="stop">35</relativeOrbitNumber></metadataSection></xfdu>"""
        annotation = b"""<product><adsHeader><ascendingNodeTime>2024-01-01T00:00:00.000000</ascendingNodeTime></adsHeader><swathTiming><burstList count="2"><burst><sensingTime>2024-01-01T00:01:40.000000</sensingTime></burst><burst><sensingTime>2024-01-01T00:01:42.758273</sensingTime></burst></burstList></swathTiming></product>"""
        annotations = {"S1.SAFE/annotation/s1a-iw1-slc-vv-test.xml": annotation}

        assert parse_relative_orbit_numbers(manifest) == (35, 35)
        assert parse_ascending_node_time(annotations) == datetime(
            2024, 1, 1, tzinfo=timezone.utc
        )
        sensing_times = parse_burst_sensing_times(annotations)
        assert len(sensing_times["IW1"]) == 2

        expected = []
        for sensing_time in sensing_times["IW1"]:
            track, burst_id, subswath = compute_esa_burst_id(
                sensing_time,
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                35,
                35,
                "IW1",
            )
            expected.append(f"{track:03d}_{burst_id:06d}_{subswath}")
        assert derive_burst_ids_from_metadata(annotations, manifest) == expected

    def test_relative_orbit_defaults_stop_to_start(self):
        manifest = b"""<xfdu><relativeOrbitNumber type="start">175</relativeOrbitNumber></xfdu>"""
        assert parse_relative_orbit_numbers(manifest) == (175, 175)

    def test_relative_orbit_requires_start(self):
        with pytest.raises(ValueError, match="type=start"):
            parse_relative_orbit_numbers(b"<xfdu />")


class TestAnnotationOnlyBurstFetch:
    def test_success_is_cached(self):
        slc = SLCGranule.from_native_id(
            "S1A_IW_SLC__1SDV_20240101T120000_20240101T120030_015470_019672_103F-SLC"
        )
        cache = MagicMock()
        cache.get.return_value = None
        with (
            patch("opera_accountability.burst_coverage.get_cache", return_value=cache),
            patch("opera_accountability.burst_coverage._edl_token", "token"),
            patch(
                "opera_accountability.burst_coverage.get_slc_download_url",
                return_value="https://example.test/slc.zip",
            ),
            patch(
                "opera_accountability.burst_coverage.extract_slc_metadata",
                return_value=({"annotation.xml": b"xml"}, b"manifest"),
            ),
            patch(
                "opera_accountability.burst_coverage.derive_burst_ids_from_metadata",
                return_value=["035_073254_IW1"],
            ),
        ):
            result = asyncio.run(fetch_bursts_for_slc(slc, MagicMock(), MagicMock()))

        assert [burst.asf_id for burst in result] == ["035_073254_IW1"]
        cache.set.assert_called_once()
        assert cache.set.call_args.args[2] == ["035_073254_IW1"]

    def test_failure_is_not_cached(self):
        slc = SLCGranule.from_native_id(
            "S1A_IW_SLC__1SDV_20240101T120000_20240101T120030_015470_019672_103F-SLC"
        )
        cache = MagicMock()
        cache.get.return_value = None
        with (
            patch("opera_accountability.burst_coverage.get_cache", return_value=cache),
            patch("opera_accountability.burst_coverage._edl_token", "token"),
            patch(
                "opera_accountability.burst_coverage.get_slc_download_url",
                side_effect=OSError("range request failed"),
            ),
        ):
            result = asyncio.run(fetch_bursts_for_slc(slc, MagicMock(), MagicMock()))

        assert result == []
        cache.set.assert_not_called()
