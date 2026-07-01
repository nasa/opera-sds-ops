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
    analyze_annotations,
)
from opera_accountability.burst_coverage import (
    BurstInfo,
    SLCGranule,
    ExpectedBurst,
    RequestCache,
    geojson_to_bbox,
    generate_time_chunks,
    _parse_asf_burst_response,
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
# ASF burst response parsing tests
# =============================================================================

class TestASFParsing:
    def test_parse_empty_response(self):
        result = asyncio.run(_parse_asf_burst_response([]))
        assert result == []

    def test_parse_with_polarization_filter(self):
        data = [
            {"burst": {"fullBurstID": "035_073254_IW1"}, "polarization": "VV"},
            {"burst": {"fullBurstID": "035_073255_IW1"}, "polarization": "VH"},
        ]
        result = asyncio.run(_parse_asf_burst_response(data, polarization="VV"))
        assert len(result) == 1
        assert result[0] == "035_073254_IW1"

    def test_parse_deduplication(self):
        data = [
            {"burst": {"fullBurstID": "035_073254_IW1"}, "polarization": "VV"},
            {"burst": {"fullBurstID": "035_073254_IW1"}, "polarization": "VV"},
        ]
        result = asyncio.run(_parse_asf_burst_response(data))
        assert len(result) == 1


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
