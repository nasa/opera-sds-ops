"""Unit tests for new accountability strategies (Phase 3)."""

import json
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock

from opera_accountability.strategies.forward_map import ForwardMapStrategy
from opera_accountability.strategies.date_count import DateCountStrategy
from opera_accountability.strategies.delegated_validator import DelegatedValidatorStrategy
from opera_accountability.strategies.db_based import DBBasedStrategy


class TestForwardMapStrategy:
    def test_get_strategy_name(self):
        strategy = ForwardMapStrategy("DSWX_HLS")
        assert strategy.get_strategy_name() == "forward_map"

    def test_analyze_raises_without_forward_map_config(self):
        strategy = ForwardMapStrategy("DSWX_HLS")
        # Remove forward_map config temporarily
        original_config = strategy.product_config.get("accountability", {})
        strategy.product_config["accountability"] = {}
        with pytest.raises(ValueError, match="No forward_map configuration"):
            strategy.analyze(datetime(2025, 1, 1), datetime(2025, 1, 2), "PROD")
        # Restore config
        strategy.product_config["accountability"] = original_config

    @patch("opera_accountability.cmr.query_cmr")
    def test_analyze_with_dswx_outputs(self, mock_cmr):
        """Phase 3: Chris's algorithm queries inputs first, generates expected outputs."""
        mock_cmr.side_effect = [
            # First call: INPUT granules (HLS)
            [
                {"umm": {"GranuleUR": "HLS.L30.T15SXR.2024001T180732.v2.0"}},
                {"umm": {"GranuleUR": "HLS.S30.T15SXR.2024001T180732.v2.0"}},
                {"umm": {"GranuleUR": "HLS.L30.T15SYS.2024002T180732.v2.0"}},
                {"umm": {"GranuleUR": "HLS.S30.T15SYS.2024002T180732.v2.0"}},
            ],
            # Second call: OUTPUT granules (DSWx-HLS) - only 2 out of 4 expected
            [
                {"umm": {"GranuleUR": "OPERA_L3_DSWx-HLS_T15SXR_20240101T180732Z_20240102T000000Z_S2A_30_v1.0"}},
                {"umm": {"GranuleUR": "OPERA_L3_DSWx-HLS_T15SXR_20240101T180732Z_20240102T000001Z_L8_30_v1.0"}},
            ]
        ]
        
        strategy = ForwardMapStrategy("DSWX_HLS")
        strategy.product_config = {
            "ccid": {"PROD": "C123"},
            "accountability": {
                "forward_map": {
                    "input_ccid": {"PROD": "C456"},
                    "input_product_type": "HLS"
                }
            }
        }
        results = strategy.analyze(datetime(2025, 1, 1), datetime(2025, 1, 2), "PROD")

        assert results["strategy"] == "forward_map"
        # Expected: 2 output prefixes (T15SXR + T15SYS, from 4 HLS inputs)
        assert results["expected"] == 2
        # Actual: 1 output prefix found (T15SXR only)
        assert results["actual"] == 1
        # Missing: 1 output prefix (T15SYS) not found in CMR
        assert results["missing_count"] == 1
        # Missing input granules: both T15SYS HLS inputs
        assert len(results["missing_input_granules"]) == 2

    def test_hls_to_dswx_patterns(self):
        """Phase 3: Test Chris's HLS→DSWx pattern generation."""
        from collections import defaultdict
        strategy = ForwardMapStrategy("DSWX_HLS")
        hls_ids = {
            "HLS.L30.T15SXR.2024001T180732.v2.0",
            "HLS.S30.T15SXR.2024001T180732.v2.0",
        }
        
        input_to_outputs_map = defaultdict(set)
        output_to_inputs_map = defaultdict(set)
        patterns = strategy._hls_to_dswx_patterns(hls_ids, input_to_outputs_map, output_to_inputs_map)
        
        # L30 and S30 for same tile/time generate ONE DSWx output pattern
        assert len(patterns) == 1
        # Check pattern format includes wildcard
        assert "OPERA_L3_DSWx-HLS_T15SXR_20240101T180732Z_*" in patterns
        # Check bidirectional mappings: 2 inputs map to 1 output
        assert len(input_to_outputs_map) == 2  # 2 HLS inputs
        assert len(output_to_inputs_map) == 1  # 1 DSWx output pattern
        # Check that the output maps back to both inputs
        output_pattern = list(output_to_inputs_map.keys())[0]
        assert len(output_to_inputs_map[output_pattern]) == 2


class TestDateCountStrategy:
    def test_get_strategy_name(self):
        strategy = DateCountStrategy("TROPO")
        assert strategy.get_strategy_name() == "date_count"

    @patch("opera_accountability.strategies.date_count.query_cmr")
    def test_analyze_identifies_missing_dates(self, mock_cmr):
        mock_cmr.return_value = [
            {"umm": {
                "GranuleUR": "granule_1",
                "TemporalExtent": {"RangeDateTime": {"BeginningDateTime": "2025-01-01T00:00:00Z"}},
            }},
            {"umm": {
                "GranuleUR": "granule_2",
                "TemporalExtent": {"RangeDateTime": {"BeginningDateTime": "2025-01-01T06:00:00Z"}},
            }},
        ]
        strategy = DateCountStrategy("TROPO")
        results = strategy.analyze(
            datetime(2025, 1, 1), datetime(2025, 1, 2), "PROD"
        )

        assert results["strategy"] == "date_count"
        assert results["expected_per_day"] == 4
        assert results["total_dates"] == 2
        assert results["actual_total"] == 2
        assert results["missing_dates"] == 2
        assert "2025-01-02" in results["missing"]

    @patch("opera_accountability.strategies.date_count.query_cmr")
    def test_analyze_no_granules(self, mock_cmr):
        mock_cmr.return_value = []
        strategy = DateCountStrategy("TROPO")
        results = strategy.analyze(
            datetime(2025, 1, 1), datetime(2025, 1, 1), "PROD"
        )
        assert results["actual_total"] == 0
        assert results["missing_dates"] == 1


class TestDelegatedValidatorStrategy:
    def test_get_strategy_name(self):
        strategy = DelegatedValidatorStrategy("DISP_S1")
        assert strategy.get_strategy_name() == "delegated_validator"

    def test_basic_analysis_no_validator(self):
        strategy = DelegatedValidatorStrategy("DISP_S1")
        granules = [
            {"umm": {"GranuleUR": "OPERA_L3_DISP-S1_IW_F09154_VV_20240101T000000Z_20240115T000000Z_v1.1_20240116T120000Z"}}
        ]
        results = strategy._basic_analysis(granules)
        assert results["strategy"] == "delegated_validator"
        assert results["delegated"] is False
        assert results["validated"] is False
        assert results["expected"] is None
        assert results["actual"] == 1
        assert results["missing_count"] is None
        assert "note" in results

    @patch("opera_accountability.strategies.delegated_validator.query_cmr")
    def test_analyze_falls_back_to_basic_without_validator(self, mock_cmr):
        mock_cmr.return_value = [
            {"umm": {"GranuleUR": "granule_1"}},
            {"umm": {"GranuleUR": "granule_2"}},
        ]
        strategy = DelegatedValidatorStrategy("DISP_S1")
        results = strategy.analyze(datetime(2025, 1, 1), datetime(2025, 1, 2), "PROD")
        assert results["delegated"] is False
        assert results["validated"] is False
        assert results["expected"] is None
        assert results["actual"] == 2


class TestDBBasedStrategy:
    def test_get_strategy_name(self):
        strategy = DBBasedStrategy("DISP_S1_STATIC")
        assert strategy.get_strategy_name() == "db_based"

    def test_analyze_no_db_path(self):
        strategy = DBBasedStrategy("DISP_S1_STATIC")
        # Override config to remove default db_path to test error case
        strategy.product_config = {
            "ccid": {"PROD": "C123"},
            "accountability": {
                "db_based": {
                    "db_path": None
                }
            }
        }
        with pytest.raises(ValueError, match="No database path configured"):
            strategy.analyze(datetime(2024, 1, 1), datetime(2024, 1, 2), "PROD")

    def test_analyze_db_not_found(self):
        strategy = DBBasedStrategy("DISP_S1_STATIC")
        with pytest.raises(FileNotFoundError, match="Database file not found"):
            strategy.analyze(
                datetime(2024, 1, 1), datetime(2024, 1, 2), "PROD",
                db_path="/nonexistent/db.json"
            )

    @patch("opera_accountability.strategies.db_based.query_cmr")
    def test_analyze_with_db_disp_s1_static(self, mock_cmr, tmp_path):
        # Create test frame-to-burst DB
        db_file = tmp_path / "test_db.json"
        db_data = {
            "data": {
                "16938": {"is_north_america": True},
                "16939": {"is_north_america": True},
                "16940": {"is_north_america": False},
                "16941": {"is_north_america": True}
            }
        }
        db_file.write_text(json.dumps(db_data))
        
        # Mock CMR granules with DISP-S1-STATIC native IDs
        mock_cmr.return_value = [
            {"meta": {"native-id": "OPERA_L3_DISP-S1-STATIC_F16938_20140403_S1A_v1.0"}, "umm": {"GranuleUR": "test1"}},
            {"meta": {"native-id": "OPERA_L3_DISP-S1-STATIC_F16940_20140403_S1A_v1.0"}, "umm": {"GranuleUR": "test2"}},
        ]
        
        strategy = DBBasedStrategy("DISP_S1_STATIC")
        strategy.product_config = {
            "ccid": {"PROD": "C123"},
            "accountability": {
                "db_based": {
                    "filter_north_america": True
                }
            }
        }
        
        results = strategy.analyze(
            datetime(2025, 1, 1), datetime(2025, 1, 2), "PROD",
            db_path=str(db_file)
        )
        
        assert results["strategy"] == "db_based"
        # Expected: 3 frames in North America (16938, 16939, 16941)
        assert results["expected"] == 3
        # Actual: 1 frame in CMR that's in North America (16938)
        # Note: 16940 is in CMR but not in North America, so not counted
        assert results["actual"] == 1
        # Missing: 2 frames (16939 and 16941) - they're in DB but not in CMR
        assert results["missing_count"] == 2
        assert "16939" in results["missing"]
        assert "16941" in results["missing"]
        # Coverage: 1 out of 3 = 33.33%
        assert results["coverage_pct"] == pytest.approx(33.33, rel=0.1)

    def test_extract_frame_id_from_native_id(self):
        strategy = DBBasedStrategy("DISP_S1_STATIC")
        granules = [
            {"meta": {"native-id": "OPERA_L3_DISP-S1-STATIC_F16938_20140403_S1A_v1.0"}, "umm": {"GranuleUR": "test1"}},
            {"meta": {"native-id": "OPERA_L3_DISP-S1-STATIC_F00123_20140403_S1A_v1.0"}, "umm": {"GranuleUR": "test2"}},
        ]
        
        frame_ids = strategy._extract_actual_items(granules, {})
        
        # Should strip F prefix and leading zeros
        assert "16938" in frame_ids
        assert "123" in frame_ids
        assert len(frame_ids) == 2
