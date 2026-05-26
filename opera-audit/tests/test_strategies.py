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
        strategy = ForwardMapStrategy('DSWX_HLS')
        assert strategy.get_strategy_name() == 'forward_map'

    def test_analyze_raises_without_forward_map_config(self):
        strategy = ForwardMapStrategy('DSWX_HLS')
        with pytest.raises(ValueError, match="No forward_map configuration"):
            strategy.analyze(datetime(2025, 1, 1), datetime(2025, 1, 2), 'PROD')

    @patch('opera_accountability.cmr.query_cmr')
    def test_analyze_with_no_expected_inputs(self, mock_cmr):
        mock_cmr.return_value = [
            {'umm': {'GranuleUR': 'output_granule_1'}},
        ]
        strategy = ForwardMapStrategy('DSWX_HLS')
        strategy.product_config = {
            'ccid': {'PROD': 'C123'},
            'accountability': {
                'forward_map': {
                    'input_ccid': {'PROD': 'C456'},
                }
            }
        }
        results = strategy.analyze(datetime(2025, 1, 1), datetime(2025, 1, 2), 'PROD')

        assert results['strategy'] == 'forward_map'
        assert results['expected'] == 0
        assert results['missing_count'] == 0


class TestDateCountStrategy:
    def test_get_strategy_name(self):
        strategy = DateCountStrategy('TROPO')
        assert strategy.get_strategy_name() == 'date_count'

    @patch('opera_accountability.strategies.date_count.query_cmr')
    def test_analyze_identifies_missing_dates(self, mock_cmr):
        mock_cmr.return_value = [
            {'umm': {
                'GranuleUR': 'granule_1',
                'TemporalExtent': {'RangeDateTime': {'BeginningDateTime': '2025-01-01T00:00:00Z'}},
            }},
            {'umm': {
                'GranuleUR': 'granule_2',
                'TemporalExtent': {'RangeDateTime': {'BeginningDateTime': '2025-01-01T06:00:00Z'}},
            }},
        ]
        strategy = DateCountStrategy('TROPO')
        results = strategy.analyze(
            datetime(2025, 1, 1), datetime(2025, 1, 2), 'PROD'
        )

        assert results['strategy'] == 'date_count'
        assert results['expected_per_day'] == 4
        assert results['total_dates'] == 2
        assert results['actual_total'] == 2
        assert results['missing_dates'] == 2
        assert '2025-01-02' in results['missing']

    @patch('opera_accountability.strategies.date_count.query_cmr')
    def test_analyze_no_granules(self, mock_cmr):
        mock_cmr.return_value = []
        strategy = DateCountStrategy('TROPO')
        results = strategy.analyze(
            datetime(2025, 1, 1), datetime(2025, 1, 1), 'PROD'
        )
        assert results['actual_total'] == 0
        assert results['missing_dates'] == 1


class TestDelegatedValidatorStrategy:
    def test_get_strategy_name(self):
        strategy = DelegatedValidatorStrategy('DISP_S1')
        assert strategy.get_strategy_name() == 'delegated_validator'

    def test_basic_analysis_no_validator(self):
        strategy = DelegatedValidatorStrategy('DISP_S1')
        granules = [
            {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_F09154_VV_20240101T000000Z_20240115T000000Z_v1.1_20240116T120000Z'}}
        ]
        results = strategy._basic_analysis(granules)
        assert results['strategy'] == 'delegated_validator'
        assert results['delegated'] is False
        assert results['expected'] == 1
        assert results['actual'] == 1
        assert results['missing_count'] == 0

    @patch('opera_accountability.strategies.delegated_validator.query_cmr')
    def test_analyze_falls_back_to_basic_without_validator(self, mock_cmr):
        mock_cmr.return_value = [
            {'umm': {'GranuleUR': 'granule_1'}},
            {'umm': {'GranuleUR': 'granule_2'}},
        ]
        strategy = DelegatedValidatorStrategy('DISP_S1')
        results = strategy.analyze(datetime(2025, 1, 1), datetime(2025, 1, 2), 'PROD')
        assert results['delegated'] is False
        assert results['expected'] == 2
        assert results['actual'] == 2


class TestDBBasedStrategy:
    def test_get_strategy_name(self):
        strategy = DBBasedStrategy('DSWX_HLS')
        assert strategy.get_strategy_name() == 'db_based'

    def test_analyze_no_db_path(self):
        strategy = DBBasedStrategy('DSWX_HLS')
        with pytest.raises(ValueError, match="No database path configured"):
            strategy.analyze(datetime(2024, 1, 1), datetime(2024, 1, 2), 'PROD')

    def test_analyze_db_not_found(self):
        strategy = DBBasedStrategy('DSWX_HLS')
        with pytest.raises(FileNotFoundError, match="Database file not found"):
            strategy.analyze(
                datetime(2024, 1, 1), datetime(2024, 1, 2), 'PROD',
                db_path='/nonexistent/db.json'
            )

    @patch('opera_accountability.strategies.db_based.query_cmr')
    def test_analyze_with_db(self, mock_cmr, tmp_path):
        db_file = tmp_path / "test_db.json"
        db_file.write_text(json.dumps({'data': {'frame1': [], 'frame2': [], 'frame3': []}}))
        mock_cmr.return_value = [
            {'umm': {'GranuleUR': 'frame1'}},
        ]
        strategy = DBBasedStrategy('DSWX_HLS')
        results = strategy.analyze(
            datetime(2025, 1, 1), datetime(2025, 1, 2), 'PROD',
            db_path=str(db_file)
        )
        assert results['strategy'] == 'db_based'
        assert results['expected'] == 3
        assert results['actual'] == 1
        assert results['missing_count'] == 2
        assert results['coverage_pct'] == pytest.approx(33.33, rel=0.1)
