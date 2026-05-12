"""Unit tests for new accountability strategies (Phase 3)."""

import pytest
from datetime import datetime

from opera_accountability.strategies.forward_map import ForwardMapStrategy
from opera_accountability.strategies.date_count import DateCountStrategy
from opera_accountability.strategies.delegated_validator import DelegatedValidatorStrategy
from opera_accountability.strategies.db_based import DBBasedStrategy


class TestForwardMapStrategy:
    """Tests for forward-map accountability strategy."""
    
    def test_get_strategy_name(self):
        """Test strategy name."""
        strategy = ForwardMapStrategy('DSWX_HLS')
        assert strategy.get_strategy_name() == 'forward_map'
    
    def test_validate_config_no_forward_map_config(self):
        """Test that strategy fails without forward_map config."""
        strategy = ForwardMapStrategy('DSWX_HLS')
        # DSWX_HLS uses dswx_hls strategy, not forward_map
        # This test would need proper config setup
        pass
    
    def test_analyze_placeholder(self):
        """Test analyze method (placeholder until full implementation)."""
        strategy = ForwardMapStrategy('DSWX_HLS')
        # This would require proper config and CMR mocking
        pass


class TestDateCountStrategy:
    """Tests for date-count accountability strategy."""
    
    def test_get_strategy_name(self):
        """Test strategy name."""
        strategy = DateCountStrategy('TROPO')
        assert strategy.get_strategy_name() == 'date_count'
    
    def test_analyze_placeholder(self):
        """Test analyze method (placeholder until full implementation)."""
        strategy = DateCountStrategy('TROPO')
        # This would require proper config and CMR mocking
        pass


class TestDelegatedValidatorStrategy:
    """Tests for delegated-validator accountability strategy."""
    
    def test_get_strategy_name(self):
        """Test strategy name."""
        strategy = DelegatedValidatorStrategy('DISP_S1')
        assert strategy.get_strategy_name() == 'delegated_validator'
    
    def test_basic_analysis_no_validator(self):
        """Test basic analysis when no validator is configured."""
        strategy = DelegatedValidatorStrategy('DISP_S1')
        
        # Mock granules
        granules = [
            {'umm': {'GranuleUR': 'OPERA_L3_DISP-S1_IW_09154_VV_20240101T000000Z_20240115T000000Z_v1.1_20240116T120000Z'}}
        ]
        
        results = strategy._basic_analysis(granules)
        
        assert results['strategy'] == 'delegated_validator'
        assert results['delegated'] is False
        assert results['expected'] == 1
        assert results['actual'] == 1
        assert results['missing_count'] == 0


class TestDBBasedStrategy:
    """Tests for DB-based accountability strategy."""
    
    def test_get_strategy_name(self):
        """Test strategy name."""
        strategy = DBBasedStrategy('DSWX_HLS')  # Use existing product for test
        assert strategy.get_strategy_name() == 'db_based'
    
    def test_analyze_no_db_path(self):
        """Test that analyze fails without database path."""
        strategy = DBBasedStrategy('DSWX_HLS')  # Use existing product for test
        
        with pytest.raises(ValueError, match="No database path configured"):
            strategy.analyze(
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
                'PROD'
            )
    
    def test_analyze_db_not_found(self):
        """Test that analyze fails if database file doesn't exist."""
        strategy = DBBasedStrategy('DSWX_HLS')  # Use existing product for test
        
        with pytest.raises(FileNotFoundError, match="Database file not found"):
            strategy.analyze(
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
                'PROD',
                db_path='/nonexistent/db.json'
            )
