"""Delegated-validator accountability strategy (from Chris's cmr_audit_disp_s1.py)."""

import logging
from datetime import datetime
from typing import Any

from .base import AccountabilityStrategy
from .. import CONFIG
from ..cmr import query_cmr

logger = logging.getLogger(__name__)


class DelegatedValidatorStrategy(AccountabilityStrategy):
    """
    Delegated-validator accountability strategy: delegates validation to external validator.
    
    Example:
    - DISP-S1: delegates to opera_validator.opv_disp_s1.validate_disp_s1
    
    Strategy: Query CMR for products, then pass to external validator for detailed validation.
    The external validator handles product-specific logic (e.g., burst coverage, frame states).
    """
    
    def __init__(self, product: str):
        self.product = product
        self.product_config = CONFIG['products'][product]
    
    def get_strategy_name(self) -> str:
        return "delegated_validator"
    
    def analyze(
        self,
        start_date: datetime,
        end_date: datetime,
        venue: str = 'PROD',
        **kwargs
    ) -> dict[str, Any]:
        """Run delegated-validator accountability analysis."""
        config = self.product_config.get('accountability', {}).get('delegated_validator', {})
        
        # Get validator module path (optional - if not configured, skip delegation)
        validator_module = config.get('validator_module')
        validator_function = config.get('validator_function')
        
        ccid = self.product_config['ccid'].get(venue)
        if not ccid:
            raise ValueError(f"No CCID configured for {self.product} in {venue}")
        
        logger.info(f"Querying CMR for {self.product} from {start_date} to {end_date}")
        granules = query_cmr(ccid, start_date, end_date, venue)
        
        # If validator is configured, delegate to it
        if validator_module and validator_function:
            try:
                # Dynamically import and call the validator
                module = __import__(validator_module, fromlist=[validator_function])
                validator_func = getattr(module, validator_function)
                
                logger.info(f"Delegating validation to {validator_module}.{validator_function}")
                validation_results = validator_func(start_date, end_date, granules, **kwargs)
                
                # Extract accountability metrics from validation results
                return self._extract_accountability_metrics(validation_results)
            except ImportError as e:
                logger.warning(f"Could not import validator {validator_module}: {e}")
                logger.info("Falling back to basic accountability analysis")
                return self._basic_analysis(granules)
        else:
            logger.info("No validator configured, performing basic analysis")
            return self._basic_analysis(granules)
    
    def _extract_accountability_metrics(self, validation_results: Any) -> dict[str, Any]:
        """Extract accountability metrics from validator results."""
        # This would be customized based on the specific validator's output format
        # For now, return a basic structure
        if isinstance(validation_results, dict):
            return {
                'strategy': self.get_strategy_name(),
                'delegated': True,
                **validation_results
            }
        else:
            return {
                'strategy': self.get_strategy_name(),
                'delegated': True,
                'validation_results': str(validation_results)
            }
    
    def _basic_analysis(self, granules: list[dict]) -> dict[str, Any]:
        """Perform basic accountability analysis without external validator."""
        total = len(granules)
        
        return {
            'strategy': self.get_strategy_name(),
            'delegated': False,
            'expected': total,
            'actual': total,
            'missing_count': 0,
            'missing': [],
            'total_surveyed': total,
        }
