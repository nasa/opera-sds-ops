"""Forward-map accountability strategy (ported from Chris's cmr_audit_hls.py, cmr_audit_slc.py)."""

import re
import logging
from datetime import datetime
from typing import Any, Optional
from collections import defaultdict

from .base import AccountabilityStrategy
from .. import CONFIG
from ..cmr_async import async_cmr_posts, params_to_request_body

logger = logging.getLogger(__name__)


class ForwardMapStrategy(AccountabilityStrategy):
    """
    Forward-map accountability strategy: maps input products to output products.
    
    Examples:
    - HLS (L30, S30) → DSWx-HLS outputs
    - SLC (S1A, S1B, S1C) → CSLC-S1, RTC-S1 outputs
    
    Strategy: Extract input product IDs from output product metadata,
    then query CMR for those inputs and identify missing outputs.
    """
    
    def __init__(self, product: str):
        self.product = product
        self.product_config = CONFIG['products'][product]
    
    def get_strategy_name(self) -> str:
        return "forward_map"
    
    def analyze(
        self,
        start_date: datetime,
        end_date: datetime,
        venue: str = 'PROD',
        **kwargs
    ) -> dict[str, Any]:
        """Run forward-map accountability analysis."""
        input_config = self.product_config.get('accountability', {}).get('forward_map', {})
        
        if not input_config:
            raise ValueError(f"No forward_map configuration found for {self.product}")
        
        # Get input collection info
        input_ccid = input_config.get('input_ccid', {}).get(venue)
        if not input_ccid:
            raise ValueError(f"No input CCID configured for {self.product} in {venue}")
        
        # Get output collection info
        output_ccid = self.product_config['ccid'].get(venue)
        if not output_ccid:
            raise ValueError(f"No output CCID configured for {self.product} in {venue}")
        
        # Query CMR for output products
        logger.info(f"Querying CMR for {self.product} outputs from {start_date} to {end_date}")
        output_granules = self._query_cmr(output_ccid, start_date, end_date, venue)
        
        # Extract input IDs from output products (if they contain input metadata)
        # For now, use a simple pattern-based approach
        # In a full implementation, this would parse metadata or use native ID patterns
        expected_inputs = self._extract_expected_inputs(output_granules)
        
        # Query CMR for input products
        logger.info(f"Querying CMR for input products")
        input_granules = self._query_cmr(input_ccid, start_date, end_date, venue)
        input_ids = set(g['umm']['GranuleUR'] for g in input_granules)
        
        # Find missing inputs (inputs that should exist but don't)
        missing_inputs = expected_inputs - input_ids
        
        # Calculate accountability metrics
        expected_count = len(expected_inputs)
        actual_count = len(expected_inputs & input_ids)
        missing_count = len(missing_inputs)
        
        return {
            'strategy': self.get_strategy_name(),
            'expected': expected_count,
            'actual': actual_count,
            'missing_count': missing_count,
            'missing': sorted(list(missing_inputs)),
            'input_surveyed': len(input_granules),
            'output_surveyed': len(output_granules),
        }
    
    def _query_cmr(self, ccid: str, start_date: datetime, end_date: datetime, venue: str) -> list[dict]:
        """Query CMR for granules (synchronous for now, could be async)."""
        from ..cmr import query_cmr
        return query_cmr(ccid, start_date, end_date, venue)
    
    def _extract_expected_inputs(self, output_granules: list[dict]) -> set[str]:
        """
        Extract expected input product IDs from output products.
        
        This is a simplified implementation. In Chris's tools, this involves:
        - Parsing native IDs from output products
        - Extracting input references from metadata (e.g., PostRtcOperaIds for DIST-S1)
        - For HLS/SLC, may need to parse temporal coverage or use pattern matching
        
        For now, return empty set as placeholder for full implementation.
        """
        # Placeholder: would parse output metadata to extract input references
        # This is product-specific and requires detailed metadata parsing
        return set()
