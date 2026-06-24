"""Forward-map accountability strategy (ported from Chris's cmr_audit_hls.py, cmr_audit_slc.py)."""

import re
import logging
from datetime import datetime, timedelta
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
        """Run forward-map accountability analysis.
        
        Exact port of Chris's algorithm (cmr_audit_hls.py:211-248):
        1. Query CMR for input products (e.g., HLS L30/S30)
        2. Generate expected output patterns from inputs (e.g., DSWx native ID patterns)
        3. Query CMR for actual outputs
        4. Find missing outputs (inputs that should have been processed but weren't)
        """
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
        
        # Step 1: Query CMR for INPUT products (Chris's line 217-218, 220-222)
        logger.info(f"Querying CMR for input products from {start_date} to {end_date}")
        input_granules = self._query_cmr(input_ccid, start_date, end_date, venue)
        input_ids = set(g['umm']['GranuleUR'] for g in input_granules)
        logger.info(f"Expected input (granules): {len(input_ids):,}")
        
        # Step 2: Generate expected output patterns from inputs (Chris's line 224-228)
        input_to_outputs_map = defaultdict(set)
        output_to_inputs_map = defaultdict(set)
        expected_output_patterns = self._generate_output_patterns_from_inputs(
            input_ids, input_to_outputs_map, output_to_inputs_map
        )
        
        # Step 3: Query CMR for actual OUTPUT products (Chris's line 231)
        logger.info(f"Querying CMR for {self.product} outputs")
        output_granules = self._query_cmr(output_ccid, start_date, end_date, venue)
        actual_output_ids = set(g['umm']['GranuleUR'] for g in output_granules)
        
        # Step 4: Extract output prefixes (Chris's line 233-235)
        expected_output_prefixes = {pattern.rstrip('*') for pattern in expected_output_patterns}
        actual_output_prefixes = self._extract_output_prefixes(actual_output_ids)
        missing_output_prefixes = expected_output_prefixes - actual_output_prefixes
        
        # Step 5: Map back to missing inputs (Chris's line 242-243)
        missing_input_sets = [output_to_inputs_map[prefix] for prefix in missing_output_prefixes]
        missing_inputs = set()
        if missing_input_sets:
            import functools
            missing_inputs = functools.reduce(set.union, missing_input_sets)
        
        # Calculate accountability metrics (Chris's line 246-248)
        expected_count = len(input_ids)
        actual_count = len(actual_output_prefixes)
        missing_count = len(missing_inputs)
        
        logger.info(f"Fully published (granules): {actual_count:,}")
        logger.info(f"Missing processed (granules): {missing_count:,}")
        
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
    
    def _generate_output_patterns_from_inputs(
        self, 
        input_ids: set[str], 
        input_to_outputs_map: defaultdict,
        output_to_inputs_map: defaultdict
    ) -> set[str]:
        """
        Generate expected output patterns from input product IDs.
        
        Exact port of Chris's cmr_audit_hls.py:140-164 (hls_granule_ids_to_dswx_native_id_patterns)
        
        For HLS inputs: HLS.L30.T15SXR.2024001T180732.v2.0
        Generate DSWx pattern: OPERA_L3_DSWx-HLS_T15SXR_20240101T180732Z_*
        """
        forward_map_config = self.product_config.get('accountability', {}).get('forward_map', {})
        input_product_type = forward_map_config.get('input_product_type', 'HLS')
        
        if input_product_type == 'HLS':
            return self._hls_to_dswx_patterns(input_ids, input_to_outputs_map, output_to_inputs_map)
        elif input_product_type == 'SLC':
            logger.warning("SLC forward-map not yet implemented")
            return set()
        else:
            logger.warning(f"Unknown input_product_type: {input_product_type}")
            return set()
    
    def _hls_to_dswx_patterns(
        self, 
        hls_ids: set[str],
        input_to_outputs_map: defaultdict,
        output_to_inputs_map: defaultdict
    ) -> set[str]:
        """
        Convert HLS granule IDs to DSWx native ID patterns.
        
        Exact port of Chris's cmr_audit_hls.py:140-164
        """
        dswx_native_id_patterns = set()
        
        for granule in hls_ids:
            # HLS pattern (Chris's line 143-149)
            m = re.match(
                r'(?P<product_shortname>HLS[.]([LS])30)[.]'
                r'(?P<tile_id>T[^\W_]{5})[.]'
                r'(?P<acquisition_ts>(?P<year>\d{4})(?P<day_of_year>\d{3})T(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2}))[.]'
                r'(?P<collection_version>v\d+[.]\d+)$',
                granule
            )
            if not m:
                logger.debug(f"Could not parse HLS ID: {granule}")
                continue
            
            # Extract fields (Chris's line 150-155)
            tile = m.group("tile_id")
            year = m.group("year")
            doy = m.group("day_of_year")
            time_of_day = m.group("acquisition_ts").split("T")[1]
            date = datetime(int(year), 1, 1) + timedelta(int(doy) - 1)
            dswx_acquisition_dt_str = f"{date.strftime('%Y%m%d')}T{time_of_day}"
            
            # Generate DSWx pattern (Chris's line 157)
            dswx_native_id_pattern = f'OPERA_L3_DSWx-HLS_{tile}_{dswx_acquisition_dt_str}Z_*'
            dswx_native_id_patterns.add(dswx_native_id_pattern)
            
            # Bidirectional mapping (Chris's line 160-162)
            input_to_outputs_map[granule].add(dswx_native_id_pattern[:-1])  # strip wildcard
            output_to_inputs_map[dswx_native_id_pattern[:-1]].add(granule)
        
        return dswx_native_id_patterns
    
    def _extract_output_prefixes(self, output_ids: set[str]) -> set[str]:
        """
        Extract DSWx native ID prefixes from full product IDs.
        
        Exact port of Chris's cmr_audit_hls.py:167-175 (dswx_native_ids_to_prefixes)
        """
        dswx_regex_pattern = (
            r'(?P<project>OPERA)_'
            r'(?P<level>L3)_'
            r'(?P<product_type>DSWx)-(?P<source>HLS)_'
            r'(?P<tile_id>T[^\W_]{5})_'
            r'(?P<acquisition_ts>(?P<acq_year>\d{4})(?P<acq_month>\d{2})(?P<acq_day>\d{2})T(?P<acq_hour>\d{2})(?P<acq_minute>\d{2})(?P<acq_second>\d{2})Z)_'
        )
        prefixes = set()
        for prefix in output_ids:
            m = re.match(dswx_regex_pattern, prefix)
            if m:
                prefixes.add(m.group(0))
        return prefixes
