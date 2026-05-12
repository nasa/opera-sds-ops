"""DB-based accountability strategy (from Chris's cmr_audit_disp_s1_static.py)."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from .base import AccountabilityStrategy
from .. import CONFIG
from ..cmr import query_cmr

logger = logging.getLogger(__name__)


class DBBasedStrategy(AccountabilityStrategy):
    """
    DB-based accountability strategy: uses database for coverage checks.
    
    Example:
    - DISP-S1-STATIC: uses frame-to-burst DB to check which frames should have products
    
    Strategy: Load reference database (e.g., frame-to-burst mapping), query CMR for products,
    then identify gaps (items in DB but not in CMR).
    """
    
    def __init__(self, product: str):
        self.product = product
        self.product_config = CONFIG['products'][product]
    
    def get_strategy_name(self) -> str:
        return "db_based"
    
    def analyze(
        self,
        start_date: datetime,
        end_date: datetime,
        venue: str = 'PROD',
        db_path: Optional[str] = None,
        **kwargs
    ) -> dict[str, Any]:
        """Run DB-based accountability analysis."""
        config = self.product_config.get('accountability', {}).get('db_based', {})
        
        # Get database path from config or parameter
        if not db_path:
            db_path = config.get('db_path')
        
        if not db_path:
            raise ValueError(f"No database path configured for {self.product}")
        
        # Load reference database
        db_path = Path(db_path)
        if not db_path.exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        logger.info(f"Loading reference database from {db_path}")
        with db_path.open() as f:
            db_data = json.load(f)
        
        # Extract expected items from database
        # For DISP-S1-STATIC, this would be frame IDs
        expected_items = self._extract_expected_items(db_data, config)
        
        # Query CMR for products
        ccid = self.product_config['ccid'].get(venue)
        if not ccid:
            raise ValueError(f"No CCID configured for {self.product} in {venue}")
        
        logger.info(f"Querying CMR for {self.product} from {start_date} to {end_date}")
        granules = query_cmr(ccid, start_date, end_date, venue)
        
        # Extract actual items from CMR results
        actual_items = self._extract_actual_items(granules, config)
        
        # Identify missing items (in DB but not in CMR)
        missing_items = expected_items - actual_items
        
        # Calculate metrics
        expected_count = len(expected_items)
        actual_count = len(actual_items)
        missing_count = len(missing_items)
        
        return {
            'strategy': self.get_strategy_name(),
            'expected': expected_count,
            'actual': actual_count,
            'missing_count': missing_count,
            'missing': sorted(list(missing_items)),
            'db_path': str(db_path),
            'coverage_pct': (actual_count / expected_count * 100) if expected_count > 0 else 0,
        }
    
    def _extract_expected_items(self, db_data: dict, config: dict) -> set:
        """
        Extract expected items from database.
        
        For DISP-S1-STATIC, this extracts frame IDs from the frame-to-burst DB.
        The specific extraction logic depends on the database structure.
        """
        # Placeholder: would parse DB structure based on product-specific config
        # For DISP-S1-STATIC: extract frame IDs from frame-to-burst mapping
        if 'data' in db_data:
            return set(db_data['data'].keys())
        return set()
    
    def _extract_actual_items(self, granules: list[dict], config: dict) -> set:
        """
        Extract actual items from CMR granules.
        
        For DISP-S1-STATIC, this extracts frame IDs from granule native IDs.
        """
        # Placeholder: would parse granule IDs based on product-specific pattern
        # For DISP-S1-STATIC: extract frame ID from OPERA_L3_DISP-S1-STATIC_F16938_20140403_S1A_v1.0
        items = set()
        for granule in granules:
            granule_id = granule['umm']['GranuleUR']
            # Extract frame ID (product-specific parsing)
            # This would use the regex pattern from config
            items.add(granule_id)  # Placeholder
        return items
