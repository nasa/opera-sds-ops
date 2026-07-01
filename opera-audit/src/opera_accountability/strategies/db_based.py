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
        self.product_config = CONFIG["products"][product]
    
    def get_strategy_name(self) -> str:
        return "db_based"
    
    def analyze(
        self,
        start_date: datetime,
        end_date: datetime,
        venue: str = "PROD",
        db_path: Optional[str] = None,
        **kwargs
    ) -> dict[str, Any]:
        """Run DB-based accountability analysis."""
        config = self.product_config.get("accountability", {}).get("db_based", {})
        
        # Get database path from config or parameter
        if not db_path:
            db_path = config.get("db_path")
        
        if not db_path:
            raise ValueError(f"No database path configured for {self.product}")
        
        # Load reference database - try multiple resolution strategies
        db_path = Path(db_path)
        if not db_path.exists():
            # Try relative to package root (for relative paths in config.yaml)
            try:
                from importlib.resources import files as pkg_files
                pkg_root = pkg_files("opera_accountability").joinpath("../..")
                candidate = pkg_root.joinpath(db_path).resolve()
                if candidate.exists():
                    db_path = candidate
            except Exception:
                pass
        
        if not db_path.exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        logger.info(f"Loading reference database from {db_path}")
        with db_path.open() as f:
            db_data = json.load(f)
        
        # Extract expected items from database
        # For DISP-S1-STATIC, this would be frame IDs
        expected_items = self._extract_expected_items(db_data, config)
        
        # Query CMR for products
        ccid = self.product_config["ccid"].get(venue)
        if not ccid:
            raise ValueError(f"No CCID configured for {self.product} in {venue}")
        
        logger.info(f"Querying CMR for {self.product} from {start_date} to {end_date}")
        granules = query_cmr(ccid, start_date, end_date, venue)
        
        # Extract actual items from CMR results
        actual_items_raw = self._extract_actual_items(granules, config)
        
        # Filter actual items to only include those that are expected
        # (e.g., if filtering by is_north_america, don't count non-NA frames in CMR)
        actual_items = expected_items & actual_items_raw
        
        # Identify missing items (in DB but not in CMR)
        missing_items = expected_items - actual_items
        
        # Calculate metrics
        expected_count = len(expected_items)
        actual_count = len(actual_items)
        missing_count = len(missing_items)
        
        return {
            "strategy": self.get_strategy_name(),
            "expected": expected_count,
            "actual": actual_count,
            "missing_count": missing_count,
            "missing": sorted(list(missing_items)),
            "db_path": str(db_path),
            "coverage_pct": (actual_count / expected_count * 100) if expected_count > 0 else 0,
        }
    
    def _extract_expected_items(self, db_data: dict, config: dict) -> set:
        """
        Extract expected items from database.
        
        For DISP-S1-STATIC, this extracts frame IDs from the frame-to-burst DB.
        The specific extraction logic depends on the database structure.
        """
        # For DISP-S1-STATIC: extract frame IDs from frame-to-burst mapping
        if "data" in db_data:
            # Check if filtering by is_north_america is configured
            filter_north_america = config.get("filter_north_america", True)
            
            if filter_north_america:
                # Filter frames that are in North America
                return {
                    frame_id for frame_id, frame_data in db_data["data"].items()
                    if isinstance(frame_data, dict) and frame_data.get("is_north_america", False)
                }
            else:
                return set(db_data["data"].keys())
        return set()
    
    def _extract_actual_items(self, granules: list[dict], config: dict) -> set:
        """
        Extract actual items from CMR granules.
        
        For DISP-S1-STATIC, this extracts frame IDs from granule native IDs.
        """
        # For DISP-S1-STATIC: extract frame ID from native-id
        # Example: 'OPERA_L3_DISP-S1-STATIC_F16938_20140403_S1A_v1.0'
        # Frame ID is in position [3], with 'F' prefix and leading zeros
        items = set()
        for granule in granules:
            # Try to get native-id from meta first, fall back to GranuleUR
            native_id = granule.get("meta", {}).get("native-id")
            if not native_id:
                native_id = granule["umm"]["GranuleUR"]
            
            try:
                # Split by underscore and extract frame ID
                parts = native_id.split("_")
                if len(parts) >= 4 and parts[3].startswith("F"):
                    # Remove 'F' prefix and leading zeros: 'F16938' -> '16938'
                    frame_id = str(int(parts[3][1:]))
                    items.add(frame_id)
                else:
                    logger.warning(f"Could not parse frame ID from: {native_id}")
            except (ValueError, IndexError) as e:
                logger.warning(f"Failed to parse frame ID from {native_id}: {e}")
        
        return items
