"""Date-count accountability strategy (ported from Chris's cmr_audit_tropo.py)."""

import logging
from datetime import datetime, timedelta
from typing import Any
from collections import defaultdict

from .base import AccountabilityStrategy
from .. import CONFIG
from ..cmr import query_cmr

logger = logging.getLogger(__name__)


class DateCountStrategy(AccountabilityStrategy):
    """
    Date-count accountability strategy: counts products by date and identifies gaps.
    
    Example:
    - TROPO-ZENITH: expects 4 granules per day (one per model)
    
    Strategy: Count granules by beginning date, identify dates below threshold.
    """
    
    def __init__(self, product: str):
        self.product = product
        self.product_config = CONFIG["products"][product]
    
    def get_strategy_name(self) -> str:
        return "date_count"
    
    def analyze(
        self,
        start_date: datetime,
        end_date: datetime,
        venue: str = "PROD",
        **kwargs
    ) -> dict[str, Any]:
        """Run date-count accountability analysis."""
        config = self.product_config.get("accountability", {}).get("date_count", {})
        
        # Get expected count per day (default: 1)
        expected_per_day = config.get("expected_per_day", 1)
        
        # Query CMR for products
        ccid = self.product_config["ccid"].get(venue)
        if not ccid:
            raise ValueError(f"No CCID configured for {self.product} in {venue}")
        
        logger.info(f"Querying CMR for {self.product} from {start_date} to {end_date}")
        granules = query_cmr(ccid, start_date, end_date, venue)
        
        # Count granules by beginning date
        date_counts = defaultdict(int)
        for granule in granules:
            temporal = granule["umm"].get("TemporalExtent", {}).get("RangeDateTime", {})
            begin_dt = temporal.get("BeginningDateTime")
            if begin_dt:
                date_str = begin_dt.split("T")[0]
                date_counts[date_str] += 1
        
        # Ensure all dates in range are represented
        current = start_date.date()
        while current <= end_date.date():
            date_str = current.strftime("%Y-%m-%d")
            if date_str not in date_counts:
                date_counts[date_str] = 0
            current += timedelta(days=1)
        
        # Identify missing dates (dates with fewer than expected count)
        missing_dates = {
            date: count for date, count in date_counts.items() 
            if count < expected_per_day
        }
        
        # Calculate metrics
        total_dates = len(date_counts)
        missing_count = len(missing_dates)
        expected_total = total_dates * expected_per_day
        actual_total = sum(date_counts.values())
        
        return {
            "strategy": self.get_strategy_name(),
            "expected_per_day": expected_per_day,
            "total_dates": total_dates,
            "missing_dates": missing_count,
            "expected_total": expected_total,
            "actual_total": actual_total,
            "expected": expected_total,  # Standard format for reports
            "actual": actual_total,  # Standard format for reports
            "missing_count": sum(max(0, expected_per_day - count) for count in date_counts.values()),
            "missing": sorted(list(missing_dates.keys())),
            "date_counts": dict(date_counts),
        }
