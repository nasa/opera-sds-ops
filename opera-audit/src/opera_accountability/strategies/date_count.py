"""Date-count accountability strategy (ported from Chris's cmr_audit_tropo.py)."""

import logging
import time
from datetime import datetime, timedelta
from typing import Any
from collections import defaultdict

from .base import AccountabilityStrategy
from .. import CONFIG
from ..checkpoint import CheckpointStore, collect_chunked_records, generate_time_chunks
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
        **kwargs,
    ) -> dict[str, Any]:
        """Run date-count accountability analysis."""
        t0 = time.monotonic()
        logger.info(
            "=== Date-count accountability START for %s (venue=%s, %s .. %s) ===",
            self.product, venue, start_date.date(), end_date.date(),
        )

        config = self.product_config.get("accountability", {}).get("date_count", {})
        
        # Get expected count per day (default: 1)
        expected_per_day = config.get("expected_per_day", 1)
        
        # Query CMR for products
        ccid = self.product_config["ccid"].get(venue)
        if not ccid:
            raise ValueError(f"No CCID configured for {self.product} in {venue}")
        
        chunk_days = kwargs.get("chunk_days", 30)
        checkpoint = CheckpointStore(
            command="accountability",
            product=self.product,
            venue=venue,
            start=start_date,
            end=end_date,
            chunk_days=chunk_days,
            output_dir=kwargs.get("output_dir", "./output"),
            checkpoint_dir=kwargs.get("checkpoint_dir"),
            resume=kwargs.get("resume", True),
            keep=kwargs.get("keep_checkpoints", False),
        )

        logger.info(
            "[Step 1/3] Querying CMR for %s (ccid=%s, expected_per_day=%d)",
            self.product, ccid, expected_per_day,
        )

        def project(granule: dict):
            granule_id = granule["umm"]["GranuleUR"]
            temporal = granule["umm"].get("TemporalExtent", {}).get("RangeDateTime", {})
            return granule_id, {
                "id": granule_id,
                "begin": temporal.get("BeginningDateTime"),
            }

        collect_chunked_records(
            store=checkpoint,
            namespace="products",
            chunks=generate_time_chunks(start_date, end_date, chunk_days),
            query=lambda chunk_start, chunk_end: query_cmr(
                ccid, chunk_start, chunk_end, venue
            ),
            project=project,
        )
        
        # Count only beginning dates owned by this half-open run window. CMR
        logger.info("[Step 2/3] Counting granules by beginning date")
        # temporal search uses interval intersection, so it may return a
        # granule that begins before ``start_date`` or exactly at ``end_date``.
        # A same-day programmatic range still represents that one calendar day.
        start_day = start_date.date()
        end_day_exclusive = end_date.date()
        if end_day_exclusive <= start_day:
            end_day_exclusive = start_day + timedelta(days=1)

        date_counts = defaultdict(int)
        counted = 0
        for granule in checkpoint.iter_payloads("products"):
            counted += 1
            if counted % 100_000 == 0:
                logger.info(
                    "  ... date counting: %d granules processed (%d unique dates so far)",
                    counted, len(date_counts),
                )
            begin_dt = granule.get("begin")
            if begin_dt:
                begin_day = datetime.fromisoformat(
                    begin_dt.replace("Z", "+00:00")
                ).date()
                if start_day <= begin_day < end_day_exclusive:
                    date_counts[begin_day.strftime("%Y-%m-%d")] += 1
        logger.info("Date counting complete: %d granules, %d unique dates", counted, len(date_counts))
        
        # Ensure all dates in range are represented
        current = start_day
        while current < end_day_exclusive:
            date_str = current.strftime("%Y-%m-%d")
            if date_str not in date_counts:
                date_counts[date_str] = 0
            current += timedelta(days=1)
        
        logger.info("[Step 3/3] Identifying missing dates (threshold=%d per day)", expected_per_day)

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
        
        elapsed = time.monotonic() - t0
        logger.info(
            "=== Date-count accountability DONE for %s in %.1fs "
            "(total_dates=%d, missing_dates=%d, expected_total=%d, actual_total=%d) ===",
            self.product, elapsed, total_dates, missing_count, expected_total, actual_total,
        )

        results = {
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
            "checkpoint": {
                "chunk_days": chunk_days,
                "path": str(checkpoint.path)
                if kwargs.get("keep_checkpoints", False)
                else None,
            },
        }
        checkpoint.mark_successful()
        checkpoint.close()
        return results
