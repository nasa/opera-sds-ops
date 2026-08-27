"""Forward-map accountability strategy (ported from Chris's cmr_audit_hls.py, cmr_audit_slc.py)."""

import re
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Optional
from collections import defaultdict

from .base import AccountabilityStrategy
from .. import CONFIG
from ..checkpoint import CheckpointStore, collect_chunked_records, generate_time_chunks

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
        self.product_config = CONFIG["products"][product]
    
    def get_strategy_name(self) -> str:
        return "forward_map"
    
    def analyze(
        self,
        start_date: datetime,
        end_date: datetime,
        venue: str = "PROD",
        **kwargs
    ) -> dict[str, Any]:
        """Run forward-map accountability analysis.
        
        Exact port of Chris's algorithm (cmr_audit_hls.py:211-248):
        1. Query CMR for input products (e.g., HLS L30/S30)
        2. Generate expected output patterns from inputs (e.g., DSWx native ID patterns)
        3. Query CMR for actual outputs
        4. Find missing outputs (inputs that should have been processed but weren't)
        """
        t0 = time.monotonic()
        logger.info(
            "=== Forward-map accountability START for %s (venue=%s, %s .. %s) ===",
            self.product, venue, start_date.date(), end_date.date(),
        )

        input_config = self.product_config.get("accountability", {}).get("forward_map", {})
        
        if not input_config:
            raise ValueError(f"No forward_map configuration found for {self.product}")
        
        # Get input collection info
        input_ccid = input_config.get("input_ccid", {}).get(venue)
        if not input_ccid:
            raise ValueError(f"No input CCID configured for {self.product} in {venue}")
        
        # Get output collection info
        output_ccid = self.product_config["ccid"].get(venue)
        if not output_ccid:
            raise ValueError(f"No output CCID configured for {self.product} in {venue}")
        
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

        # Step 1: Query CMR for INPUT products (Chris's line 217-218, 220-222)
        logger.info("[Step 1/5] Querying CMR for input products (ccid=%s)", input_ccid)
        chunks = list(generate_time_chunks(start_date, end_date, chunk_days))
        collect_chunked_records(
            store=checkpoint,
            namespace="inputs",
            chunks=chunks,
            query=lambda chunk_start, chunk_end: self._query_cmr(
                input_ccid, chunk_start, chunk_end, venue
            ),
            project=lambda granule: (
                granule["umm"]["GranuleUR"], granule["umm"]["GranuleUR"]
            ),
        )
        input_ids = set(checkpoint.iter_payloads("inputs"))
        logger.info(f"Expected input (granules): {len(input_ids):,}")
        
        # Step 2: Generate expected output patterns from inputs (Chris's line 224-228)
        logger.info("[Step 2/5] Generating expected output patterns from %d inputs", len(input_ids))
        input_to_outputs_map = defaultdict(set)
        output_to_inputs_map = defaultdict(set)
        expected_output_patterns = self._generate_output_patterns_from_inputs(
            input_ids, input_to_outputs_map, output_to_inputs_map
        )
        
        # Step 3: Query CMR for actual OUTPUT products (Chris's line 231)
        logger.info("[Step 3/5] Querying CMR for %s output products (ccid=%s)", self.product, output_ccid)
        collect_chunked_records(
            store=checkpoint,
            namespace="outputs",
            chunks=chunks,
            query=lambda chunk_start, chunk_end: self._query_cmr(
                output_ccid, chunk_start, chunk_end, venue
            ),
            project=lambda granule: (
                granule["umm"]["GranuleUR"], granule["umm"]["GranuleUR"]
            ),
        )
        actual_output_ids = set(checkpoint.iter_payloads("outputs"))
        
        logger.info("Found %d actual output IDs from CMR", len(actual_output_ids))

        # Step 4: Extract output prefixes (Chris's line 233-235)
        logger.info("[Step 4/5] Comparing expected vs. actual output prefixes")
        expected_output_prefixes = {pattern.rstrip("*") for pattern in expected_output_patterns}
        actual_output_prefixes = self._extract_output_prefixes(actual_output_ids)
        missing_output_prefixes = expected_output_prefixes - actual_output_prefixes
        
        # Step 5: Map back to missing inputs (Chris's line 242-243)
        logger.info("[Step 5/5] Mapping missing output prefixes back to input granules")
        missing_input_sets = [output_to_inputs_map[prefix] for prefix in missing_output_prefixes]
        missing_inputs = set()
        if missing_input_sets:
            import functools
            missing_inputs = functools.reduce(set.union, missing_input_sets)
        
        # Calculate accountability metrics (Chris's line 246-248)
        # All counts are in the OUTPUT-prefix domain for consistency:
        #   expected = output prefixes we should see (derived from inputs)
        #   actual   = output prefixes we found in CMR
        #   missing  = expected - actual (output prefixes)
        expected_output_count = len(expected_output_prefixes)
        actual_output_count = len(actual_output_prefixes)
        missing_output_count = len(missing_output_prefixes)
        
        elapsed = time.monotonic() - t0
        logger.info("Expected output prefixes: %s", f"{expected_output_count:,}")
        logger.info("Actual output prefixes: %s", f"{actual_output_count:,}")
        logger.info("Missing output prefixes: %s", f"{missing_output_count:,}")
        logger.info("Missing input granules: %s", f"{len(missing_inputs):,}")
        logger.info(
            "=== Forward-map accountability DONE for %s in %.1fs ===",
            self.product, elapsed,
        )
        
        results = {
            "strategy": self.get_strategy_name(),
            "expected": expected_output_count,
            "actual": actual_output_count,
            "missing_count": missing_output_count,
            "missing": sorted(list(missing_output_prefixes)),
            "input_surveyed": checkpoint.count_records("inputs"),
            "output_surveyed": checkpoint.count_records("outputs"),
            "missing_input_granules": sorted(list(missing_inputs)),
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
        forward_map_config = self.product_config.get("accountability", {}).get("forward_map", {})
        input_product_type = forward_map_config.get("input_product_type", "HLS")
        
        if input_product_type == "HLS":
            return self._hls_to_dswx_patterns(input_ids, input_to_outputs_map, output_to_inputs_map)
        elif input_product_type == "SLC":
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
        total_hls = len(hls_ids)
        hls_progress = max(100_000, total_hls // 10)
        
        for idx, granule in enumerate(hls_ids):
            if idx > 0 and idx % hls_progress == 0:
                logger.info(
                    "  ... pattern generation: %d / %d HLS IDs (%d patterns so far)",
                    idx, total_hls, len(dswx_native_id_patterns),
                )
            # HLS pattern (Chris's line 143-149)
            m = re.match(
                r"(?P<product_shortname>HLS[.]([LS])30)[.]"
                r"(?P<tile_id>T[^\W_]{5})[.]"
                r"(?P<acquisition_ts>(?P<year>\d{4})(?P<day_of_year>\d{3})T(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2}))[.]"
                r"(?P<collection_version>v\d+[.]\d+)$",
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
            dswx_native_id_pattern = f"OPERA_L3_DSWx-HLS_{tile}_{dswx_acquisition_dt_str}Z_*"
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
            r"(?P<project>OPERA)_"
            r"(?P<level>L3)_"
            r"(?P<product_type>DSWx)-(?P<source>HLS)_"
            r"(?P<tile_id>T[^\W_]{5})_"
            r"(?P<acquisition_ts>(?P<acq_year>\d{4})(?P<acq_month>\d{2})(?P<acq_day>\d{2})T(?P<acq_hour>\d{2})(?P<acq_minute>\d{2})(?P<acq_second>\d{2})Z)_"
        )
        prefixes = set()
        total_outputs = len(output_ids)
        prefix_progress = max(100_000, total_outputs // 10)
        for idx, prefix in enumerate(output_ids):
            if idx > 0 and idx % prefix_progress == 0:
                logger.info(
                    "  ... prefix extraction: %d / %d output IDs",
                    idx, total_outputs,
                )
            m = re.match(dswx_regex_pattern, prefix)
            if m:
                prefixes.add(m.group(0))
        return prefixes
