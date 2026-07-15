"""Delegated-validator accountability strategy (from Chris's cmr_audit_disp_s1.py)."""

import logging
import sys
import time
from datetime import datetime
from typing import Any
from pathlib import Path

from .base import AccountabilityStrategy
from .. import CONFIG
from ..checkpoint import CheckpointStore, collect_chunked_records, generate_time_chunks
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
        self.product_config = CONFIG["products"][product]
    
    def get_strategy_name(self) -> str:
        return "delegated_validator"
    
    def analyze(
        self,
        start_date: datetime,
        end_date: datetime,
        venue: str = "PROD",
        **kwargs
    ) -> dict[str, Any]:
        """Run delegated-validator accountability analysis."""
        t0 = time.monotonic()
        logger.info(
            "=== Delegated-validator accountability START for %s (venue=%s, %s .. %s) ===",
            self.product, venue, start_date.date(), end_date.date(),
        )

        config = self.product_config.get("accountability", {}).get("delegated_validator", {})
        
        # Get validator module path (optional - if not configured, skip delegation)
        validator_module = config.get("validator_module")
        validator_function = config.get("validator_function")
        validator_path = config.get("validator_path")
        
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
            "[Step 1/2] Querying CMR for %s (ccid=%s)",
            self.product, ccid,
        )
        collect_chunked_records(
            store=checkpoint,
            namespace="products",
            chunks=generate_time_chunks(start_date, end_date, chunk_days),
            query=lambda chunk_start, chunk_end: query_cmr(
                ccid, chunk_start, chunk_end, venue
            ),
            project=lambda granule: (
                granule["umm"]["GranuleUR"],
                {"umm": {"GranuleUR": granule["umm"]["GranuleUR"]}},
            ),
        )
        granules = list(checkpoint.iter_payloads("products"))
        logger.info("Fetched %d %s granules from CMR", len(granules), self.product)
        
        # If validator is configured, delegate to it
        if validator_module and validator_function:
            try:
                # If a path is specified, add it to sys.path temporarily
                if validator_path:
                    validator_path = Path(validator_path).resolve()
                    if validator_path.exists() and str(validator_path) not in sys.path:
                        sys.path.insert(0, str(validator_path))
                        logger.info(f"Added {validator_path} to Python path")
                
                # Dynamically import and call the validator
                module = __import__(validator_module, fromlist=[validator_function])
                validator_func = getattr(module, validator_function)
                
                logger.info(
                    "[Step 2/2] Delegating validation to %s.%s",
                    validator_module, validator_function,
                )
                
                # Pass validator-specific parameters from kwargs
                processing_mode = kwargs.get("processing_mode", "forward")
                k = kwargs.get("k", 15)
                frames_only = kwargs.get("frames_only")
                
                validation_results = validator_func(
                    start_date, end_date, 
                    "TEMPORAL", "OPS", "OPS",
                    frames_only, False, processing_mode, k
                )
                
                # Extract accountability metrics from validation results
                results = self._extract_accountability_metrics(validation_results, granules)
                return self._finish_checkpoint(results, checkpoint, chunk_days, kwargs)
            except ImportError as e:
                logger.warning(f"Could not import validator {validator_module}: {e}")
                logger.info("Falling back to basic accountability analysis")
                results = self._basic_analysis(granules)
                return self._finish_checkpoint(results, checkpoint, chunk_days, kwargs)
            except Exception as e:
                logger.error(f"Validator failed: {e}", exc_info=True)
                raise RuntimeError(
                    f"External validator {validator_module}.{validator_function} "
                    f"raised an error: {e}. Fix the validator or remove "
                    f"validator_module/validator_function from config.yaml to "
                    f"use basic (unvalidated) mode."
                ) from e
        else:
            logger.info("[Step 2/2] No validator configured, performing basic analysis")
            results = self._basic_analysis(granules)
            elapsed = time.monotonic() - t0
            logger.info(
                "=== Delegated-validator accountability DONE for %s in %.1fs (actual=%d) ===",
                self.product, elapsed, results.get("actual", 0),
            )
            return self._finish_checkpoint(results, checkpoint, chunk_days, kwargs)

    @staticmethod
    def _finish_checkpoint(
        results: dict[str, Any],
        checkpoint: CheckpointStore,
        chunk_days: int,
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        results["checkpoint"] = {
            "chunk_days": chunk_days,
            "path": str(checkpoint.path)
            if kwargs.get("keep_checkpoints", False)
            else None,
        }
        checkpoint.mark_successful()
        checkpoint.close()
        return results
    
    def _extract_accountability_metrics(self, validation_results: Any, granules: list[dict]) -> dict[str, Any]:
        """Extract accountability metrics from validator results."""
        # validation_results from opv_disp_s1.validate_disp_s1 returns:
        # (passing: bool, should_df: DataFrame, result_df: DataFrame)
        
        if isinstance(validation_results, tuple) and len(validation_results) == 3:
            passing, should_df, result_df = validation_results
            
            # Extract metrics from DataFrames
            import pandas as pd
            
            if isinstance(should_df, pd.DataFrame) and isinstance(result_df, pd.DataFrame):
                expected_count = len(should_df)
                actual_count = len(result_df)
                missing_count = expected_count - actual_count
                
                # Get missing frame IDs if available
                missing = []
                if "frame_id" in should_df.columns and "frame_id" in result_df.columns:
                    expected_frames = set(should_df["frame_id"].astype(str))
                    actual_frames = set(result_df["frame_id"].astype(str))
                    missing = sorted(list(expected_frames - actual_frames))
                
                return {
                    "strategy": self.get_strategy_name(),
                    "delegated": True,
                    "passing": passing,
                    "expected": expected_count,
                    "actual": actual_count,
                    "missing_count": missing_count,
                    "missing": missing,
                    "total_surveyed": len(granules),
                    "validator_version": "opv_disp_s1.validate_disp_s1"
                }
        
        # Fallback for other result formats
        if isinstance(validation_results, dict):
            return {
                "strategy": self.get_strategy_name(),
                "delegated": True,
                **validation_results
            }
        else:
            return {
                "strategy": self.get_strategy_name(),
                "delegated": True,
                "validation_results": str(validation_results),
                "total_surveyed": len(granules)
            }
    
    def _basic_analysis(self, granules: list[dict]) -> dict[str, Any]:
        """Perform basic accountability analysis without external validator.

        Without a validator, we can only count what CMR has — we cannot determine
        what *should* exist. The result is therefore marked ``validated=False``
        and ``expected``/``missing_count`` are explicitly set to ``None`` to
        prevent downstream consumers from interpreting this as a passing audit.
        """
        total = len(granules)
        
        return {
            "strategy": self.get_strategy_name(),
            "delegated": False,
            "validated": False,
            "expected": None,
            "actual": total,
            "missing_count": None,
            "missing": [],
            "total_surveyed": total,
            "note": (
                "No external validator configured. Only granule count is available. "
                "Configure validator_module/validator_function in config.yaml for "
                "full accountability analysis."
            ),
        }
