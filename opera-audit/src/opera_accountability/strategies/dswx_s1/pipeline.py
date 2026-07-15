"""DSWx-S1 accountability pipeline orchestrator.

Runs the 5-step pipeline end-to-end (survey → mapping → tile sets → cycles →
real-coverage validation) and persists intermediates + a final summary under
``<output_dir>/reports/accountability/DSWX_S1/<YYYY-MM-DD>/``. Invoked by the
CLI ``opera-audit accountability DSWX_S1``.
"""

from __future__ import annotations

import json
import inspect
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from ... import CONFIG
from ...checkpoint import CheckpointStore
from . import coverage, cycles, mapping, survey, tile_sets
from .rtc_utils import has_known_epoch

logger = logging.getLogger(__name__)


def _validate_sensor_config() -> None:
    """Fail fast if ``sensor_start_dates`` references a sensor with no epoch.

    Without this check, the pipeline happily surveys + maps for the sensor,
    then raises ``NotImplementedError`` deep inside cycle expansion — after
    hours of CMR traffic and writing partial artifacts. Catching the mis-
    configuration up-front saves the operator from a painful rerun.
    """
    sensor_starts = (
        CONFIG["products"]["DSWX_S1"]["accountability"].get("sensor_start_dates") or {}
    )
    bad = [s for s in sensor_starts if not has_known_epoch(s)]
    if bad:
        raise ValueError(
            f"Sensor(s) {bad} are listed in "
            f"products.DSWX_S1.accountability.sensor_start_dates but have no "
            f"12-day cycle epoch defined in "
            f"opera_accountability.strategies.dswx_s1.rtc_utils._EPOCH_MAP. "
            f"Either remove the sensor from sensor_start_dates or add its "
            f"epoch to _EPOCH_MAP."
        )


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info("Wrote %s (%s)", path, _human_size(path.stat().st_size))


def _write_json_array(path: Path, values) -> None:
    """Stream an iterable as a JSON array without materializing it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write("[\n")
        first = True
        for value in values:
            if not first:
                f.write(",\n")
            f.write(json.dumps(value, separators=(",", ":")))
            first = False
        f.write("\n]\n")
    logger.info("Wrote %s (%s)", path, _human_size(path.stat().st_size))


def _write_rtc_map(path: Path, checkpoint: CheckpointStore) -> None:
    """Stream the RTC→DSWx mapping grouped by stable RTC tuple."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write("{\n")
        current_key = None
        current_values: list[str] = []
        first_group = True

        def flush() -> None:
            nonlocal first_group
            if current_key is None:
                return
            if not first_group:
                f.write(",\n")
            f.write(json.dumps(current_key))
            f.write(":")
            f.write(json.dumps(current_values, separators=(",", ":")))
            first_group = False

        for _, payload in checkpoint.iter_records("rtc_to_dswx_pairs"):
            rtc_key = payload["rtc_key"]
            if current_key is not None and rtc_key != current_key:
                flush()
                current_values = []
            current_key = rtc_key
            current_values.append(payload["dswx_id"])
        flush()
        f.write("\n}\n")
    logger.info("Wrote %s (%s)", path, _human_size(path.stat().st_size))


def _human_size(num_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f}{unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f}TB"


def _write_summary(path: Path, results: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write("OPERA DSWx-S1 Accountability Report\n")
        f.write("=" * 50 + "\n")
        for key, label in (
            ("venue", "Venue"),
            ("start_date", "Start date"),
            ("end_date", "End date"),
            ("generated_at", "Generated"),
        ):
            if key in results["metadata"]:
                f.write(f"{label:<22}{results['metadata'][key]}\n")
        f.write("\n")
        f.write("SURVEY\n")
        f.write("-" * 50 + "\n")
        f.write(f"RTC-S1 surveyed:      {results['rtc_surveyed']:,}\n")
        f.write(f"DSWx-S1 surveyed:     {results['dswx_surveyed']:,}\n")
        f.write("\n")
        f.write("MAPPING\n")
        f.write("-" * 50 + "\n")
        f.write(f"RTCs after sensor-date filter: {results['filtered_rtc_count']:,}\n")
        f.write(f"RTCs used in DSWx-S1:          {results['used_rtc_count']:,}\n")
        f.write(f"Missing RTCs:                  {results['missing_count']:,}\n")
        # Use actual / expected so the rate is bounded to [0, 100]. ``used``
        # can include RTCs outside the surveyed window (e.g. DSWx products
        # that reference older RTCs), which made the older ``used/filtered``
        # formula exceed 100% on window edges.
        if results["expected"]:
            pct = results["actual"] / results["expected"] * 100
            f.write(f"Accountability rate:           {pct:.2f}%\n")
        f.write("\n")
        f.write("TILE SETS\n")
        f.write("-" * 50 + "\n")
        f.write(f"MGRS tile sets affected:       {results['tile_set_count']:,}\n")
        f.write(f"Tile-set / cycle / sensor buckets: {results['cycle_bucket_count']:,}\n")
        f.write("\n")
        f.write("COVERAGE VALIDATION\n")
        f.write("-" * 50 + "\n")
        if results["coverage_validation_enabled"]:
            f.write(f"Required RTC coverage:          {results['coverage_threshold']:,}\n")
            f.write(f"Validated cycle buckets:        {results['coverage_valid_count']:,}\n")
            f.write(f"Dropped cycle buckets:          {results['coverage_dropped_count']:,}\n")
            f.write(f"Recovery RTC candidates:        {results['recovery_candidate_count']:,}\n")
        else:
            f.write("Disabled; recovery candidates are the raw missing RTC set.\n")
    logger.info("Wrote %s", path)


def run(
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    output_dir: str | Path,
    venue: str = "PROD",
    save: bool = True,
    mgrs_db_override: Optional[str] = None,
    validate_coverage: Optional[bool] = None,
    chunk_days: Optional[int] = 30,
    checkpoint_dir: Optional[str] = None,
    resume: bool = True,
    keep_checkpoints: bool = False,
) -> dict[str, Any]:
    """Execute the full DSWx-S1 accountability pipeline.

    Returns
    -------
    dict:
        Final accountability results plus a ``files`` dict listing the
        artifacts written (when ``save=True``).
    """
    # Fail fast on mis-configuration before any CMR traffic.
    _validate_sensor_config()

    pipeline_t0 = time.monotonic()
    logger.info(
        "=== DSWx-S1 accountability pipeline START (venue=%s, %s .. %s) ===",
        venue,
        start_date.date() if start_date else "open",
        end_date.date() if end_date else "open",
    )

    generated_at = datetime.now()
    date_str = generated_at.strftime("%Y-%m-%d")
    report_dir = Path(output_dir) / "reports" / "accountability" / "DSWX_S1" / date_str
    files: dict[str, Path] = {}

    checkpoint: Optional[CheckpointStore] = None
    if start_date is not None and end_date is not None:
        checkpoint = CheckpointStore(
            command="accountability",
            product="DSWX_S1",
            venue=venue,
            start=start_date,
            end=end_date,
            chunk_days=chunk_days,
            output_dir=output_dir,
            checkpoint_dir=checkpoint_dir,
            resume=resume,
            keep=keep_checkpoints,
            extra_identity={"coverage_validation": validate_coverage},
        )

    # --- Step 1: CMR survey -------------------------------------------------
    step_t0 = time.monotonic()
    logger.info("--- Step 1/5: CMR survey (RTC-S1 + DSWx-S1) ---")
    rtc_params = inspect.signature(survey.survey_rtc).parameters
    dswx_params = inspect.signature(survey.survey_dswx).parameters
    checkpoint_reducer = (
        checkpoint is not None
        and "materialize" in rtc_params
        and "materialize" in dswx_params
    )
    survey_kwargs = {
        "checkpoint": checkpoint,
        "chunk_days": chunk_days,
        "materialize": not checkpoint_reducer,
    }
    rtc_products = survey.survey_rtc(
        start_date,
        end_date,
        venue,
        **{key: value for key, value in survey_kwargs.items() if key in rtc_params},
    )
    dswx_products = survey.survey_dswx(
        start_date,
        end_date,
        venue,
        **{key: value for key, value in survey_kwargs.items() if key in dswx_params},
    )

    logger.info(
        "--- Step 1/5 complete (%.1fs) ---",
        time.monotonic() - step_t0,
    )

    # --- Step 2: RTC → DSWx mapping + missing RTC set ----------------------
    step_t0 = time.monotonic()
    logger.info("--- Step 2/5: RTC → DSWx-S1 mapping ---")
    if checkpoint_reducer:
        map_results = mapping.analyze_checkpoint(checkpoint)
        rtc_surveyed_count = map_results.pop("rtc_surveyed")
        dswx_surveyed_count = map_results.pop("dswx_surveyed")
    else:
        map_results = mapping.analyze(rtc_products, dswx_products)
        rtc_surveyed_count = len(rtc_products)
        dswx_surveyed_count = len(dswx_products)

    missing_rtcs: list[str] = map_results["missing"]
    logger.info(
        "Step 2 results: expected=%s, actual=%s, missing=%s",
        map_results["expected"],
        map_results["actual"],
        map_results["missing_count"],
    )
    logger.info(
        "--- Step 2/5 complete (%.1fs) ---",
        time.monotonic() - step_t0,
    )

    if save:
        if checkpoint_reducer:
            _write_json_array(
                report_dir / "rtc_survey.json",
                checkpoint.iter_reduced_payloads("rtc_unique"),
            )
            _write_json_array(
                report_dir / "dswx_survey.json",
                checkpoint.iter_reduced_payloads("dswx_unique"),
            )
        else:
            _write_json(report_dir / "rtc_survey.json", rtc_products)
            _write_json(report_dir / "dswx_survey.json", dswx_products)
        files["rtc_survey"] = report_dir / "rtc_survey.json"
        files["dswx_survey"] = report_dir / "dswx_survey.json"

        _write_json(
            report_dir / "missing_rtc_products.json",
            missing_rtcs,
        )
        if checkpoint_reducer:
            _write_rtc_map(report_dir / "rtc_to_dswx_map.json", checkpoint)
        else:
            _write_json(
                report_dir / "rtc_to_dswx_map.json",
                map_results["rtc_to_dswx_map"],
            )
        files["missing_rtc_products"] = report_dir / "missing_rtc_products.json"
        files["rtc_to_dswx_map"] = report_dir / "rtc_to_dswx_map.json"

    # The raw compact surveys and full RTC→DSWx mapping are not needed by
    # tile/cycle coverage validation. Release them before the next high-cardinality
    # stage instead of retaining every intermediate until function return.
    map_results.pop("rtc_to_dswx_map", None)
    if rtc_products is not None:
        del rtc_products
    if dswx_products is not None:
        del dswx_products

    # --- Steps 3 & 4: tile-set resolution + cycle/sensor expansion ---------
    step_t0 = time.monotonic()
    logger.info("--- Steps 3-4/5: tile-set resolution + cycle expansion ---")
    tile_set_map: dict[str, list[str]] = {}
    cycle_map: dict[str, list[str]] = {}
    db_path: Optional[Path] = None

    if missing_rtcs:
        db_path = tile_sets.resolve_mgrs_tile_db(mgrs_db_override)
        tile_set_map = tile_sets.map_missing_rtcs_to_tile_sets(missing_rtcs, db_path)
        cycle_map = cycles.expand_with_cycle_indices(tile_set_map)
    else:
        logger.info("No missing RTCs — skipping tile-set resolution and cycle expansion.")

    logger.info(
        "Steps 3-4 results: %d tile sets, %d cycle/sensor buckets",
        len(tile_set_map),
        len(cycle_map),
    )
    logger.info(
        "--- Steps 3-4/5 complete (%.1fs) ---",
        time.monotonic() - step_t0,
    )

    if save:
        _write_json(report_dir / "missing_rtcs_to_tile_sets.json", tile_set_map)
        _write_json(report_dir / "missing_mgrs_set_cycle_indices.json", cycle_map)
        files["missing_rtcs_to_tile_sets"] = report_dir / "missing_rtcs_to_tile_sets.json"
        files["missing_mgrs_set_cycle_indices"] = report_dir / "missing_mgrs_set_cycle_indices.json"

    # --- Step 5: validate real RTC burst coverage --------------------------
    step_t0 = time.monotonic()
    logger.info("--- Step 5/5: RTC burst coverage validation ---")
    coverage_cfg = (
        CONFIG["products"]["DSWX_S1"]["accountability"].get("coverage_validation")
        or {}
    )
    coverage_enabled = (
        bool(coverage_cfg.get("enabled", True))
        if validate_coverage is None
        else validate_coverage
    )
    coverage_results = coverage.empty_result()

    if coverage_enabled and cycle_map:
        if db_path is None:
            raise RuntimeError("MGRS tile DB was not resolved before coverage validation")
        coverage_results = coverage.validate_cycle_coverage(
            cycle_map,
            db_path,
            venue=venue,
        )
    elif coverage_enabled:
        logger.info("No cycle buckets — coverage validation has nothing to check.")
    else:
        logger.info("DSWx-S1 real-coverage validation is disabled.")

    logger.info(
        "--- Step 5/5 complete (%.1fs) ---",
        time.monotonic() - step_t0,
    )

    recovery_candidates = (
        sorted(coverage_results["reduced"])
        if coverage_enabled
        else sorted(set(missing_rtcs))
    )

    if save and coverage_enabled:
        coverage_report = {
            key: value
            for key, value in coverage_results.items()
            if key != "reduced"
        }
        _write_json(
            report_dir / "missing_mgrs_sets_by_coverage.json",
            coverage_report,
        )
        _write_json(
            report_dir / "missing_rtc_mgrs_set_mappings_with_sufficient_coverage_reduced.json",
            coverage_results["reduced"],
        )
        files["coverage_validation"] = report_dir / "missing_mgrs_sets_by_coverage.json"
        files["reduced_recovery_candidates"] = (
            report_dir / "missing_rtc_mgrs_set_mappings_with_sufficient_coverage_reduced.json"
        )

    # --- Final results payload --------------------------------------------
    # Reserve summary artifact paths up-front so the on-disk summary.json and
    # the returned ``results['files']`` agree on the full artifact list.
    if save:
        files["summary_json"] = report_dir / "summary.json"
        files["summary_txt"] = report_dir / "summary.txt"

    results = {
        "metadata": {
            "product": "DSWX_S1",
            "strategy": "dswx_s1",
            "venue": venue,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
            "generated_at": generated_at.isoformat(),
        },
        "rtc_surveyed": rtc_surveyed_count,
        "dswx_surveyed": dswx_surveyed_count,
        "filtered_rtc_count": map_results["filtered_rtc_count"],
        "used_rtc_count": map_results["used_rtc_count"],
        "missing_count": map_results["missing_count"],
        # opera-audit-wide accountability contract:
        "expected": map_results["expected"],
        "actual": map_results["actual"],
        "missing": map_results["missing"],
        "tile_set_count": len(tile_set_map),
        "cycle_bucket_count": len(cycle_map),
        "coverage_validation_enabled": coverage_enabled,
        "coverage_threshold": coverage_results["threshold"],
        "coverage_valid_count": coverage_results["valid_count"],
        "coverage_dropped_count": coverage_results["dropped_count"],
        "recovery_candidate_count": len(recovery_candidates),
        "recovery_candidates": recovery_candidates,
        "files": {k: str(v) for k, v in files.items()},
        "checkpoint": {
            "chunk_days": chunk_days,
            "resume": resume,
            "kept": keep_checkpoints,
            "path": str(checkpoint.path) if checkpoint and keep_checkpoints else None,
        },
    }

    if save:
        _write_json(report_dir / "summary.json", results)
        _write_summary(report_dir / "summary.txt", results)

    if checkpoint is not None:
        checkpoint.mark_successful()
        checkpoint.close()

    elapsed = time.monotonic() - pipeline_t0
    logger.info(
        "=== DSWx-S1 accountability pipeline DONE in %.1fs "
        "(expected=%s, actual=%s, missing=%s, recovery=%d) ===",
        elapsed,
        results["expected"],
        results["actual"],
        results["missing_count"],
        results["recovery_candidate_count"],
    )
    return results
