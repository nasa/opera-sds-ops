"""DSWx-S1 accountability pipeline orchestrator.

Runs the 4-step pipeline end-to-end (survey → mapping → tile sets → cycles)
and persists intermediates + a final summary under
``<output_dir>/reports/accountability/DSWX_S1/<YYYY-MM-DD>/``. Invoked by the
CLI ``opera-audit accountability DSWX_S1``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from ... import CONFIG
from . import survey, mapping, tile_sets, cycles
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
    logger.info("Wrote %s", path)


def run(
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    output_dir: str | Path,
    venue: str = "PROD",
    save: bool = True,
    mgrs_db_override: Optional[str] = None,
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

    generated_at = datetime.now()
    date_str = generated_at.strftime("%Y-%m-%d")
    report_dir = Path(output_dir) / "reports" / "accountability" / "DSWX_S1" / date_str
    files: dict[str, Path] = {}

    # --- Step 1: CMR survey -------------------------------------------------
    rtc_products = survey.survey_rtc(start_date, end_date, venue)
    dswx_products = survey.survey_dswx(start_date, end_date, venue)

    if save:
        _write_json(report_dir / "rtc_survey.json", rtc_products)
        _write_json(report_dir / "dswx_survey.json", dswx_products)
        files["rtc_survey"] = report_dir / "rtc_survey.json"
        files["dswx_survey"] = report_dir / "dswx_survey.json"

    # --- Step 2: RTC → DSWx mapping + missing RTC set ----------------------
    map_results = mapping.analyze(rtc_products, dswx_products)
    missing_rtcs: list[str] = map_results["missing"]

    if save:
        _write_json(
            report_dir / "missing_rtc_products.json",
            missing_rtcs,
        )
        _write_json(
            report_dir / "rtc_to_dswx_map.json",
            map_results["rtc_to_dswx_map"],
        )
        files["missing_rtc_products"] = report_dir / "missing_rtc_products.json"
        files["rtc_to_dswx_map"] = report_dir / "rtc_to_dswx_map.json"

    # --- Steps 3 & 4: tile-set resolution + cycle/sensor expansion ---------
    tile_set_map: dict[str, list[str]] = {}
    cycle_map: dict[str, list[str]] = {}

    if missing_rtcs:
        db_path = tile_sets.resolve_mgrs_tile_db(mgrs_db_override)
        tile_set_map = tile_sets.map_missing_rtcs_to_tile_sets(missing_rtcs, db_path)
        cycle_map = cycles.expand_with_cycle_indices(tile_set_map)
    else:
        logger.info("No missing RTCs — skipping tile-set resolution and cycle expansion.")

    if save:
        _write_json(report_dir / "missing_rtcs_to_tile_sets.json", tile_set_map)
        _write_json(report_dir / "missing_mgrs_set_cycle_indices.json", cycle_map)
        files["missing_rtcs_to_tile_sets"] = report_dir / "missing_rtcs_to_tile_sets.json"
        files["missing_mgrs_set_cycle_indices"] = report_dir / "missing_mgrs_set_cycle_indices.json"

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
        "rtc_surveyed": len(rtc_products),
        "dswx_surveyed": len(dswx_products),
        "filtered_rtc_count": map_results["filtered_rtc_count"],
        "used_rtc_count": map_results["used_rtc_count"],
        "missing_count": map_results["missing_count"],
        # opera-audit-wide accountability contract:
        "expected": map_results["expected"],
        "actual": map_results["actual"],
        "missing": map_results["missing"],
        "tile_set_count": len(tile_set_map),
        "cycle_bucket_count": len(cycle_map),
        "files": {k: str(v) for k, v in files.items()},
    }

    if save:
        _write_json(report_dir / "summary.json", results)
        _write_summary(report_dir / "summary.txt", results)

    return results
