from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from ... import CONFIG
from ...burst_db import load_dist_s1_bursts_to_products
from . import accountability, survey

logger = logging.getLogger(__name__)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def _write_lines(path: Path, values: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for value in values:
            f.write(f"{value}\n")


def _write_summary(path: Path, results: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write("OPERA DIST-S1 Accountability Report\n")
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
        f.write("SUMMARY\n")
        f.write("-" * 50 + "\n")
        f.write(f"RTC-S1 surveyed:       {results['rtc_surveyed']:,}\n")
        f.write(f"DIST-S1 surveyed:      {results['dist_surveyed']:,}\n")
        f.write(f"RTCs used in DIST-S1:  {results['used_rtc_count']:,}\n")
        f.write(f"Missing RTCs:          {results['missing_count']:,}\n")
        if results["expected"]:
            rate = results["actual"] / results["expected"] * 100
            f.write(f"Accountability rate:   {rate:.2f}%\n")
        f.write("\n")
        f.write("BURST DB\n")
        f.write("-" * 50 + "\n")
        f.write(f"Enabled:               {results['burst_db_enabled']}\n")
        f.write(f"Product groups:        {results['missing_product_group_count']:,}\n")
        f.write(f"Missing product times: {results['missing_dist_product_count']:,}\n")


def run(
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    output_dir: str | Path,
    venue: str = "PROD",
    save: bool = True,
    burst_db: Optional[str] = None,
    max_concurrent: Optional[int] = None,
    max_retries: Optional[int] = None,
    prefer_s3: Optional[bool] = None,
) -> dict[str, Any]:
    cfg = CONFIG["products"]["DIST_S1"]["accountability"]
    if max_concurrent is None:
        max_concurrent = cfg.get("max_concurrent_iso_downloads", 10)
    if max_retries is None:
        max_retries = cfg.get("max_iso_retries", 3)
    if prefer_s3 is None:
        prefer_s3 = cfg.get("prefer_s3_iso_xml", False)

    generated_at = datetime.now()
    date_str = generated_at.strftime("%Y-%m-%d")
    report_dir = Path(output_dir) / "reports" / "accountability" / "DIST_S1" / date_str
    files: dict[str, Path] = {}

    rtc_products = survey.survey_rtc(start_date, end_date, venue)
    dist_products, existing_tile_times = survey.survey_dist(
        start_date,
        end_date,
        venue,
        max_concurrent=max_concurrent,
        max_retries=max_retries,
        prefer_s3=prefer_s3,
    )

    bursts_to_products = load_dist_s1_bursts_to_products(burst_db)
    results = accountability.analyze(
        rtc_products,
        dist_products,
        existing_tile_times,
        bursts_to_products=bursts_to_products,
    )

    if save:
        artifacts = {
            "rtc_survey": ("rtc_survey.json", rtc_products),
            "dist_survey": ("dist_survey.json", dist_products),
            "existing_tile_times": ("existing_tile_times.json", sorted(existing_tile_times)),
            "missing_rtc_products": ("missing_rtc_products.json", results["missing"]),
            "rtc_to_dist_map": ("rtc_to_dist_map.json", results["rtc_to_dist_map"]),
            "missing_rtcs_to_product_groups": (
                "missing_rtcs_to_product_groups.json",
                results["missing_rtcs_to_product_groups"],
            ),
            "missing_dist_products": (
                "missing_dist_products.json",
                results["missing_dist_products"],
            ),
            "missing_dist_product_rows": (
                "missing_dist_product_rows.json",
                results["missing_dist_product_rows"],
            ),
        }
        for key, (filename, data) in artifacts.items():
            path = report_dir / filename
            _write_json(path, data)
            files[key] = path
        _write_lines(report_dir / "missing_rtc_products.txt", results["missing"])
        files["missing_rtc_products_txt"] = report_dir / "missing_rtc_products.txt"
        _write_lines(report_dir / "missing_dist_products.txt", results["missing_dist_products"])
        files["missing_dist_products_txt"] = report_dir / "missing_dist_products.txt"
        files["summary_json"] = report_dir / "summary.json"
        files["summary_txt"] = report_dir / "summary.txt"

    results = {
        "metadata": {
            "product": "DIST_S1",
            "strategy": "dist_s1",
            "venue": venue,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
            "generated_at": generated_at.isoformat(),
        },
        **results,
        "files": {key: str(value) for key, value in files.items()},
    }

    if save:
        _write_json(report_dir / "summary.json", results)
        _write_summary(report_dir / "summary.txt", results)

    return results
