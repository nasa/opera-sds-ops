"""Report generation in multiple formats (JSON, text, summary)."""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Any

from . import CONFIG

logger = logging.getLogger(__name__)


def save_reports(
    results: dict[str, Any],
    output_dir: str,
    product: str,
    report_type: str,
    venue: str = "PROD",
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> dict[str, Path]:
    """
    Save reports in multiple formats.

    Args:
        results: Results dict from duplicates.detect_duplicates() or strategies.dswx_hls.analyze_accountability()
        output_dir: Base output directory
        product: Product name
        report_type: 'duplicates' or 'accountability'
        venue: 'PROD' or 'UAT'

    Returns:
        Dict of file paths created
    """
    # Create output directory structure
    base_dir = Path(output_dir) / "reports" / report_type / product
    base_dir.mkdir(parents=True, exist_ok=True)

    # Use current date for filenames
    date_str = datetime.now().strftime("%Y-%m-%d")

    files_created = {}

    # 1. JSON format (full report)
    json_path = base_dir / f"{date_str}.json"
    report_metadata = {
        "generated_at": datetime.now().isoformat(),
        "product_type": product,
        "venue": venue,
        "report_type": report_type,
    }
    if start_date is not None:
        report_metadata["start_date"] = start_date.isoformat()
    if end_date is not None:
        report_metadata["end_date"] = end_date.isoformat()

    report_data = {
        "report_metadata": report_metadata,
        "results": results,
    }

    with open(json_path, "w") as f:
        json.dump(report_data, f, indent=2)
    logger.info(f"Saved JSON report: {json_path}")
    files_created["json"] = json_path

    # 2. Text format (DAAC format - list of granule IDs)
    if report_type == "duplicates" and "conflicts" in results:
        txt_path = base_dir / f"{date_str}_conflicts.txt"
        conflicts = results["conflicts"]
        total_conflicts = len(conflicts)
        conflict_progress = max(10_000, total_conflicts // 10)
        written = 0
        with open(txt_path, "w") as f:
            for idx, (conflict_key, conflict) in enumerate(conflicts.items()):
                f.write(f"# {conflict_key}\n")
                for product_id in conflict["products"]:
                    f.write(f"{product_id}\n")
                    written += 1
                if idx > 0 and idx % conflict_progress == 0:
                    logger.info(
                        "  ... writing conflict list: %d / %d groups (%d products)",
                        idx, total_conflicts, written,
                    )
        logger.info(f"Saved conflict list: {txt_path}")
        files_created["text"] = txt_path

    elif report_type == "duplicates" and "duplicate_list" in results:
        txt_path = base_dir / f"{date_str}.txt"
        duplicate_list = results["duplicate_list"]
        total_dups = len(duplicate_list)
        dup_progress = max(50_000, total_dups // 10)
        with open(txt_path, "w") as f:
            for idx, granule_id in enumerate(duplicate_list):
                f.write(f"{granule_id}\n")
                if idx > 0 and idx % dup_progress == 0:
                    logger.info(
                        "  ... writing duplicate list: %d / %d granule IDs",
                        idx, total_dups,
                    )
        logger.info(f"Saved text list: {txt_path}")
        files_created["text"] = txt_path

    elif report_type == "accountability" and "missing" in results:
        txt_path = base_dir / f"{date_str}_missing.txt"
        missing = results["missing"]
        total_missing = len(missing)
        missing_progress = max(50_000, total_missing // 10)
        with open(txt_path, "w") as f:
            for idx, granule_id in enumerate(missing):
                f.write(f"{granule_id}\n")
                if idx > 0 and idx % missing_progress == 0:
                    logger.info(
                        "  ... writing missing list: %d / %d granule IDs",
                        idx, total_missing,
                    )
        logger.info(f"Saved missing list: {txt_path}")
        files_created["text"] = txt_path

    # 3. Summary text (human-readable)
    summary_path = base_dir / f"{date_str}_summary.txt"
    with open(summary_path, "w") as f:
        f.write(f"OPERA {report_type.title()} Report\n")
        f.write("=" * 50 + "\n")
        f.write(f"Product:        {product}\n")
        f.write(f"Venue:          {venue}\n")
        f.write(f"Generated:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n")

        if report_type == "duplicates":
            f.write("SUMMARY\n")
            f.write("-" * 50 + "\n")
            if "conflict_groups" in results:
                f.write(f"Total Granules:         {results['total']:,}\n")
                f.write(f"Conflict Groups:        {results['conflict_groups']:,}\n")
                f.write(f"Conflicting Products:   {results['conflicting_products']:,}\n")
                if results["total"] > 0:
                    conflict_rate = (results["conflicting_products"] / results["total"]) * 100
                    f.write(f"Conflict Rate:          {conflict_rate:.2f}%\n")
            else:
                f.write(f"Total Granules:     {results['total']:,}\n")
                f.write(f"Unique Granules:    {results['unique']:,}\n")
                f.write(f"Duplicate Count:    {results['duplicates']:,}\n")
                if results["total"] > 0:
                    dup_rate = (results["duplicates"] / results["total"]) * 100
                    f.write(f"Duplicate Rate:     {dup_rate:.2f}%\n")

        elif report_type == "accountability":
            f.write("SUMMARY\n")
            f.write("-" * 50 + "\n")
            expected = results.get("expected")
            actual = results.get("actual")
            missing_count = results.get("missing_count")
            f.write(f"Expected Granules:  {expected:,}\n" if expected is not None else "Expected Granules:  N/A\n")
            f.write(f"Actual Granules:    {actual:,}\n" if actual is not None else "Actual Granules:    N/A\n")
            f.write(f"Missing Granules:   {missing_count:,}\n" if missing_count is not None else "Missing Granules:   N/A\n")
            if expected and expected > 0 and actual is not None:
                acc_rate = (actual / expected) * 100
                f.write(f"Accountability:     {acc_rate:.2f}%\n")

        f.write("\n")
        f.write("Files Generated:\n")
        f.write(f"- Full report:  {json_path}\n")
        if "text" in files_created:
            f.write(f"- List file:    {files_created['text']}\n")

    logger.info(f"Saved summary: {summary_path}")
    files_created["summary"] = summary_path

    return files_created
