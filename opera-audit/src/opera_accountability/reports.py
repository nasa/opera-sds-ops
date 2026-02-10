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
    venue: str = 'PROD'
) -> dict[str, Path]:
    """
    Save reports in multiple formats.

    Args:
        results: Results dict from duplicates.detect_duplicates() or accountability.analyze_accountability()
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
    date_str = datetime.now().strftime('%Y-%m-%d')

    files_created = {}

    # 1. JSON format (full report)
    json_path = base_dir / f"{date_str}.json"
    report_data = {
        "report_metadata": {
            "generated_at": datetime.now().isoformat(),
            "product_type": product,
            "venue": venue,
            "report_type": report_type
        },
        "results": results
    }

    with open(json_path, 'w') as f:
        json.dump(report_data, f, indent=2)
    logger.info(f"Saved JSON report: {json_path}")
    files_created['json'] = json_path

    # 2. Text format (DAAC format - list of granule IDs)
    if report_type == 'duplicates' and 'duplicate_list' in results:
        txt_path = base_dir / f"{date_str}.txt"
        with open(txt_path, 'w') as f:
            for granule_id in results['duplicate_list']:
                f.write(f"{granule_id}\n")
        logger.info(f"Saved text list: {txt_path}")
        files_created['text'] = txt_path

    elif report_type == 'accountability' and 'missing' in results:
        txt_path = base_dir / f"{date_str}_missing.txt"
        with open(txt_path, 'w') as f:
            for granule_id in results['missing']:
                f.write(f"{granule_id}\n")
        logger.info(f"Saved missing list: {txt_path}")
        files_created['text'] = txt_path

    # 3. Summary text (human-readable)
    summary_path = base_dir / f"{date_str}_summary.txt"
    with open(summary_path, 'w') as f:
        f.write(f"OPERA {report_type.title()} Report\n")
        f.write("=" * 50 + "\n")
        f.write(f"Product:        {product}\n")
        f.write(f"Venue:          {venue}\n")
        f.write(f"Generated:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n")

        if report_type == 'duplicates':
            f.write("SUMMARY\n")
            f.write("-" * 50 + "\n")
            f.write(f"Total Granules:     {results['total']:,}\n")
            f.write(f"Unique Granules:    {results['unique']:,}\n")
            f.write(f"Duplicate Count:    {results['duplicates']:,}\n")
            if results['total'] > 0:
                dup_rate = (results['duplicates'] / results['total']) * 100
                f.write(f"Duplicate Rate:     {dup_rate:.2f}%\n")

        elif report_type == 'accountability':
            f.write("SUMMARY\n")
            f.write("-" * 50 + "\n")
            f.write(f"Expected Granules:  {results['expected']:,}\n")
            f.write(f"Actual Granules:    {results['actual']:,}\n")
            f.write(f"Missing Granules:   {results['missing_count']:,}\n")
            if results['expected'] > 0:
                acc_rate = (results['actual'] / results['expected']) * 100
                f.write(f"Accountability:     {acc_rate:.2f}%\n")

        f.write("\n")
        f.write("Files Generated:\n")
        f.write(f"- Full report:  {json_path}\n")
        if 'text' in files_created:
            f.write(f"- List file:    {files_created['text']}\n")

    logger.info(f"Saved summary: {summary_path}")
    files_created['summary'] = summary_path

    return files_created
