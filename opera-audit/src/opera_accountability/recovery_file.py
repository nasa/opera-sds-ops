"""Recovery-file output format (port from Chris's tools for daac_data_subscriber.py)."""

import json
from pathlib import Path
from typing import Any, Optional


def write_recovery_file(
    results: dict[str, Any],
    output_path: str,
    format: str = "txt"
) -> str:
    """
    Write accountability results in recovery-file format for daac_data_subscriber.py.
    
    The recovery file format is a simple text file with one granule ID per line,
    suitable for input to the daac_data_subscriber tool for reprocessing.
    
    Args:
        results: Accountability results dict with 'missing' list
        output_path: Output file path
        format: Output format ('txt' or 'json')
        
    Returns:
        Path to the written file
    """
    output_file = Path(output_path)
    
    missing_ids = results.get("missing", [])
    
    if format == "txt":
        txt_file = output_file.with_suffix(".txt")
        with txt_file.open("w") as f:
            for granule_id in missing_ids:
                f.write(f"{granule_id}\n")
        return str(txt_file)
    
    elif format == "json":
        json_file = output_file.with_suffix(".json")
        with json_file.open("w") as f:
            json.dump({
                "missing_count": len(missing_ids),
                "missing_ids": missing_ids,
                "strategy": results.get("strategy"),
                "product": results.get("product"),
            }, f, indent=2)
        return str(json_file)
    
    else:
        raise ValueError(f"Unsupported format: {format}")


def write_recovery_files_by_date(
    results: dict[str, Any],
    output_dir: str,
    format: str = "txt"
) -> list[str]:
    """
    Write recovery files grouped by date (for date-count strategy results).
    
    Args:
        results: Accountability results dict with 'date_counts' or 'missing' by date
        output_dir: Output directory path
        format: Output format ('txt' or 'json')
        
    Returns:
        List of paths to written files
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    files_written = []
    
    # If results have date_counts (date-count strategy)
    if "date_counts" in results:
        date_counts = results["date_counts"]
        expected_per_day = results.get("expected_per_day", 1)
        
        for date_str, count in date_counts.items():
            if count < expected_per_day:
                # This date is missing products
                date_file = output_path / f"missing_{date_str}.{format}"
                
                if format == "txt":
                    with date_file.open("w") as f:
                        f.write(f"# Missing products for {date_str}\n")
                        f.write(f"# Expected: {expected_per_day}, Actual: {count}\n")
                        f.write(f"# Missing: {expected_per_day - count}\n")
                        # In a full implementation, would list specific missing IDs
                        f.write(f"# Date: {date_str}\n")
                elif format == "json":
                    with date_file.open("w") as f:
                        json.dump({
                            "date": date_str,
                            "expected": expected_per_day,
                            "actual": count,
                            "missing": expected_per_day - count
                        }, f, indent=2)
                
                files_written.append(str(date_file))
    
    # If results have missing list (other strategies)
    elif "missing" in results:
        file_path = output_path / f"recovery.{format}"
        written_path = write_recovery_file(results, str(file_path), format)
        files_written.append(written_path)
    
    return files_written
