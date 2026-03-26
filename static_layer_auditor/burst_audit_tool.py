import argparse
import logging
import os
import sys

from dataclasses import dataclass

from cmr_auditor import run_audit
from burst_analysis import run_analysis
from burst_to_input_safe import burst_to_input_safe


def setup_logger(name: str = "burst-audit-tool", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("[%(levelname)s] %(asctime)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


logger = setup_logger("burst-audit-tool")


@dataclass
class CollectionInfo:
    static_collection_id: str
    static_native_id_pattern: str
    baseline_collection_id: str
    baseline_native_id_pattern: str


COLLECTIONS = {
    "RTC": CollectionInfo(
        static_collection_id="C2795135174-ASF",
        static_native_id_pattern="OPERA_L2_RTC-S1-STATIC",
        baseline_collection_id="C2777436413-ASF",
        baseline_native_id_pattern="OPERA_L2_RTC-S1",
    ),
    "CSLC": CollectionInfo(
        static_collection_id="C2795135668-ASF",
        static_native_id_pattern="OPERA_L2_CSLC-S1-STATIC_V1",
        baseline_collection_id="C2777443834-ASF",
        baseline_native_id_pattern="OPERA_L2_CSLC-S1",
    ),
}


def main():
    description = """
    Audit OPERA Sentinel-1 burst coverage and identify missing static layers.

    Performs three steps:
      1. audit: Query CMR to cache burst IDs from static and baseline collections
      2. analysis: Identify bursts missing static layers
      3. safe: Find input SAFE files for reprocessing (with S1C calibration filtering)
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--collection", required=True, choices=COLLECTIONS.keys())
    parser.add_argument(
        "--steps",
        nargs="+",
        choices=["audit", "analysis", "safe"],
        default=["audit", "analysis", "safe"],
        help="Which steps to perform: 'audit', 'analysis', 'safe' or all (default).",
    )
    parser.add_argument(
        "--layers",
        nargs="+",
        choices=["static", "baseline"],
        default=["static", "baseline"],
        help="Which layers to audit: 'static', 'baseline', or both (default).",
    )
    parser.add_argument(
        "--geo-filter",
        default=None,
        help="Use 'bbox:minx,miny,maxx,maxy' or path to a GeoJSON file. Only applies to analysis.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging for detailed output.",
    )
    parser.add_argument(
        "--cmr-workers",
        type=int,
        default=5,
        help="Number of parallel workers for CMR audit queries (default: 5). Lower if hitting rate limits.",
    )
    parser.add_argument(
        "--asf-workers",
        type=int,
        default=8,
        help="Number of parallel workers for ASF SAFE queries (default: 8). Lower if hitting rate limits.",
    )
    args = parser.parse_args()

    # Update logger level if debug mode enabled
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.info("Debug logging enabled")

    collection_name = args.collection
    coll_info = COLLECTIONS[collection_name]

    # Execute auditor steps
    if "audit" in args.steps:
        logger.info(f"Running audit on {collection_name} {', '.join(args.layers)} products")

        # Ensure burst_inventory directory exists
        os.makedirs("burst_inventory", exist_ok=True)

        if "static" in args.layers:
            audit_collection = f"{collection_name}_static"
            run_audit(coll_info.static_collection_id, audit_collection, max_workers=args.cmr_workers)
        if "baseline" in args.layers:
            audit_collection = f"{collection_name}_baseline"
            run_audit(coll_info.baseline_collection_id, audit_collection, max_workers=args.cmr_workers)
            

    # Execute analysis steps
    if "analysis" in args.steps:
        run_analysis(collection_name, args.geo_filter)
        
    if "safe" in args.steps:
        burst_to_input_safe(collection_name, max_workers=args.asf_workers)


if __name__ == "__main__":
    main()
