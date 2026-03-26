"""
Query ASF DAAC for SAFE files corresponding to bursts missing static layers.

This module handles parallel querying of the Alaska Satellite Facility (ASF) API
to find the earliest appropriate SAFE file for each burst, accounting for platform-specific
constraints (e.g., S1C commissioning phase).
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging
import os
from time import sleep
from typing import Optional
from requests import RequestException, Session
import re
import pandas as pd
import geopandas as gpd
import requests
import zipfile

logger = logging.getLogger("burst-audit-tool")


# Configuration constants
ASF_URL = "https://api.daac.asf.alaska.edu/services/search/param"
START_TIME = "2017-01-01T00:00:00Z"
END_TIME = "2026-01-01T00:00:00Z"
S1C_CALIBRATION_DATE = "2025-05-20T00:00:00Z"

# Burst geometry file configuration
BURST_DB_URL = "https://github.com/opera-adt/burst_db/releases/download/v0.9.0/burst-id-geometries-simple-0.9.0.geojson.zip"
BURST_GEOJSON_FILE = "burst-id-geometries-simple-0.9.0.geojson"

# Rate limiting and retry configuration
RATE_LIMIT_BACKOFF_BASE = 5  # seconds
REQUEST_DELAY = 0.1  # seconds between requests (reduced from 0.5)
MAX_RETRIES = 3
PROGRESS_LOG_INTERVAL = 100  # log progress every N bursts (increased for less spam)


BASE_PARAMS = {
    "platform": "Sentinel-1",
    "processingLevel": "SLC",
    "beamMode": "IW",
    "start": START_TIME,
    "end": END_TIME,
    "output": "json",
    # Note: fetch all results and sort client-side to get earliest
}


def normalize_burst_id(burst_id: str) -> str:
    """
    Normalize burst ID from lowercase with underscores to uppercase with hyphens.

    Args:
        burst_id: Burst ID in format like 't001_000001_iw1'

    Returns:
        Normalized burst ID like 'T001-000001-IW1'
    """
    return burst_id.upper().replace('_', '-')


def download_burst_geometry() -> None:
    """
    Download burst geometry GeoJSON from opera-adt/burst_db if not already present.

    Downloads the zip file, extracts the GeoJSON, and removes the zip.
    """
    if os.path.exists(BURST_GEOJSON_FILE):
        logger.debug(f"{BURST_GEOJSON_FILE} already exists, skipping download")
        return

    logger.info(f"Downloading burst geometry from {BURST_DB_URL}")

    zip_file = f"{BURST_GEOJSON_FILE}.zip"

    try:
        # Download with progress
        response = requests.get(BURST_DB_URL, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        logger.info(f"Downloading {total_size / 1024 / 1024:.1f} MB...")

        with open(zip_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info("Extracting GeoJSON...")
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall('.')

        # Clean up zip file
        os.remove(zip_file)
        logger.info(f"Successfully downloaded {BURST_GEOJSON_FILE}")

    except Exception as e:
        # Clean up on failure
        if os.path.exists(zip_file):
            os.remove(zip_file)
        raise RuntimeError(f"Failed to download burst geometry: {e}")


def load_geometry_file() -> pd.DataFrame:
    """
    Load burst geometry reference file.

    Downloads from opera-adt/burst_db if not present locally.
    Normalizes burst IDs from lowercase_underscore to UPPERCASE-HYPHEN format.

    Returns:
        pd.DataFrame: DataFrame with burst_id_jpl and geometry columns (normalized IDs)
    """
    # Download if needed
    download_burst_geometry()

    logger.info(f"Loading {BURST_GEOJSON_FILE}")
    gdf = gpd.read_file(BURST_GEOJSON_FILE)

    # Normalize burst IDs to match CMR cache format (T###-######-IW#)
    gdf['burst_id_jpl'] = gdf['burst_id_jpl'].apply(normalize_burst_id)

    # Convert to DataFrame with WKT geometry for consistency with existing code
    df = pd.DataFrame({
        'burst_id_jpl': gdf['burst_id_jpl'],
        'geometry': gdf['geometry'].apply(lambda geom: geom.wkt)
    })

    logger.info(f"Loaded {len(df):,} burst geometries")
    return df


def select_appropriate_granule(granules: list, burst_id: str) -> Optional[dict]:
    """
    Select the appropriate SAFE file from query results.

    For S1A/S1B: Returns earliest granule
    For S1C: Returns earliest granule on or after 2025-05-20 (post-commissioning)

    Args:
        granules: List of granules from ASF API
        burst_id: Burst ID for logging

    Returns:
        Selected granule dict, or None if no appropriate granule found
    """
    if not granules:
        return None

    # Sort by start time
    granules_sorted = sorted(granules, key=lambda g: g.get("startTime", ""))

    # Check if any S1C granules exist
    has_s1c = any(g.get("platform") == "Sentinel-1C" for g in granules_sorted)

    if has_s1c:
        # Filter S1C granules to post-CALIBRATION only
        calibration_dt = datetime.fromisoformat(S1C_CALIBRATION_DATE.replace('Z', '+00:00'))

        valid_granules = []
        for g in granules_sorted:
            if g.get("platform") == "Sentinel-1C":
                granule_dt = datetime.fromisoformat(g.get("startTime", "").replace('Z', '+00:00'))
                if granule_dt >= calibration_dt:
                    valid_granules.append(g)
            else:
                # Include all S1A/S1B granules
                valid_granules.append(g)

        if not valid_granules:
            logger.warning(
                f"[{burst_id}] Found S1C granules but all are before calibration date "
                f"({S1C_CALIBRATION_DATE}). No valid granules."
            )
            return None

        selected = valid_granules[0]
        if selected.get("platform") == "Sentinel-1C":
            logger.debug(f"[{burst_id}] Selected post-calibration S1C: {selected.get('startTime')}")
    else:
        # No S1C, just use earliest
        selected = granules_sorted[0]

    if len(granules_sorted) > 1:
        logger.debug(
            f"[{burst_id}] Found {len(granules_sorted)} results, "
            f"selected {selected.get('platform')}: {selected.get('startTime')}"
        )

    return selected


def query_asf(session: Session, params: dict, burst_id: str, polygon: str) -> Optional[dict]:
    """
    Query ASF for SAFE files matching the burst criteria.

    Returns the earliest appropriate SAFE file, accounting for S1C commissioning phase.

    Args:
        session: Requests session for connection pooling
        params: ASF API query parameters
        burst_id: Burst ID for logging and error tracking
        polygon: WKT polygon string for the burst geometry

    Returns:
        Dict with burst metadata and selected SAFE file, or dict with None values if not found
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(ASF_URL, params=params)
            response.raise_for_status()
            granules = response.json()
        except RequestException as e:
            # Handle rate limiting with exponential backoff
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 429:
                backoff = RATE_LIMIT_BACKOFF_BASE * (2 ** (attempt - 1))
                logger.warning(f"[{burst_id}] Rate limited (429), waiting {backoff}s before retry {attempt}")
                sleep(backoff)
            else:
                logger.warning(f"[{burst_id}] Attempt {attempt} failed: {e}")
                sleep(2)
            continue

        # ASF returns [[granules]]
        granules = granules[0]

        if len(granules) < 1:
            logger.warning(f"[{burst_id}] No results found")
            return {
                "burst_id": burst_id,
                "product_file_id": None,
                "absolute_orbit": None,
                "relative_orbit": None,
                "platform": None,
                "start_time": None,
                "geometry": polygon
            }

        # Select appropriate granule based on platform and commissioning dates
        granule = select_appropriate_granule(granules, burst_id)

        if not granule:
            # No valid granule found (e.g., only pre-commissioning S1C)
            return {
                "burst_id": burst_id,
                "product_file_id": None,
                "absolute_orbit": None,
                "relative_orbit": None,
                "platform": None,
                "start_time": None,
                "geometry": polygon
            }

        # Small delay to avoid rate limiting (even with parallel workers)
        sleep(REQUEST_DELAY)

        return {
            "burst_id": burst_id,
            "product_file_id": granule.get("product_file_id"),
            "absolute_orbit": granule.get("absoluteOrbit"),
            "relative_orbit": granule.get("relativeOrbit"),
            "platform": granule.get("platform"),
            "start_time": granule.get("startTime"),
            "geometry": polygon
        }

    raise RuntimeError(f"Unable to query ASF for burst {burst_id} within {MAX_RETRIES} attempts")


def process_burst(burst_id: str, burst_grid: pd.DataFrame, session: Session) -> Optional[dict]:
    """
    Process a single burst: lookup geometry, query ASF, and return result.

    Args:
        burst_id: Burst ID in format T###-######-IW#
        burst_grid: DataFrame with burst geometries
        session: Reusable requests Session for connection pooling

    Returns:
        Dict with burst and SAFE metadata, or None if processing failed
    """
    # Lookup geometry
    row = burst_grid.loc[burst_grid["burst_id_jpl"] == burst_id]
    if row.empty:
        logger.warning(f"[{burst_id}] No matching polygon found.")
        return None

    polygon = row.iloc[0].geometry

    # Parse burst ID
    match = re.match(r"T(\d{3})-(\d+)-IW(\d)", burst_id)
    if not match:
        logger.warning(f"[{burst_id}] Invalid burst ID format")
        return None

    relative_orbit = int(match.group(1))

    # Build query parameters
    params = BASE_PARAMS.copy()
    params.update({"relativeOrbit": relative_orbit, "intersectsWith": str(polygon)})

    # Query ASF
    try:
        result = query_asf(session, params, burst_id, polygon)
        logger.debug(f"[{burst_id}] Found SAFE: {result['product_file_id']}")
        return result
    except RuntimeError as e:
        logger.warning(f"[{burst_id}] Skipping due to repeated failure: {e}")
        return None


def asf_audit(collection_name: str, burst_grid: pd.DataFrame, max_workers: int = 8) -> pd.DataFrame:
    """
    Query ASF DAAC for SAFE files corresponding to bursts missing static layers.

    Uses parallel workers with connection pooling for improved performance.

    Args:
        collection_name: Name of the collection (RTC or CSLC)
        burst_grid: DataFrame containing burst geometries
        max_workers: Maximum number of concurrent ASF queries (default: 8, increased from 3)

    Returns:
        DataFrame with burst IDs and their corresponding SAFE file metadata

    Raises:
        FileNotFoundError: If the input burst list file doesn't exist
    """
    input_file = f"analysis_outputs/{collection_name}_bursts_without_static_bursts.txt"
    output_file = f"analysis_outputs/audit_{collection_name}_safe_file_ids.txt"

    if not os.path.isfile(input_file):
        raise FileNotFoundError(f"Input burst list file {input_file} does not exist!")

    burst_df = pd.read_csv(input_file, names=["burst_id_jpl"])
    burst_ids = burst_df["burst_id_jpl"].tolist()

    if not burst_ids:
        logger.warning("No bursts to process in input file")
        return pd.DataFrame()

    logger.info(f"Processing {len(burst_ids)} bursts with {max_workers} parallel workers")

    results = []
    completed = 0

    # Create persistent sessions for each worker to reuse connections
    sessions = [Session() for _ in range(max_workers)]

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks with round-robin session assignment
            future_to_burst = {
                executor.submit(process_burst, burst_id, burst_grid, sessions[i % max_workers]): burst_id
                for i, burst_id in enumerate(burst_ids)
            }

            # Process completed tasks as they finish
            for future in as_completed(future_to_burst):
                completed += 1
                burst_id = future_to_burst[future]

                try:
                    result = future.result()
                    if result:
                        results.append(result)

                    # Log progress periodically
                    if completed % PROGRESS_LOG_INTERVAL == 0:
                        logger.info(f"Progress: {completed}/{len(burst_ids)} bursts processed")
                except Exception as e:
                    logger.error(f"[{burst_id}] Unexpected error: {e}")
    finally:
        # Clean up sessions
        for session in sessions:
            session.close()

    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False)
    logger.info(f"ASF audit complete — wrote {len(df)} entries to {output_file}")
    return df

def burst_to_input_safe(collection_name: str, max_workers: int = 8) -> None:
    """
    Main entry point: loads burst geometries and audits ASF for SAFE files.

    Args:
        collection_name: Name of the collection (RTC or CSLC)
        max_workers: Maximum number of concurrent ASF queries (default: 8)
    """
    burst_grid = load_geometry_file()
    asf_audit(collection_name, burst_grid, max_workers=max_workers)
