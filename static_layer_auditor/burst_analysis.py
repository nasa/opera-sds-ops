import os
from typing import Optional, Union
import pandas as pd
import geopandas as gpd
from shapely.geometry import box, Polygon
from shapely.geometry.base import BaseGeometry
import logging
import requests
import zipfile
from pathlib import Path


logger = logging.getLogger("burst-audit-tool")

# Burst geometry file configuration
BURST_DB_URL = "https://github.com/opera-adt/burst_db/releases/download/v0.9.0/burst-id-geometries-simple-0.9.0.geojson.zip"
BURST_GEOJSON_FILE = "burst-id-geometries-simple-0.9.0.geojson"


def parse_geo_filter(arg: str) -> Union[Polygon, BaseGeometry]:
    """
    Parse a geometry filter from a bounding box string or GeoJSON file.

    Supports two input formats:
      - Bounding box string: 'bbox:minx,miny,maxx,maxy'
      - Path to a .geojson file containing geometries

    Args:
        arg (str): Either a bbox string (e.g., 'bbox:-120,35,-119,36') or a path to a GeoJSON file.

    Returns:
        Union[Polygon, BaseGeometry]: A Shapely geometry representing the parsed spatial filter.
                                      For bbox input, a rectangular Polygon is returned.
                                      For GeoJSON input, the unary union of geometries is returned.

    Raises:
        ValueError: If the bounding box is incorrectly formatted,
                    if the GeoJSON file is empty,
                    or if the input format is invalid.
    """
    if arg.startswith("bbox:"):
        coords = list(map(float, arg[5:].split(",")))
        if len(coords) != 4:
            raise ValueError("Bounding box must have four comma-separated values: minx,miny,maxx,maxy")
        return box(*coords)
    elif os.path.isfile(arg) and arg.endswith(".geojson"):
        gdf = gpd.read_file(arg)
        if gdf.empty:
            raise ValueError(f"No geometry found in {arg}")
        return gdf.union_all()
    else:
        raise ValueError("Invalid --geo-filter. Must be 'bbox:minx,miny,maxx,maxy' or a valid .geojson file")


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


def load_burst_csv() -> pd.DataFrame:
    """
    Load the burst geometry reference file.

    Downloads from opera-adt/burst_db if not present locally.
    Normalizes burst IDs from lowercase_underscore to UPPERCASE-HYPHEN format.

    Returns:
        pd.DataFrame: A DataFrame with burst_id_jpl and geometry columns (normalized IDs).
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




def apply_geofilter(
    burst_list: pd.DataFrame,
    geo_filter: Union[Polygon, BaseGeometry],
    burst_geom_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Applies geofiltering to burst lists:
    Adds geometry to bursts
    Subsets based on geometry
    """
    burst_with_geom = burst_list.merge(burst_geom_df[["burst_id_jpl", "geometry"]], on="burst_id_jpl", how="left")

    # Check for missing geometries
    missing_geom = burst_with_geom["geometry"].isna()
    if missing_geom.any():
        missing_count = missing_geom.sum()
        missing_bursts = burst_with_geom.loc[missing_geom, "burst_id_jpl"].tolist()[:5]  # Show first 5
        logger.warning(
            f"{missing_count} bursts have no geometry data and will be excluded from geo-filtering. "
            f"Examples: {missing_bursts}"
        )
        # Drop rows with missing geometries
        burst_with_geom = burst_with_geom.dropna(subset=["geometry"])

    # Convert WKT strings to geometries (vectorized for performance)
    burst_with_geom["geometry"] = gpd.GeoSeries.from_wkt(burst_with_geom["geometry"])
    bursts_gdf = gpd.GeoDataFrame(burst_with_geom, geometry="geometry", crs="EPSG:4326")
    filter_gdf = gpd.GeoSeries([geo_filter], crs="EPSG:4326")

    # Subset by intersection
    intersecting_bursts = bursts_gdf[bursts_gdf.intersects(filter_gdf.iloc[0])]
    logger.info(f"Geo-filter kept {len(intersecting_bursts)} out of {len(burst_list)} bursts")
    return intersecting_bursts[["burst_id_jpl"]]


def run_analysis(collection_name: str, geo_filter_arg: Optional[str] = None, save_results: bool = True):
    '''
    Analyzes baseline and static burst collections to identify missing static layers.
    '''
    logger.info(f"Running analysis on {collection_name} baseline and static products")

    static_cache_path = os.path.join("burst_inventory", f"{collection_name}_static_cmr_cache.csv")
    baseline_cache_path = os.path.join("burst_inventory", f"{collection_name}_baseline_cmr_cache.csv")

    # Check if cache files exist
    if not os.path.exists(static_cache_path):
        raise FileNotFoundError(f"Static cache not found: {static_cache_path}. Run audit step first.")
    if not os.path.exists(baseline_cache_path):
        raise FileNotFoundError(f"Baseline cache not found: {baseline_cache_path}. Run audit step first.")

    static_burst_df = pd.read_csv(static_cache_path, header=None, names=["burst_id_jpl"])
    baseline_burst_df = pd.read_csv(baseline_cache_path, header=None, names=["burst_id_jpl"])

    logger.info(f"Loaded {len(static_burst_df)} static bursts and {len(baseline_burst_df)} baseline bursts")

    if geo_filter_arg:
        burst_geom_df = load_burst_csv()
        geo_filter_geom = parse_geo_filter(geo_filter_arg)
        logger.info(f"Applying geo_filter {geo_filter_arg} to baseline and static products")

        baseline_burst_df = apply_geofilter(baseline_burst_df, geo_filter_geom, burst_geom_df)
        static_burst_df = apply_geofilter(static_burst_df, geo_filter_geom, burst_geom_df)

        if baseline_burst_df.empty:
            logger.warning("No baseline bursts remain after geo-filtering")
        if static_burst_df.empty:
            logger.warning("No static bursts remain after geo-filtering")

    only_in_baseline = baseline_burst_df[~baseline_burst_df["burst_id_jpl"].isin(static_burst_df["burst_id_jpl"])]
    logger.info(f"Found {len(only_in_baseline)} baseline bursts without matching static layer.")

    if save_results:
        os.makedirs("analysis_outputs", exist_ok=True)
        output_path = f"analysis_outputs/{collection_name}_bursts_without_static_bursts.txt"
        only_in_baseline.to_csv(output_path, header=False, index=False)
        logger.info(f"Saved results to {output_path}")

    return only_in_baseline
