import csv
import geopandas as gpd
from shapely import wkt

def csv_to_geojson(input_csv, output_geojson):
    """
    Convert a CSV file with WKT polygons to a GeoJSON file.
    """
    features = []

    with open(input_csv, newline='') as f:
        header = next(f)  # Skip the header line

        for line_num, line in enumerate(f, start=2):
            line = line.strip()

            if not line:
                continue  # skip empty lines

            parts = line.split(",", 4)  # split into at most 5 parts
            if len(parts) < 5:
                print(f"⚠️  Line {line_num} is malformed: {line}")
                continue

            burst_id = parts[0].strip()
            safe_id = parts[1].strip()
            abs_orbit = parts[2].strip()
            rel_orbit = parts[3].strip()
            polygon_wkt = parts[4].strip().replace('\n', ' ').replace('\r', ' ').strip('"')

            print(f"Line {line_num}:")
            print(f"  burst_id    = {burst_id}")
            print(f"  safe_id     = {safe_id}")
            print(f"  abs_orbit   = {abs_orbit}")
            print(f"  rel_orbit   = {rel_orbit}")
            print(f"  polygon_wkt = {polygon_wkt[:60]}...")

            # Determine color based on 'SAFE file' field
            if burst_id.lower() != 'none':
                color = '#1f78b4'  # e.g. blue for valid files
            else:
                color = '#e31a1c'  # e.g. red for missing/None

            geom = wkt.loads(polygon_wkt)
            props = {
                "burst_id": burst_id,
                "safe_file_id": safe_id,
                "absolute_orbit": abs_orbit,
                "relative_orbit": rel_orbit,
                "marker-color": color,
                "marker-symbol": "circle",
                "marker-size": "medium"
            }
            features.append({
                "geometry": geom,
                "properties": props
            })

    gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
    gdf.to_file(output_geojson, driver="GeoJSON")
    print(f"✅ GeoJSON written to: {output_geojson}")

# === Example usage ===
input_csv_file = "safe_file_ids_1_1000.txt"
csv_to_geojson(input_csv_file, "bursts_output.geojson")

