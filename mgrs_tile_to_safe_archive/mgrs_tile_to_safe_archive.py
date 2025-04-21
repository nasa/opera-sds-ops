import requests
import mgrs
import csv
import json
from shapely.geometry import box, mapping
from pyproj import Transformer
from collections.abc import Iterable
import folium
from folium.plugins import MarkerCluster

# === INPUT PARAMETERS ===
# === INPUT PARAMETERS ===
# Define a dictionary of MGRS tiles over the U.S. with descriptions
mgrs_tiles = {
    "11SLT": "Southern California (Los Angeles)",
    "15TWL": "Midwest (Minnesota)",
    "13SDA": "Colorado (Denver)",         # ✅ Replaces 14SPT
    "17RMP": "Florida (Orlando)",         # ✅ Replaces 17SPQ
    "18SUJ": "Mid-Atlantic (Washington, D.C.)",
    "16TDM": "Texas (Dallas)",
    "12SVC": "Arizona (Phoenix)",
    "10TET": "Oregon (Portland)",
    "19TCE": "Northeast (Boston)",
}


start_date = "2021-01-01"
end_date = "2021-12-31"
platforms = ["Sentinel-1A", "Sentinel-1B"]
product_type = "SLC"
csv_output = "s1_slc_results.csv"
geojson_output = "s1_slc_results.geojson"
map_output = "s1_slc_map.html"

def flatten(items):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, dict) and not isinstance(x, str):
            yield from flatten(x)
        else:
            yield x

def get_mgrs_tile_bounds(tile):
    m = mgrs.MGRS()
    if len(tile) != 5:
        raise ValueError(f"MGRS tile ID '{tile}' must be exactly 5 characters")
    try:
        lat, lon = m.toLatLon(tile + "55")
    except Exception as e:
        raise ValueError(f"Invalid MGRS tile '{tile}': {e}")

    zone_number = int(tile[:2])
    hemisphere = "north" if tile[2] >= "N" else "south"
    epsg = 32600 + zone_number if hemisphere == "north" else 32700 + zone_number

    transformer_to_utm = Transformer.from_crs("EPSG:4326", f"EPSG:{epsg}", always_xy=True)
    transformer_to_latlon = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)

    x_center, y_center = transformer_to_utm.transform(lon, lat)
    half = 50000
    x_min, x_max = x_center - half, x_center + half
    y_min, y_max = y_center - half, y_center + half

    lon_min, lat_min = transformer_to_latlon.transform(x_min, y_min)
    lon_max, lat_max = transformer_to_latlon.transform(x_max, y_max)

    return (lon_min, lat_min, lon_max, lat_max)

def search_asf_s1_slc(minx, miny, maxx, maxy, start_date, end_date, platform):
    url = "https://api.daac.asf.alaska.edu/services/search/param"
    params = {
        "platform": platform,
        "processingLevel": product_type,
        "start": start_date,
        "end": end_date,
        "intersectsWith": f"POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))",
        "output": "json"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

all_results = []
features = []

for tile, description in mgrs_tiles.items():
    print(f"\n🔍 Searching MGRS tile: {tile} — {description}")
    try:
        minx, miny, maxx, maxy = get_mgrs_tile_bounds(tile)
        for platform in platforms:
            print(f"   → Searching {platform}")
            try:
                results = search_asf_s1_slc(minx, miny, maxx, maxy, start_date, end_date, platform)
                flat_results = list(flatten(results))
                count = 0
                for item in flat_results:
                    if isinstance(item, dict):
                        record = {
                            "mgrs_tile": tile,
                            "description": description,
                            "platform": platform,
                            "file_id": item.get("fileID"),
                            "start_time": item.get("startTime"),
                            "stop_time": item.get("stopTime"),
                            "absolute_orbit": item.get("absoluteOrbit"),
                            "path_number": item.get("pathNumber"),
                            "frame_number": item.get("frameNumber"),
                            "beam_mode": item.get("beamMode"),
                            "polarization": item.get("polarization"),
                            "flight_direction": item.get("flightDirection"),
                            "look_direction": item.get("lookDirection"),
                            "burst_count": item.get("burstCount"),
                            "download_url": item.get("downloadUrl")
                        }
                        all_results.append(record)
                        geometry = box(minx, miny, maxx, maxy)
                        features.append({
                            "type": "Feature",
                            "geometry": mapping(geometry),
                            "properties": record
                        })
                        count += 1
                if count > 0:
                    print(f"   ✅ {count} results from {platform}")
                else:
                    print(f"   ⚠️ No usable results from {platform}")
            except Exception as e:
                print(f"   ❌ Error querying {platform}: {e}")
    except Exception as e:
        print(f"❌ Error processing tile {tile}: {e}")

# === Output CSV ===
if all_results:
    keys = all_results[0].keys()
    with open(csv_output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(all_results)
    print(f"\n📁 CSV saved: {csv_output}")
else:
    print("\n⚠️ No results to write to CSV.")

# === Output GeoJSON ===
if features:
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    with open(geojson_output, "w") as f:
        json.dump(geojson, f, indent=2)
    print(f"🗺️  GeoJSON saved: {geojson_output}")
else:
    print("⚠️ No features to write to GeoJSON.")

# === Folium Map Visualization ===
print("\n🌍 Generating interactive map...")

m = folium.Map(location=[0, 0], zoom_start=2, tiles="CartoDB positron")
marker_cluster = MarkerCluster().add_to(m)

for feature in features:
    props = feature["properties"]
    geom = feature["geometry"]
    coords = geom["coordinates"][0]
    center_lat = sum([pt[1] for pt in coords]) / len(coords)
    center_lon = sum([pt[0] for pt in coords]) / len(coords)

    # Polygon
    folium.GeoJson(
        data=feature,
        style_function=lambda x: {
            "fillColor": "#3186cc",
            "color": "#3186cc",
            "weight": 1,
            "fillOpacity": 0.3,
        },
        tooltip=props["file_id"]
    ).add_to(m)

    # Marker
    folium.Marker(
        location=[center_lat, center_lon],
        popup=folium.Popup(
            f"<b>{props['file_id']}</b><br>"
            f"Platform: {props['platform']}<br>"
            f"Tile: {props['mgrs_tile']}<br>"
            f"Orbit: {props['absolute_orbit']}<br>"
            f"Date: {props['start_time'][:10]}",
            max_width=250
        ),
        icon=folium.Icon(color="blue", icon="info-sign")
    ).add_to(marker_cluster)

m.save(map_output)
print(f"✅ Interactive map saved: {map_output}")
