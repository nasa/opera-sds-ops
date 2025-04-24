import requests
import mgrs
import csv
import json
from shapely.geometry import box, mapping
from pyproj import Transformer
from collections.abc import Iterable
import folium
from folium.plugins import MarkerCluster
import re

# === INPUT PARAMETERS ===
# Define a dictionary of MGRS tiles over the U.S. with descriptions
mgrs_tiles = {
    "35LKG": "35LKG", "43WFS": "43WFS", "19QBB": "19QBB", "18UXA": "18UXA", "22KDG": "22KDG",
    "30NTN": "30NTN", "48MYC": "48MYC", "33KYQ": "33KYQ", "37MEV": "37MEV", "50UQD": "50UQD",
    "36PYQ": "36PYQ", "54KVE": "54KVE", "30STE": "30STE", "22MHC": "22MHC", "18SUG": "18SUG",
    "48PUS": "48PUS", "43VDJ": "43VDJ", "16PFA": "16PFA", "22MCB": "22MCB", "18NYK": "18NYK",
    "19PFK": "19PFK", "13SER": "13SER", "20UPE": "20UPE", "18NUL": "18NUL", "19MBR": "19MBR",
    "21LZC": "21LZC", "33LXE": "33LXE", "52LHL": "52LHL", "37VEK": "37VEK", "30PXT": "30PXT",
    "51UWQ": "51UWQ", "44WME": "44WME", "34LFR": "34LFR", "44QPF": "44QPF", "49MET": "49MET",
    "30TVM": "30TVM", "37MBN": "37MBN", "14SMF": "14SMF", "10TES": "10TES", "14SMJ": "14SMJ",
    "21KVT": "21KVT", "20LKH": "20LKH", "20LQK": "20LQK", "47NRA": "47NRA", "15QZC": "15QZC",
    "21KTU": "21KTU", "22LDL": "22LDL", "21KTQ": "21KTQ", "16QDJ": "16QDJ", "48MWC": "48MWC",
    "36VVM": "36VVM", "37VCG": "37VCG", "12TVM": "12TVM", "34LCK": "34LCK", "18MUU": "18MUU",
    "21MZT": "21MZT", "21KXT": "21KXT", "34LGP": "34LGP", "50NNJ": "50NNJ", "48SUC": "48SUC",
    "21JVG": "21JVG", "35LML": "35LML", "47QKU": "47QKU", "40WFC": "40WFC", "34MFC": "34MFC",
    "51VVG": "51VVG", "53WMQ": "53WMQ", "51PVM": "51PVM", "32UMC": "32UMC", "37TCN": "37TCN",
    "18TXL": "18TXL", "38TMP": "38TMP", "16TGM": "16TGM", "30UWC": "30UWC", "22JCR": "22JCR",
    "33UVS": "33UVS", "37TGN": "37TGN", "16TFL": "16TFL", "23KKP": "23KKP", "50SPF": "50SPF",
    "20LNJ": "20LNJ", "50UME": "50UME", "51VUG": "51VUG", "10WFT": "10WFT", "10TFK": "10TFK",
    "54WVD": "54WVD", "11VLE": "11VLE", "50VNK": "50VNK", "55WFP": "55WFP", "22MBT": "22MBT",
    "37LBH": "37LBH", "53MRS": "53MRS", "48RTQ": "48RTQ", "07VFJ": "07VFJ", "52PBQ": "52PBQ",
    "44QKK": "44QKK", "49UCQ": "49UCQ", "56MPV": "56MPV", "13UDV": "13UDV", "56HLH": "56HLH"
}


product_type = "SLC"
csv_output = "s1_slc_results.csv"
csv_output_2 = "s1_slc_ids.csv"
geojson_output = "s1_slc_results.geojson"
map_output = "s1_slc_map.html"

# Use these settings for Sentinel-1a/B
# start_date = "2021-01-01"
# end_date = "2021-12-31"
# platforms = ["Sentinel-1A", "Sentinel-1B"]

# Use these settings for Sentinel-1C
start_date = "2024-12-01"
end_date = "2025-04-24"
platforms = ["Sentinel-1C"]


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


# regular expression to filter out any SAFE file that is not "_IW_"
# S1B_IW_SLC__1SDV_20210325T190648_20210325T190715_026175_031FB3_744C.zip
PATTERN = re.compile(r'^S1[ABC]_IW_SLC__\d{1}[A-Z]{3}_\d{8}T\d{6}_\d{8}T\d{6}_\d{6}_[A-Z0-9]{6}_[A-Z0-9]{4}$')

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
    for attempt in range(3):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt == 2:
                print("All attempts failed.")
                raise e


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

    # write out granule id only
    with open(csv_output_2, "w", newline="\n") as f:
        for result in all_results:
            filename = result['download_url'].split("/")[-1].replace(".zip", "")
            if PATTERN.match(filename):
                f.write(f"{filename}\n")
    print(f"\n📁 CSV saved: {csv_output_2}")

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
