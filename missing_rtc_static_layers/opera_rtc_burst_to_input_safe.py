"""
Instructions:
- Edit the custom parameters section, including start/stop count to break up the number of queries.
- nohup python -u opera_rtc_burst_to_input_safe.py &

The input files contain the burst ids with missing static products, in the form:
T174-372337-IW3

where:
- T174: absolute Track number
- 372337: globally unique burst id assigned by ASF
- IW3: polarization

"""
import requests
import re
import geopandas as gpd

# Parameters
url = "https://api.daac.asf.alaska.edu/services/search/param"
burst_geometry_file = "https://github.com/opera-adt/burst_db/releases/download/v0.9.0/burst-id-geometries-simple-0.9.0.geojson.zip"
start_time = "2017-01-01T00:00:00Z"
end_time = "2018-01-01T00:00:00Z"

# Global scope
# input_file = "rtc_bursts_without_static_bursts.txt"
# start_count = 10001
# stop_count = 12000

# Missing RTC/CSLC static layers from DISP-S1-STATIC and Tropo System Tests
input_file = "rtc_cslc_missing_static_layers.txt"
start_count = 1
stop_count = 1000

# Australia scope
# input_file = "rtc_bursts_without_static_bursts_au.txt"
# start_count = 1
# stop_count = 4

output_file = f"safe_file_ids_{start_count}_{stop_count}.txt"

# Read the Sentinel-1 geometry file from GitHub
print(f"Reading burst geometry file: {burst_geometry_file}")
burst_grid = gpd.read_file(burst_geometry_file)
# print(burst_grid)
# print(burst_grid.columns)

# Open output file for writing
with open(output_file, "w") as fout:
    fout.write("RTC-S1 Burst ID, SAFE file ID, Absolute Orbit, Relative Orbit, Polygon\n")
    
    # --- Load burst IDs from a local file
    with open(input_file) as f:
        burst_ids = [line.strip() for line in f if line.strip()]

    # --- Loop over each burst ID and search ASF
    count = 0
    for burst_id in burst_ids:
        count += 1
        match = re.match(r"T(\d{3})-(\d+)-IW(\d)", burst_id)
        if not match:
            print(f"Invalid burst ID format: {burst_id}")
            continue

        if (count >= start_count) and (count <= stop_count):

            relative_orbit = int(match.group(1))
            swath = f"IW{match.group(3)}"

            # "T174-372072-IW1" --> "t174_372072_iw1"
            burst_id_converted = burst_id.lower().replace('-', '_')
            burst_geom = burst_grid[burst_grid['burst_id_jpl'] == burst_id_converted]
            # print(burst_geom)
            (minx, miny, maxx, maxy) = burst_geom.geometry.iloc[0].bounds
            # print(minx, miny, maxx, maxy)
            polygon = f"POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))"

            params = {
                "platform": "Sentinel-1",
                "processingLevel": "SLC",
                "beamMode": "IW",
                "relativeOrbit": relative_orbit,
                "start": start_time,
                "end": end_time,
                "output": "json",
                "maxResults": 1,
                "intersectsWith": polygon,
                # "burst": burst_id,
                # "absoluteOrbit": "15301",
            }

            print(f"\n🔍 Granule count: {count} Searching for burst ID: {burst_id} (Track {relative_orbit}, Swath {swath})")

            attempts = 0
            while attempts < 3:
                try:
                    response = requests.get(url, params=params)
                    response.raise_for_status()
                    granules = response.json()[0]
                    print(f"Number of results: {len(granules)}")
                    if not granules:
                        print("  No results found.")
                        fout.write(f"{burst_id}, None, None, None, {polygon}\n")
                    else:
                        for granule in granules:
                            fout.write(f"{burst_id}, {granule['product_file_id']}, "
                                       f"{granule['absoluteOrbit']}, {granule['relativeOrbit']}, {polygon}\n")
                    # no more attempts for this granule
                    break
                except requests.RequestException as e:
                    attempts += 1
                    print(f"  ❌ Attempt #: {attempts} Request failed: {e}")
