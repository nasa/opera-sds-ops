import requests
import re

# parameters
url = "https://api.daac.asf.alaska.edu/services/search/param"
input_file = "rtc_bursts_without_static_bursts.txt"
start_count = 1
stop_count = 1000
output_file = f"safe_file_ids_{start_count}_{stop_count}.txt"
# Optional: Define a time window (can be omitted or customized)
start_time = "2017-01-01T00:00:00Z"
end_time = "2018-01-01T00:00:00Z"

# Open output file for writing
with open(output_file, "w") as fout:
    fout.write("RTC-S1 Burst ID, SAFE file ID, Absolute Orbit\n")
    
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

            params = {
                "platform": "Sentinel-1",
                "processingLevel": "SLC",
                "beamMode": "IW",
                "relativeOrbit": relative_orbit,
                "start": start_time,
                "end": end_time,
                "output": "json",
                "maxResults": 1,
                # "absoluteOrbit": "15301",
            }

            print(f"\n🔍 Granule count: {count} Searching for burst ID: {burst_id} (Track {relative_orbit}, Swath {swath})")

            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                granules = response.json()[0]
                print(f"Number of results: {len(granules)}")
                if not granules:
                    print("  No results found.")
                else:
                    for granule in granules:
                        # print(granule["product_file_id"], granule["absoluteOrbit"])
                        fout.write(f"{burst_id}, {granule['product_file_id']}, {granule['absoluteOrbit']}\n")
            except requests.RequestException as e:
                print(f"  ❌ Request failed: {e}")
