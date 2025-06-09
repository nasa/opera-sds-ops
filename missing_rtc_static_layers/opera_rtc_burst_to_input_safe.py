import requests
import re

# parameters
url = "https://api.daac.asf.alaska.edu/services/search/param"
input_file = "rtc_bursts_without_static_bursts.txt"
output_file = "safe_file_ids.txt"

# Open output file for writing
with open(output_file, "w") as fout:
    fout.write("RTC-S1 Burst ID, SAFE file ID, Absolute Orbit\n")
    
    # --- Load burst IDs from a local file
    with open(input_file) as f:
        burst_ids = [line.strip() for line in f if line.strip()]

    # Optional: Define a time window (can be omitted or customized)
    start_time = "2017-01-01T00:00:00Z"
    end_time = "2018-01-01T00:00:00Z"

    # --- Loop over each burst ID and search ASF
    count = 0
    for burst_id in burst_ids:
        match = re.match(r"T(\d{3})-(\d+)-IW(\d)", burst_id)
        if not match:
            print(f"Invalid burst ID format: {burst_id}")
            continue

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

        print(f"\n🔍 Searching for burst ID: {burst_id} (Track {relative_orbit}, Swath {swath})")

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
                    fout.write(f"{granule['product_file_id']}, {granule['absoluteOrbit']}\n")
        except requests.RequestException as e:
            print(f"  ❌ Request failed: {e}")

        count += 1
        # if count > 3:
        #     break
