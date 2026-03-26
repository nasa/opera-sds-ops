# OPERA Static Layer Auditor

Tool for auditing OPERA Sentinel-1 burst coverage across static and baseline product collections.

## Setup

Navigate to the tool directory and create a virtual environment:
```bash
cd static_layer_auditor
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

**Note**: The burst geometry reference file (`burst-id-geometries-simple-0.9.0.geojson`) is automatically downloaded from [opera-adt/burst_db](https://github.com/opera-adt/burst_db) on first run if not already present.

## Usage

The tool performs three steps:

1. **audit**: Query CMR to build/update cache of burst IDs from static and baseline collections
2. **analysis**: Compare baseline vs static to identify bursts missing static layers
3. **safe**: Query ASF to find input SAFE files for missing bursts (handles S1C calibration phase)

**Basic usage:**
```bash
python burst_audit_tool.py --collection {RTC|CSLC}
```

**Options:**
- `--collection`: Required. Either `RTC` or `CSLC`
- `--steps`: Select steps to run: `audit`, `analysis`, `safe` (default: all three)
- `--layers`: Select layers to audit: `static`, `baseline` (default: both)
- `--geo-filter`: Geographic filter for analysis (bbox or GeoJSON file path)
- `--cmr-workers`: Parallel workers for CMR audit queries (default: 5)
- `--asf-workers`: Parallel workers for ASF SAFE queries (default: 8)
- `--debug`: Enable debug logging

**Examples:**
```bash
# Audit only static layers for CSLC
python burst_audit_tool.py --collection CSLC --steps audit --layers static

# Run full analysis with geographic filter
python burst_audit_tool.py --collection RTC --geo-filter "bbox:-120,35,-115,40"

# Run with debug logging and custom worker count
python burst_audit_tool.py --collection CSLC --debug --cmr-workers 8 --asf-workers 5
```

## Outputs

The tool creates several output files:

- `burst_inventory/{COLLECTION}_{static|baseline}_cmr_cache.csv` - Cached burst IDs from CMR (incrementally updated)
- `analysis_outputs/{COLLECTION}_bursts_without_static_bursts.txt` - List of bursts missing static layers
- `analysis_outputs/audit_{COLLECTION}_safe_file_ids.txt` - Mapping of bursts to input SAFE files

## Notes

**S1C Calibration Handling**: The SAFE file lookup automatically filters out Sentinel-1C data from before 2025-05-20 (calibration phase). For bursts with both S1A/S1B and post-calibration S1C coverage, the tool selects the earliest appropriate granule.

**Incremental Updates**: The audit step uses file modification times to perform incremental CMR queries, only fetching granules updated since the last run.

**Parallel Processing**: Both CMR and ASF queries use parallel workers for improved performance. Adjust worker counts if you encounter rate limiting.