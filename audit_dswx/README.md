# OPERA DSWx-S1 High-Latitude Input Audit Script

## Purpose

This script audits OPERA DSWx-S1 products at high latitudes (≥60°N by default) to verify they are not incorrectly composed from RTC-S1 inputs belonging to different Sentinel-1 tracks or with excessive time spread between acquisitions.

**Related to**: OPERA SDS Issue #138

## Requirements

- Python 3.7+
- `requests` library

Install dependencies:
```bash
pip install -r requirements.txt
```

Or with the virtual environment:
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Running Individual Audits

Use the `audit_dswx_inputs.py` script directly for custom time ranges and parameters.

#### Basic Example (10-day window)
```bash
python audit_dswx_inputs.py \
  --temporal "2026-01-20T00:00:00Z,2026-01-30T23:59:59Z" \
  --out failures.csv
```

### Running Pre-configured Audits

Use the provided shell scripts to run audits for predefined time ranges with optimized settings:

```bash
bash audit_last_1_year.sh      # Last 1 year (365 days)
bash audit_last_2_years.sh     # Last 2 years (730 days)
bash audit_last_5_years.sh     # Last 5 years (1825 days)
```

Each script automatically:
- **Calculates current date in UTC** (no manual date updates needed)
- **Computes lookback period** (365, 730, or 1825 days from today)
- Configures appropriate `--max-pages` based on data volume
- Applies throttling (`--sleep`) to avoid CMR rate limits
- Uses 5-minute max time span threshold
- Outputs results to a separate CSV file

The scripts display the calculated start and end dates before running, so you can verify the time range.

### Full Options
```bash
python audit_dswx_inputs.py \
  --collection C2949811996-POCLOUD \
  --temporal "2026-01-01T00:00:00Z,2026-12-31T23:59:59Z" \
  --bbox "-180,60,180,90" \
  --max-time-span-minutes 10.0 \
  --page-size 200 \
  --max-pages 100 \
  --sleep 0.1 \
  --out dswx_failures.csv \
  --json-out dswx_failures.json
```

### Arguments

- `--collection`: DSWx-S1 collection concept-id (default: C2949811996-POCLOUD)
- `--temporal` **(required)**: Time range "start,end" in ISO 8601 format
- `--bbox`: Bounding box W,S,E,N (default: "-180,60,180,90" for ≥60°N)
- `--max-time-span-minutes`: Maximum allowed time span between RTC acquisitions (default: 10.0)
- `--page-size`: CMR page size (default: 200)
- `--max-pages`: Maximum pages to fetch (default: 10)
- `--sleep`: Seconds to sleep between UMM metadata fetches (default: 0.0)
- `--out`: Output CSV path for failures (default: dswx_rtc_failures.csv)
- `--json-out`: Optional JSON output path

## Output

The script produces a CSV (and optionally JSON) containing **only failed** DSWx-S1 granules. Each row includes:

- **dswx_granule_ur**: DSWx-S1 granule name
- **dswx_concept_id**: CMR concept ID
- **start_time**: Granule temporal start
- **end_time**: Granule temporal end
- **bbox**: Spatial bounding box
- **tracks_found**: Comma-separated list of Sentinel-1 tracks (e.g., "056,127")
- **acq_time_min**: Earliest RTC acquisition time
- **acq_time_max**: Latest RTC acquisition time
- **acq_time_span_minutes**: Time span in minutes
- **rtc_inputs_unique**: Pipe-separated list of normalized RTC input identifiers
- **notes**: Description of failure reason

### Failure Conditions

A DSWx-S1 granule is flagged as **failed** if:

1. **Mixed tracks**: RTC inputs come from different Sentinel-1 tracks (e.g., T056 and T127)
2. **Excessive time spread**: Time between earliest and latest RTC acquisition exceeds threshold
3. **Missing/unparseable inputs**: InputGranules missing or cannot be parsed

## Exit Status

- **0**: No failures found (all DSWx-S1 granules passed validation)
- **2**: One or more failures found (for use in CI/automated checks)

## Example Output

```csv
dswx_granule_ur,dswx_concept_id,start_time,end_time,bbox,tracks_found,acq_time_min,acq_time_max,acq_time_span_minutes,rtc_inputs_unique,notes
OPERA_L3_DSWx-S1_...,G123456789-POCLOUD,2026-01-20T05:00:00Z,2026-01-20T06:00:00Z,"[-180,60,180,90]","056,127",2026-01-20T05:10:00Z,2026-01-20T05:15:00Z,5.000,OPERA_L2_RTC-S1_T056-... | OPERA_L2_RTC-S1_T127-...,Mixed tracks detected
```

## How It Works

1. **Query CMR**: Search for DSWx-S1 granules matching temporal and spatial filters
2. **Fetch UMM metadata**: Retrieve native UMM-JSON for each granule to extract InputGranules
3. **Normalize inputs**: Deduplicate RTC variants (HH/HV/mask/h5) to unique base identifiers
4. **Parse metadata**: Extract track IDs (T056) and acquisition times (YYYYMMDDThhmmssZ)
5. **Validate**:
   - All RTC inputs must share the same track
   - Time spread must be within threshold
6. **Report**: Write failures to CSV/JSON

## Available Audit Scripts

The following pre-configured scripts are available for common time ranges:

| Script | Time Range | Max Pages | Sleep | Output File |
|--------|-----------|-----------|-------|-------------|
| `audit_last_1_year.sh` | Last 1 year (365 days) | 200 | 0.1s | `failures_1year_5min.csv` |
| `audit_last_2_years.sh` | Last 2 years (730 days) | 400 | 0.1s | `failures_2years_5min.csv` |
| `audit_last_5_years.sh` | Last 5 years (1825 days) | 1000 | 0.2s | `failures_5years_5min.csv` |

All scripts use:
- **Threshold**: 5-minute max time span between RTC acquisitions
- **Spatial filter**: ≥60°N (high latitudes)
- **Collection**: C2949811996-POCLOUD (OPERA DSWx-S1)

### Creating Custom Audit Scripts

To create an audit script for a different time range, copy the template:

```bash
#!/bin/bash
set -e
END_DATE="2026-02-12T23:59:59Z"
echo "Running custom audit..."
python audit_dswx_inputs.py \
  --temporal "START_DATE,$END_DATE" \
  --max-time-span-minutes 5.0 \
  --max-pages 200 \
  --sleep 0.1 \
  --out custom_failures.csv
```

## Duplicate Detection

The audit script includes enhanced duplicate detection:

1. **Exact duplicates**: Detects when the same file is listed multiple times in InputGranules
2. **File variant deduplication**: Removes HH/HV/mask/h5 variants to count unique acquisitions
3. **Unparseable entries**: Flags entries that don't match expected RTC naming patterns

Duplicate warnings are printed to stderr during execution.

## Scaling to Multi-Year Ranges

For large temporal ranges:
- Increase `--max-pages` as needed (e.g., 1000 for 5+ years)
- Add `--sleep 0.1` to 0.2 to avoid CMR rate limiting
- Consider splitting into multiple runs by year/quarter if needed

## Notes

- RTC normalization handles multiple file variants per acquisition (e.g., `_HH.tif`, `_HV.tif`, `_mask.tif`, `.h5`)
- Track parsing expects format: `RTC-S1_T<track>-...` (e.g., T056)
- Acquisition time parsing expects: `_YYYYMMDDThhmmssZ_` token in RTC identifier
- All scripts use exponential backoff for CMR API retries (2s, 4s, 8s, 10s max)
