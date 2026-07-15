# Usage Examples

Comprehensive usage guide for the OPERA Accountability Framework.

**Note:** This package consolidates tools from 4 contributors (Riley, Gerald, Chris, Kevin).
See `CONSOLIDATION_MAP.md` for original tool locations and `README.md` for consolidation history.

## Installation

```bash
cd opera-audit
uv venv
source .venv/bin/activate
uv pip install -e .

# Optional extras:
uv pip install -e ".[grq]"             # GRQ (OpenSearch) duplicate detection
uv pip install -e ".[burst_coverage]"   # SLC burst-level coverage audit
uv pip install -e ".[dist_s1]"          # DIST-S1 S3 ISO-XML access
```

## Supported Products

| Product | Duplicates | Accountability | Burst Coverage | Strategy | Notes |
|---------|-----------|----------------|----------------|----------|-------|
| DSWX_HLS | yes | yes | — | `dswx_hls` / `forward_map` | Chris |
| RTC_S1 | yes | no | yes | — | Riley |
| CSLC_S1 | yes | no | yes | — | Riley |
| DSWX_S1 | yes | yes | — | `dswx_s1` | Riley, requires `--mgrs-db` |
| DIST_S1 | yes | yes | — | `dist_s1` | Kevin, uses ISO-XML extraction |
| DISP_S1 | yes | yes | — | `delegated_validator` | Gerald + Chris, supports `--check-end-conflicts` |
| TROPO | yes | yes | — | `date_count` | Chris, counts by date |
| DISP_S1_STATIC | yes | yes | — | `db_based` | Chris, sample DB included |
| DIST_ALERT_HLS | yes | no | — | — | Riley |
| CSLC_S1_STATIC | yes | no | — | — | Riley |
| RTC_S1_STATIC | yes | no | — | — | Riley |

**Note on products without accountability:** 5 products (RTC_S1, CSLC_S1, CSLC_S1_STATIC, RTC_S1_STATIC, DIST_ALERT_HLS) are **intermediate inputs** or **static layers** where duplicate detection is sufficient for operational monitoring. See README.md "Why Some Products Don't Have Accountability" for detailed explanation.

## Command Reference

### Check Version

```bash
opera-audit version
```

### Duplicate Detection

#### Single Product

```bash
opera-audit duplicates DSWX_HLS --days-back 7
opera-audit duplicates RTC_S1 --start 2026-01-01 --end 2026-01-21 --venue PROD
opera-audit duplicates DSWX_HLS --days-back 7 --no-save     # stdout only, skip saving
opera-audit duplicates DSWX_HLS --days-back 7 --quiet       # minimal output (for cron)
```

#### All Products at Once

```bash
opera-audit duplicates --days-back 7
```

Example output:
```
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━┓
┃ Product        ┃   Total ┃ Duplicates ┃  Rate ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━┩
│ DSWX_HLS       │  80,392 │        871 │ 1.08% │
│ RTC_S1         │ 161,019 │      3,701 │ 2.30% │
│ CSLC_S1        │  12,354 │         54 │ 0.44% │
│ DSWX_S1        │  43,501 │        131 │ 0.30% │
│ DISP_S1        │       0 │          0 │ 0.00% │
│ TROPO          │       7 │          0 │ 0.00% │
│ DIST_ALERT_HLS │  79,525 │          8 │ 0.01% │
└────────────────┴─────────┴────────────┴───────┘
```

#### DISP-S1 End-Conflict Detection (Gerald)

```bash
opera-audit duplicates DISP_S1 --days-back 30 --check-end-conflicts
```

Reports are saved to `./output/reports/` by default. Use `--no-save` to disable.

Detects cases where the same frame+end-date has multiple begin-dates (conflicting time-series segments).
This is Gerald's "end-conflict" detection algorithm from `detect_cmr_duplicates_for_disp_s1.py`.

**Key characteristics:**
- Groups by `(frame_id, end_dt)` only (polarization NOT included)
- Identifies conflicts when same frame+end-date has different begin-dates
- Original: `opera-sds-pcm/tools/ops/cmr_audit/detect_cmr_duplicates_for_disp_s1.py`

#### Memory-Efficient Mode

```bash
opera-audit duplicates RTC_S1 --days-back 30 --chunk-days 7
```

Processes granules in resumable time chunks and checkpoints stable granule IDs
to SQLite. CMR records repeated at adjacent inclusive boundaries are upserted,
not counted twice.

#### Shared checkpoints

Duplicate detection, every accountability strategy, and burst coverage use the
same checkpoint layout under `<output-dir>/checkpoints/`. The default chunk is
30 days; DSWx-S1 operators with limited RAM can select a smaller window:

```bash
opera-audit accountability DSWX_S1 \
    --start 2024-08-21 --end 2026-02-01 \
    --chunk-days 7 --mgrs-db /path/to/MGRS_tile_collection.sqlite
```

Useful controls:

```text
--chunk-days N          Days in each temporal query
--no-chunking           Use one query range (accountability/burst coverage)
--checkpoint-dir PATH   Override OUTPUT_DIR/checkpoints
--resume / --no-resume  Reuse or discard completed chunks
--keep-checkpoints      Preserve checkpoint state after success
```

Failed and interrupted runs retain their checkpoints. Successful runs clean
them up unless `--keep-checkpoints` is supplied. Static products use one
non-temporal chunk because they have no meaningful date partition.

#### GRQ (OpenSearch) Source

```bash
# Single product from GRQ
opera-audit duplicates DSWX_HLS --venue GRQ --grq-url https://grq.example.com \
    --start 2026-01-01 --end 2026-01-21

# All products from GRQ
opera-audit duplicates --venue GRQ --grq-url https://grq.example.com --days-back 7
```

Requires `opensearch-py` (`pip install -e ".[grq]"`). Each product's GRQ index pattern is configured via the `grq_index` field in `config.yaml`.

### SLC Burst-Level Coverage Audit

```bash
# Basic burst coverage audit (CSLC-S1 + RTC-S1)
opera-audit burst-coverage \
    --start 2026-02-01T00:00:00Z --end 2026-02-07T23:59:59Z \
    --geojson north_america.geojson --save

# CSLC-S1 only, save to custom directory
opera-audit burst-coverage \
    --start 2026-02-01T00:00:00Z --end 2026-02-07T23:59:59Z \
    --geojson north_america.geojson --no-do-rtc \
    --save --output-dir /path/to/output

# Low-memory mode for long date ranges (streams to JSONL)
opera-audit burst-coverage \
    --start 2026-01-01T00:00:00Z --end 2026-06-30T23:59:59Z \
    --geojson north_america.geojson \
    --low-memory --output results.jsonl --chunk-days 30

# Multiple polarizations
opera-audit burst-coverage \
    --start 2026-02-01T00:00:00Z --end 2026-02-07T23:59:59Z \
    --geojson north_america.geojson --polarizations VV,VH --save

# Re-query selected dates while reusing all other cached data
opera-audit burst-coverage \
    --start 2026-02-01T00:00:00Z --end 2026-02-07T23:59:59Z \
    --geojson north_america.geojson \
    --recheck-dates-file dates-to-recheck.txt
```

When `--save` is used, results are written to
`<output-dir>/reports/burst_coverage/<timestamp>.json` and automatically
picked up by the dashboard's Burst Coverage tab.

Replaces the deprecated `cmr_audit_slc.py`. Requires `shapely` (`pip install -e ".[burst_coverage]"`) and EDL credentials.

### Accountability Analysis

#### DSWX_HLS

```bash
opera-audit accountability DSWX_HLS --days-back 7
```

Example output:
```
┏━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ Metric                ┃   Count ┃
┡━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━┩
│ Expected HLS Granules │  79,298 │
│ Matched DSWx Granules │  79,296 │
│ Missing DSWx Outputs  │       2 │
│ Accountability Rate   │ 100.00% │
└───────────────────────┴─────────┘
```

#### DSWX_S1 (requires MGRS tile DB)

```bash
# Pass DB path explicitly
opera-audit accountability DSWX_S1 --days-back 7 --mgrs-db /path/to/MGRS_tile_collection.sqlite

# Or set environment variable
export OPERA_MGRS_DB=/path/to/MGRS_tile_collection.sqlite
opera-audit accountability DSWX_S1 --days-back 7

# Optional: retain the raw four-stage result without real-coverage filtering
opera-audit accountability DSWX_S1 --days-back 7 \
    --mgrs-db /path/to/MGRS_tile_collection.sqlite \
    --no-coverage-validation
```

The default fifth stage checks whether each tile-set/cycle/sensor bucket had
enough real RTC burst coverage to trigger DSWx-S1. It writes validated,
dropped, and reduced recovery-candidate reports. The MGRS tile-collection
SQLite DB is available from JPL Artifactory or the ADT package repo.

#### DIST_S1 (Kevin - ISO-XML extraction)

```bash
# Basic DIST-S1 accountability (CMR-only)
opera-audit accountability DIST_S1 --days-back 7

# With burst DB for cross-checking
opera-audit accountability DIST_S1 --days-back 7 --burst-db /path/to/burst_db.json

# Tune download concurrency and retries
opera-audit accountability DIST_S1 --days-back 7 \
    --max-concurrent 10 --max-retries 3
```

**Strategy details:**
- Extracts RTC inputs from DIST-S1 ISO-XML metadata (`PostRtcOperaIds` attribute)
- Uses namespace-aware XPath queries: `.//eos:AdditionalAttribute`
- Maps RTC burst IDs to MGRS tiles using burst-to-products DB (optional)
- Original: `opera-sds-pcm/tools/ops/cmr_audit/cmr_audit_dist_s1.py`

#### TROPO (Chris - date_count strategy)

```bash
opera-audit accountability TROPO --days-back 30
```

**Strategy details:**
- Counts granules by `BeginningDateTime` (date only, no time)
- Flags dates with fewer than expected granules (threshold: 4 per day)
- Original: `opera-sds-pcm/tools/ops/cmr_audit/cmr_audit_tropo.py`

#### DISP_S1_STATIC (Chris - db_based strategy)

```bash
# Uses pre-configured sample database
opera-audit accountability DISP_S1_STATIC --days-back 30

# Or override with custom database
opera-audit accountability DISP_S1_STATIC --days-back 30 \
    --db-path /path/to/your-frame-to-burst.json
```

**Strategy details:**
- Maps frames to expected bursts using external database
- Sample database included: `data/opera-s1-disp-frame-to-burst-sample.json`
- Pre-configured in `config.yaml`, no `--db-path` needed for testing
- For production: obtain full frame-to-burst DB from opera-sds-pcm or ADT package

#### DISP_S1 (Gerald + Chris - delegated_validator)

```bash
opera-audit accountability DISP_S1 --days-back 7
```

**Strategy details:**
- Delegates to external validator: `opera_validator.opv_disp_s1.validate_disp_s1`
- Requires validator configuration in `config.yaml`
- Falls back to basic granule counting if validator not available
- Original: Gerald's end-conflicts + Chris's delegated pattern

#### All Enabled Products

```bash
opera-audit accountability --days-back 7
```

#### Recovery Files

```bash
# Generate a text recovery file listing missing granule IDs
opera-audit accountability DSWX_HLS --days-back 7 --recovery-format txt

# JSON format
opera-audit accountability DIST_S1 --days-back 7 --recovery-format json
```

Recovery files are compatible with `daac_data_subscriber.py` for automated re-processing.

### Launch Dashboard

```bash
opera-audit dashboard
# or with a custom data directory:
opera-audit dashboard --data-dir /path/to/output
```

Opens browser to `http://localhost:8501` showing:
- **Overview** — health metrics across all products (duplicates, accountability, and burst coverage)
- **Duplicates** — charts and tables per product
- **Accountability** — missing granule lists, rates, strategy-specific panels
- **Burst Coverage** — per-product coverage %, found vs missing charts, missing burst detail tables with export

## Output Files

Reports are saved under `./output/reports/{duplicates,accountability,burst_coverage}/`.

### Duplicates Report JSON

`./output/reports/duplicates/DSWX_HLS/2026-05-12.json`

```json
{
  "report_metadata": {
    "generated_at": "2026-05-12T10:30:00",
    "product_type": "DSWX_HLS",
    "venue": "PROD",
    "report_type": "duplicates"
  },
  "results": {
    "total": 80392,
    "unique": 79521,
    "duplicates": 871,
    "duplicate_list": ["..."],
    "by_date": {
      "2026-05-05": {"total": 11200, "unique": 11080, "duplicates": 120}
    }
  }
}
```

### Accountability Report (DSWX_S1 nested layout)

`./output/reports/accountability/DSWX_S1/2026-05-12/summary.json`

```json
{
  "metadata": {"generated_at": "2026-05-12T10:30:00"},
  "rtc_surveyed": 161019,
  "dswx_surveyed": 43501,
  "filtered_rtc_count": 157000,
  "used_rtc_count": 155000,
  "missing_count": 2000,
  "missing": ["..."],
  "expected": 157000,
  "actual": 155000,
  "coverage_validation_enabled": true,
  "coverage_threshold": 4,
  "coverage_valid_count": 1013,
  "coverage_dropped_count": 29,
  "recovery_candidate_count": 742,
  "recovery_candidates": ["..."]
}
```

Sibling files include `rtc_survey.json`, `dswx_survey.json`,
`missing_rtc_products.json`, `rtc_to_dswx_map.json`,
`missing_mgrs_sets_by_coverage.json`, and
`missing_rtc_mgrs_set_mappings_with_sufficient_coverage_reduced.json`.

### Burst Coverage Report

`./output/reports/burst_coverage/2026-05-12_10-30-00.json`

```json
{
  "metadata": {
    "start_datetime": "2026-05-05T00:00:00+00:00",
    "end_datetime": "2026-05-12T23:59:59+00:00",
    "geojson": "north_america.geojson",
    "slc_count": 42,
    "total_bursts_raw": 200,
    "unique_bursts": 150,
    "polarizations": ["VV"],
    "generated_at": "2026-05-12T10:30:00"
  },
  "products": {
    "CSLC-S1": {
      "expected_count": 150,
      "found_count": 145,
      "missing_count": 5,
      "coverage_percent": 96.67,
      "found": ["..."],
      "missing": [
        {
          "burst_pattern": "T064-135524-IW1",
          "acquisition_time": "2026-05-07T12:00:00",
          "platform": "S1A",
          "polarization": "VV",
          "slc_native_id": "S1A_IW_SLC__1SDV_..."
        }
      ]
    }
  }
}
```

## Testing

### Run Unit Tests (fast, no network)

```bash
python -m pytest tests/ -v -m "not slow"
```

### Run Integration Tests (requires CMR access)

```bash
python -m pytest tests/ -v -m integration
```

### Tests That Require the MGRS Tile DB

The following tests create a **temporary in-memory SQLite DB** as a fixture, so they do **not** require the real MGRS tile DB file:

- `tests/test_dswx_s1_strategy.py` — tile-set resolution, pipeline smoke test
- `tests/test_cli_dispatch.py` — CLI `--mgrs-db` argument passing (mocked)

The **integration test** `test_dswx_s1_accountability_pipeline_end_to_end` in `tests/test_cmr_integration.py` requires the real MGRS DB at runtime (via `OPERA_MGRS_DB` or the bundled path).

The `accountability` command for DSWX_S1 (when running all products) also requires the DB:

```bash
export OPERA_MGRS_DB=/path/to/MGRS_tile_collection.sqlite
opera-audit accountability --days-back 7
```

## Integration with Cron

### Daily Duplicate Check (all products)
```bash
0 2 * * * cd /path/to/opera-audit && source .venv/bin/activate && opera-audit duplicates --days-back 1 --quiet >> /var/log/opera-audit.log 2>&1
```

### Weekly Accountability Check
```bash
0 3 * * 1 cd /path/to/opera-audit && source .venv/bin/activate && opera-audit accountability --days-back 7 --quiet >> /var/log/opera-audit.log 2>&1
```

## Strategy Override Examples

You can override the default accountability strategy for any product:

```bash
# Use forward_map strategy instead of dswx_hls for DSWX_HLS
opera-audit accountability DSWX_HLS --strategy forward_map --days-back 7

# Use date_count for a custom product
opera-audit accountability CUSTOM_PRODUCT --strategy date_count --days-back 30
```

**Available strategies:**
- `dswx_hls` — HLS→DSWx mapping with L9 cutoff (Chris)
- `dswx_s1` — 5-step RTC→DSWx pipeline with real-coverage validation (Riley)
- `dist_s1` — ISO-XML RTC extraction (Kevin)
- `forward_map` — Query inputs, generate expected outputs (Chris)
- `date_count` — Count by date, flag low counts (Chris)
- `delegated_validator` — External validator (Chris)
- `db_based` — Database-driven mapping (Chris)

## Python API Usage

```python
from opera_accountability import CONFIG
from opera_accountability.cmr import query_cmr, query_cmr_by_short_name
from opera_accountability.duplicates import (
    detect_duplicates,
    detect_disp_s1_end_conflicts,
    get_granules_from_grq,
)
from opera_accountability.reports import save_reports
from datetime import datetime, timedelta

end_date = datetime.now()
start_date = end_date - timedelta(days=7)

# --- Duplicates from CMR (by ccid) - Riley ---
ccid = CONFIG["products"]["DSWX_HLS"]["ccid"]["PROD"]
granules = query_cmr(ccid, start_date, end_date, "PROD")
results = detect_duplicates(granules, "DSWX_HLS")
print(f"Found {results['duplicates']} duplicates out of {results['total']}")

# --- Duplicates from GRQ (OpenSearch) - Riley ---
grq_index = CONFIG["products"]["DSWX_HLS"]["grq_index"]
granules = get_granules_from_grq(
    grq_url="https://grq.example.com",
    index=grq_index,
    product="DSWX_HLS",
    start=start_date,
    end=end_date,
)
results = detect_duplicates(granules, "DSWX_HLS")

# --- Duplicates (by short_name, e.g. DIST_S1) - Kevin ---
coll = CONFIG["products"]["DIST_S1"]["collection"]["PROD"]
granules = query_cmr_by_short_name(coll["short_name"], coll["provider"], start_date, end_date)
results = detect_duplicates(granules, "DIST_S1")

# --- DISP-S1 end-conflicts - Gerald ---
ccid = CONFIG["products"]["DISP_S1"]["ccid"]["PROD"]
granules = query_cmr(ccid, start_date, end_date, "PROD")
results = detect_disp_s1_end_conflicts(granules)
print(f"Found {results['conflict_groups']} end-conflict groups")

# --- Accountability with strategy - Chris ---
from opera_accountability.strategies.forward_map import ForwardMapStrategy
strategy = ForwardMapStrategy("DSWX_HLS")
results = strategy.analyze(start_date, end_date, "PROD")

# --- Save reports ---
files = save_reports(results, "./output", "DSWX_HLS", "accountability", "PROD",
                     start_date=start_date, end_date=end_date)
```

## Consolidation Reference

For detailed documentation of the consolidation:
- **CONSOLIDATION_MAP.md** — Original tool locations, file structure comparisons, migration notes
- **README.md** — Consolidation history by contributor

### Original Tool Locations

**Riley:**
- `opera-sds-ops/duplicates/duplicate_check.py` → `src/opera_accountability/duplicates.py`
- `opera-sds-ops/accountability_tools/dswx_s1/` → `src/opera_accountability/strategies/dswx_s1/`

**Gerald:**
- `opera-sds-pcm/tools/ops/cmr_audit/detect_cmr_duplicates_for_disp_s1.py` → `src/opera_accountability/duplicates.py::detect_disp_s1_end_conflicts()`

**Chris:**
- `opera-sds-pcm/tools/ops/cmr_audit/cmr_audit_hls.py` → `src/opera_accountability/strategies/{dswx_hls,forward_map}.py`
- `opera-sds-pcm/tools/ops/cmr_audit/cmr_audit_tropo.py` → `src/opera_accountability/strategies/date_count.py`
- `opera-sds-pcm/tools/ops/cmr_audit/cmr_client.py` → `src/opera_accountability/cmr.py`
  (`cmr_async.py` remains as a backward-compatible import facade)

**Kevin:**
- `opera-sds-pcm/tools/ops/cmr_audit/cmr_audit_dist_s1.py` → `src/opera_accountability/strategies/dist_s1/`

## Logging

All modules emit structured log lines via Python's `logging` library with
module-level traceability (`%(name)s`):

```
2026-07-15 08:25:47 [INFO] opera_accountability.cli: === Duplicate detection ALL products START (venue=PROD, 2024-01-23 .. 2024-02-08) ===
2026-07-15 08:25:51 [INFO] opera_accountability.cmr: CMR page 1 (PROD, ccid=C2617126679-POCLOUD): 2000 cumulative granules (4.2s)
2026-07-15 08:26:36 [INFO] opera_accountability.checkpoint: [granule_ids] chunk 1/6 DONE: 18363 fetched (cumulative stored: 18363)
2026-07-15 08:30:31 [INFO] opera_accountability.duplicates: Found 0 duplicate granule IDs out of 102220 granules (0.0%)
```

Progress is logged at granular intervals:
- **CMR pagination**: pages 1-3, then every 10th page, plus a final summary
- **Checkpoint chunks**: each chunk start/completion with cumulative counts
- **Million-record loops**: every 50k-100k records (configurable per module)
- **Pipeline stages**: start/end banners with elapsed time and metrics

Use `--verbose` for DEBUG-level output or `--quiet` for WARNING-only.

## Troubleshooting

### CMR Connection Issues
```bash
opera-audit duplicates DSWX_HLS --days-back 1 --verbose
```

### Pattern Not Matching
```python
from opera_accountability import CONFIG
import re

pattern = re.compile(CONFIG['products']['DSWX_HLS']['pattern'])
test_id = 'OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0'
match = pattern.match(test_id)
print(match.groupdict() if match else "No match!")
```

### Check Configuration
```python
from opera_accountability import CONFIG
for name, prod in CONFIG['products'].items():
    acc = prod.get('accountability', {})
    print(f"{name}: strategy={acc.get('strategy', 'n/a')}, enabled={acc.get('enabled', False)}")
```

### Check Which Contributor Code Is Used
```python
# See README.md or CONSOLIDATION_MAP.md for detailed contributor mappings
from opera_accountability.duplicates import detect_duplicates         # Riley
from opera_accountability.duplicates import get_granules_from_grq     # Riley (GRQ)
from opera_accountability.duplicates import detect_disp_s1_end_conflicts  # Gerald
from opera_accountability.strategies.forward_map import ForwardMapStrategy  # Chris
from opera_accountability.strategies.dist_s1 import run as run_dist_s1     # Kevin
```

### Earthdata Login (EDL) Authentication

Required for DIST-S1 accountability and burst-coverage audits:

```bash
# Option 1: Environment variable
export EARTHDATA_TOKEN="your-bearer-token"

# Option 2: ~/.netrc file
machine urs.earthdata.nasa.gov
    login your_username
    password your_password
```
