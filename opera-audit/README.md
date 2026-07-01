# OPERA Accountability Framework

A unified Python toolkit for OPERA SDS operators that detects duplicate granules and
performs accountability analysis (input → output mapping) across every OPERA
product line, with a Streamlit dashboard for visualizing results.

**This package consolidates accountability and duplicate detection tools from 4 contributors
across the OPERA SDS team into a single, unified framework.** See `CONSOLIDATION_MAP.md`
for detailed documentation of the consolidation process.

## Features

- **Duplicate detection** for every OPERA product:
  - Regular duplicate detection with monthly/daily aggregation. *[Riley]*
  - DISP-S1 "end-conflict" mode for frames with the same end-date but different
    begin-dates. *[Gerald]*
- **Accountability strategies** — pick the right one per product:
  - `dswx_hls` — HLS S30/L30 inputs → DSWx-HLS outputs (with L9 cutoff filter). *[Chris]*
  - `dswx_s1` — 4-step RTC-S1 → DSWx-S1 pipeline (CMR survey → input mapping →
    MGRS tile-set resolution → cycle/sensor bucket expansion). *[Riley]*
  - `dist_s1` — RTC-S1 → DIST-S1 mapping driven by DIST-S1 ISO-XML metadata
    (optionally augmented by the opera-sds-pcm burst DB). *[Kevin]*
  - Generic strategies for new products:
    - `forward_map` — Query inputs first, generate expected output patterns. *[Chris]*
    - `date_count` — Count granules by date, flag dates below threshold. *[Chris]*
    - `delegated_validator` — Delegate to external validator module. *[Chris]*
    - `db_based` — Map using external frame/burst database. *[Chris]*
- **Output formats**: structured JSON (full report), plain-text granule lists,
  and human-readable summaries.
- **Burst-level coverage audit** for CSLC-S1 and RTC-S1: query expected
  bursts from the ASF catalog, check CMR for matching products, report
  coverage gaps with streaming JSONL support for long date ranges. *[Gerald]*
- **CLI** built on Typer + Rich (`opera-audit duplicates`, `accountability`,
  `burst-coverage`, `dashboard`).
- **Streamlit dashboard** with per-product panels, status pills
  (healthy / warning / critical), Altair charts, and per-artifact JSON
  previews — including a dedicated **Burst Coverage** tab.

## Quick Start

### Setting Up a Virtual Environment

It's recommended to use a virtual environment to isolate project dependencies. Choose one of the following methods:

**Using venv (built-in):**
```bash
# Create virtual environment
python3 -m venv .venv

# Activate on macOS/Linux
source .venv/bin/activate

# Activate on Windows
.venv\Scripts\activate
```

**Using conda:**
```bash
# Create virtual environment with Python 3.10+
conda create -n opera-accountability python=3.12

# Activate environment
conda activate opera-accountability
```

**Using uv (fast alternative):**
```bash
# Install uv if not already installed
pip install uv

# Create virtual environment
uv venv

# Activate on macOS/Linux
source .venv/bin/activate

# Activate on Windows
.venv\Scripts\activate
```

### Installation

```bash
# Install in development mode
cd opera-audit
pip install -e .

# Or install with test dependencies
pip install -e ".[test]"
```

### Usage

**Check for duplicates (last 7 days):**
```bash
opera-audit duplicates DSWX_HLS --days-back 7
```

**Check for duplicates (specific date range, save to files):**
```bash
opera-audit duplicates RTC_S1 --start 2026-01-01 --end 2026-01-21 --save
```

**Check for DISP-S1 end-conflicts (same frame + end-date, different begin-date):**
```bash
opera-audit duplicates DISP_S1 --check-end-conflicts --start 2026-02-01 --end 2026-02-07 --save
```

**Memory-efficient mode for very large windows:**
```bash
opera-audit duplicates RTC_S1 --start 2026-01-01 --end 2026-03-01 --memory-efficient --save
```

**Check for duplicates from GRQ (OpenSearch) instead of CMR:**
```bash
opera-audit duplicates DSWX_HLS --venue GRQ --grq-url https://grq.example.com \
    --start 2026-01-01 --end 2026-01-21 --save
```

**SLC burst-level coverage audit (replaces legacy cmr_audit_slc.py):**
```bash
opera-audit burst-coverage --start 2026-02-01 --end 2026-02-07 --save
```

**Run accountability analysis for a specific product:**
```bash
# DSWX-HLS (strategy: dswx_hls)
opera-audit accountability DSWX_HLS --start 2026-02-01 --end 2026-02-07 --save

# DSWX-S1 (4-step pipeline; requires an MGRS tile-collection SQLite DB)
opera-audit accountability DSWX_S1 \
    --start 2026-02-01 --end 2026-02-07 --save \
    --mgrs-db /path/to/MGRS_tile_collection_v0.3.sqlite

# DIST-S1 (ISO-XML pipeline; tune downloads if needed)
opera-audit accountability DIST_S1 \
    --start 2026-02-01 --end 2026-02-07 --save \
    --max-concurrent 10 --max-retries 3
```

**Sweep all products in one command:**
```bash
# All duplicate checks (omit product argument)
opera-audit duplicates --start 2026-02-01 --end 2026-02-07 --save

# All accountability strategies that are enabled in config.yaml (omit product argument)
opera-audit accountability --start 2026-02-01 --end 2026-02-07 --save
```

**Launch the dashboard:**
```bash
opera-audit dashboard
# Opens http://localhost:8501 and reads ./output by default
opera-audit dashboard --port 8502 --data-dir /path/to/output
```

**Show version:**
```bash
opera-audit version
```

### Required external resources

- **MGRS tile-collection SQLite DB** for `DSWX_S1` accountability. It is not
  bundled with this package (~55 MB). Obtain it from JPL Artifactory or the
  ADT package repo and supply its path via `--mgrs-db <path>` or the
  `OPERA_MGRS_DB` environment variable.
- **Burst DB JSON** (optional, `DIST_S1` only). Without it `opera-audit` runs
  in CMR-only RTC accountability mode. With it, the DIST-S1 strategy can
  cross-check RTC inputs against the burst DB. Provide via `--burst-db <path>`.
- **Earthdata Login (EDL)** credentials for DIST-S1 ISO-XML downloads. Set
  the `EARTHDATA_TOKEN` environment variable or configure `~/.netrc` with
  `machine urs.earthdata.nasa.gov` credentials. Required only when running
  DIST-S1 accountability or burst-coverage audits.

### Data source selection

By default, duplicate detection queries **CMR** (NASA's Common Metadata
Repository). Two alternative sources are supported:

| Source | `--venue` value | Extra args | Status |
| ------ | :-------------: | ---------- | ------ |
| CMR PROD | `PROD` (default) | — | ✅ Production |
| CMR UAT | `UAT` | — | ✅ Production |
| GRQ (OpenSearch) | `GRQ` | `--grq-url <url>` | ✅ New |
| TRQ | — | — | 🔮 Planned |

**GRQ** queries an on-premises OpenSearch cluster used by the SDS pipeline.
Install the optional dependency: `pip install opensearch-py` (or
`pip install -e ".[grq]"`). Each product has a `grq_index` field in
`config.yaml` that maps to its OpenSearch index pattern.

**TRQ** support is planned but not yet implemented (no public API available).

### Recovery files

Use `--recovery-format txt` (or `json`) with the `accountability` command to
generate recovery files listing missing product IDs. These can be fed into
downstream re-processing workflows.

## Supported Products

| Product           | Duplicates | Accountability strategy | Burst Coverage | Status | Source |
| ----------------- | :--------: | ----------------------- | :------------: | ------ | ------ |
| `DSWX_HLS`        |     ✅     | `dswx_hls` / `forward_map` | — | ✅ Production | Chris |
| `RTC_S1`          |     ✅     | —                       | ✅ | — | Riley |
| `CSLC_S1`         |     ✅     | —                       | ✅ | — | Riley |
| `DSWX_S1`         |     ✅     | `dswx_s1` (needs MGRS DB) | — | ✅ Production | Riley |
| `DIST_S1`         |     ✅     | `dist_s1`               | — | ✅ Production | Kevin |
| `DISP_S1`         |     ✅     | `delegated_validator` ⚠️ | — | ⚠️ Needs validator | Gerald + Chris |
| `TROPO`           |     ✅     | `date_count`            | — | ✅ Production | Chris |
| `DISP_S1_STATIC`  |     ✅     | `db_based`              | — | ✅ Production | Chris |
| `DIST_ALERT_HLS`  |     ✅     | —                       | — | — | Riley |
| `CSLC_S1_STATIC`  |     ✅     | —                       | — | — | Riley |
| `RTC_S1_STATIC`   |     ✅     | —                       | — | — | Riley |

**Legend:**
- ✅ Production: Fully functional and production-ready
- ⚠️ Needs validator: Requires external validator configuration (see Known Limitations)
- — : Accountability analysis not implemented (duplicate detection only)

### Why Some Products Don't Have Accountability

**5 products intentionally omit accountability analysis.** These products are either intermediate inputs, static layers, or derived alerts where duplicate detection alone provides sufficient operational monitoring.

---

#### Category 1: Intermediate Input Products
**Products:** `RTC_S1`, `CSLC_S1`

**Processing Chain:**
```
SLC (raw satellite data)
    ↓
CSLC_S1 (coregistered)
    ↓
RTC_S1 (radiometrically corrected)
    ↓
DSWX_S1 / DIST_S1 / DISP_S1 (science products)
```

**Why no accountability:**
- These are **preprocessing steps**, not final science deliverables
- Accountability targets **end-user products** that deliver science data
- For inputs, **preventing duplicate processing** is the critical concern
- Downstream product accountability (DSWx-S1, DIST-S1) validates that inputs were used correctly

**Technical note:** SLC → CSLC accountability would require complex burst ID parsing and frame mapping. This was partially prototyped by Chris but not completed due to lower operational priority compared to final product validation.

---

#### Category 2: Static Ancillary Layers
**Products:** `CSLC_S1_STATIC`, `RTC_S1_STATIC`

**Why no accountability:**
- Static layers are generated **once per location** (not time-series)
- No time-based input → output relationship exists
- These are reference datasets (e.g., layover/shadow masks, local incidence angle)
- **Duplicate detection ensures** the same static layer isn't published multiple times

---

#### Category 3: Downstream Alert Products
**Products:** `DIST_ALERT_HLS`

**Processing Chain:**
```
HLS (optical imagery)
    ↓
DSWX-HLS (surface water extent)
    ↓
DIST_ALERT_HLS (change detection alerts)
```

**Why no accountability:**
- Alerts are **event-driven**, not systematic processing of all inputs
- Complex triggering logic (threshold-based change detection)
- Accountability relationship is **indirect** — validated via DSWx-HLS accountability
- Duplicate detection catches operational issues (e.g., duplicate alert publications)

---

**Summary:** For these 5 products, **duplicate detection (100% coverage)** provides the essential operational monitoring. Full input→output accountability would add complexity without proportional operational value, as these products are either validated indirectly through downstream accountability or have non-standard processing relationships.

## Output Layout

Reports are written under `./output/reports/` (override with
`--output-dir`). Two layouts are emitted depending on the strategy:

**Flat layout** (most products + `dswx_hls`):
```
output/reports/
├── duplicates/
│   ├── DSWX_HLS/
│   │   ├── 2026-05-11.json          # full report (results wrapped under "results")
│   │   ├── 2026-05-11.txt           # newline-separated duplicate granule IDs
│   │   └── 2026-05-11_summary.txt   # human-readable summary
│   └── …                            # one folder per product
├── accountability/
│   └── DSWX_HLS/
│       ├── 2026-05-11.json
│       ├── 2026-05-11_missing.txt
│       └── 2026-05-11_summary.txt
└── burst_coverage/
    └── 2026-05-11_10-30-00.json     # timestamped burst-coverage report
```

**Nested layout** (used by the `dswx_s1` and `dist_s1` strategies — one
date directory per run, with sibling artifacts the dashboard renders):
```
output/reports/accountability/DSWX_S1/2026-05-11/
├── summary.json                       # topline + metadata (loader entry-point)
├── summary.txt
├── rtc_survey.json
├── dswx_survey.json
├── missing_rtc_products.json
├── rtc_to_dswx_map.json
├── missing_rtcs_to_tile_sets.json
└── missing_mgrs_set_cycle_indices.json
```

The dashboard's `load_reports` helper picks up all three report categories
(duplicates, accountability, burst coverage) automatically and always
surfaces the newest report per product.

## Dashboard

```bash
opera-audit dashboard --data-dir ./output
```

The dashboard has four tabs:

- **Overview** — shadcn metric cards, an Altair bar chart of duplicate rates
  by product, and per-product summary tables for duplicates, accountability,
  and burst coverage with status pills.
- **Duplicates** — per-product detail view with a by-date Altair chart, the
  duplicate granule-ID preview, and JSON / TXT export popovers. DISP-S1
  reports automatically render the end-conflict view when `--check-end-conflicts`
  was used.
- **Accountability** — strategy-aware panels: dedicated UIs for
  `dswx_hls`, `dswx_s1` (tile-set / cycle / sensor breakdown + sibling
  artifact previews) and `dist_s1` (per tile/time-group rows), plus a
  generic panel for `date_count`, `delegated_validator`, `db_based`, and
  `forward_map`.
- **Burst Coverage** — CSLC-S1 / RTC-S1 burst-level coverage audit results:
  coverage % bar chart, found vs missing stacked chart, missing burst
  detail tables with TXT / JSON export.

## Configuration

Edit `src/opera_accountability/config.yaml` to:
- Adjust CMR settings (URL, timeout, page size).
- Modify product patterns and unique-field definitions.
- Toggle each product's accountability strategy on/off.
- Tune product-specific knobs (DSWx-S1 sensor cutoffs, DIST-S1 ISO download
  concurrency, etc.).

## Testing

```bash
# Run fast unit tests (default - excludes slow integration tests)
pytest tests/ -v -m "not slow"

# Run a specific test file
pytest tests/test_duplicates.py -v

# Run integration tests (compares results with live CMR — slow, 5-10 min each)
pytest tests/test_cmr_integration.py -v -m integration

# Run ALL tests including slow ones
pytest tests/ -v -m ""
```

### Integration Tests

Integration tests in `tests/test_cmr_integration.py` verify that opera-audit
results match independent CMR queries:
- **Duplicate detection** — compares all duplicates found by opera-audit with
  a CMR cross-check for a fixed date window.
- **Accountability** — compares missing products across strategies.
- **End-to-end DSWx-S1** — runs the full 4-step pipeline against live CMR
  (requires `OPERA_MGRS_DB`; skipped automatically when unset).

**Note:** These tests are slow (5-10 minutes each) and require CMR access.
They are skipped by default during normal test runs.

**Adding new test cases:** Edit the `TEST_CASES` dict in
`tests/test_cmr_integration.py` — no code changes needed.

## Development

This package follows a simple `src/` layout:

- `src/opera_accountability/` — main package code (includes `config.yaml`).
- `tests/` — unit + integration tests.

Key files:

- `cmr.py` / `cmr_async.py` — CMR clients with retry and pagination.
- `duplicates.py` — duplicate detection (regular + DISP-S1 end-conflict mode).
- `reports.py` — JSON / text / summary report generation.
- `recovery_file.py` — recovery-file writers for missing products.
- `cli.py` — Typer-based CLI (`opera-audit …`).
- `dashboard.py` — Streamlit dashboard.
- `burst_coverage.py` / `slc_annotations.py` — Gerald’s SLC burst coverage.
- `strategies/dswx_hls/` — DSWX-HLS accountability (HLS input mapping).
- `strategies/dswx_s1/` — DSWX-S1 accountability (4-step RTC → DSWx pipeline).
- `strategies/dist_s1/` — DIST-S1 accountability (ISO-XML pipeline).
- `strategies/{forward_map,date_count,delegated_validator,db_based}.py` —
  Generic strategies (Chris) for new products.

## Known Limitations

### Generic Strategies (Chris)

The following strategies are **framework implementations** that work with appropriate configuration:

- ✅ **`date_count`** (TROPO): Fully functional for date-based accountability
- ✅ **`db_based`** (DISP_S1_STATIC): Functional when provided with frame-to-burst DB
- ⚠️ **`delegated_validator`** (DISP_S1): Requires external validator integration
- ⚠️ **`forward_map`**: Partially implemented (HLS extraction complete, SLC pending)

### DISP_S1 Delegated Validator

The `delegated_validator` strategy for DISP_S1 requires the external validator from opera-sds-pcm:
- **Validator location**: `opera-sds-pcm/report/opera_validator/opv_disp_s1.py`
- **To enable**: Edit `config.yaml` and set:
  ```yaml
  DISP_S1:
    accountability:
      delegated_validator:
        validator_module: "report.opera_validator.opv_disp_s1"
        validator_function: "validate_disp_s1"
        validator_path: "/path/to/opera-sds-pcm"
  ```
- **Without validator**: Falls back to basic granule counting (not true accountability)

### DISP_S1_STATIC DB-Based Strategy

The `db_based` strategy for DISP_S1_STATIC requires a frame-to-burst database:
- **DB format**: JSON file with frame-to-burst mappings and `is_north_america` flags
- **Sample DB included**: `data/opera-s1-disp-frame-to-burst-sample.json` (pre-configured in `config.yaml`)
- **Override DB path**: Use `--db-path` CLI option or edit `config.yaml`:
  ```yaml
  DISP_S1_STATIC:
    accountability:
      db_based:
        db_path: "path/to/your-frame-to-burst.json"
  ```
- **Production DB**: Obtain full frame-to-burst database from opera-sds-pcm or ADT package

### Forward-Map Strategy

The `forward_map` strategy has partial implementation:
- ✅ **HLS → DSWx-HLS**: Input extraction implemented
- ⚠️ **SLC → CSLC/RTC**: Placeholder only (requires burst ID parsing)
- **Usage**: Can be used with DSWX_HLS by overriding strategy:
  ```bash
  opera-audit accountability DSWX_HLS --strategy forward_map --start 2026-02-01 --end 2026-02-07
  ```

## Troubleshooting

### "No validator configured, performing basic analysis"
- This message appears when using `delegated_validator` without configuring an external validator
- Solution: Configure `validator_module` and `validator_function` in `config.yaml` (see Known Limitations)
- Alternative: The basic analysis provides granule counts but not frame-level validation

### "Database file not found" (db_based strategy)
- Solution: Provide `--db-path` pointing to the frame-to-burst JSON file
- Example: `opera-audit accountability DISP_S1_STATIC --db-path ./frame-to-burst.json --start 2024-01-01 --end 2024-01-31`

### MGRS database required for DSWX_S1
- Set `OPERA_MGRS_DB` environment variable or use `--mgrs-db` flag
- Obtain MGRS tile-collection SQLite database from JPL Artifactory

## Consolidation History

This package consolidates tools from 4 contributors:

### Riley
- **Duplicate detection** (`duplicates/duplicate_check.py`) — monthly/daily aggregation
- **DSWx-S1 accountability pipeline** (`accountability_tools/dswx_s1/`) — 4-step RTC→DSWx survey
- Source: `opera-sds-ops/duplicates/`, `opera-sds-ops/accountability_tools/dswx_s1/`

### Gerald
- **DISP-S1 end-conflict detection** — same frame+end-date, different begin-dates
- **SLC burst-level coverage audit** — CSLC-S1 / RTC-S1 burst coverage using ASF catalog + CMR
- Source: `opera-sds-pcm/tools/ops/cmr_audit/detect_cmr_duplicates_for_disp_s1.py`
- Source: `opera-sds-pcm/tools/ops/cmr_audit/cmr_audit_burst_coverage.py`
- Branch: `frame-states-via-cmr-audit`, `OPERA-2518`

### Chris
- **Multi-strategy suite** — `forward_map`, `date_count`, `delegated_validator`, `db_based`
- **Async CMR client** with exponential backoff
- **HLS/TROPO accountability** — forward-map and date-count strategies
- Source: `opera-sds-pcm/tools/ops/cmr_audit/cmr_audit_{hls,slc,tropo}.py`
- Source: `opera-sds-pcm/tools/ops/cmr_audit/cmr_client.py`

### Kevin
- **DIST-S1 ISO-XML tools** — extract RTC inputs from DIST-S1 metadata
- **Burst-to-tile MGRS mapping** — RTC burst ID extraction and tile mapping
- Source: `opera-sds-pcm/tools/ops/cmr_audit/cmr_audit_dist_s1.py`

**See `CONSOLIDATION_MAP.md` for detailed file mappings, CLI usage, and migration notes.**

## Credits

Contributing authors:
- **Riley** — duplicate detection, DSWx-S1 accountability pipeline
- **Gerald** — DISP-S1 end-conflict detection
- **Chris** — multi-strategy suite, async CMR client, validators
- **Kevin** — DIST-S1 ISO-XML tools, burst-to-tile mapping
- **Alvin Nguyen** — CMR audit wrapper

Consolidation and framework integration: This project

## License

Apache 2.0
