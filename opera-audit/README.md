# OPERA Accountability Framework

A Python toolkit for OPERA SDS operators that detects duplicate granules and
performs accountability analysis (input → output mapping) across every OPERA
product line, with a Streamlit dashboard for visualizing results.

## Features

- **Duplicate detection** for every OPERA product (regular dup-set detection plus
  a DISP-S1 "end-conflict" mode for frames with the same end-date but different
  begin-dates).
- **Accountability strategies** — pick the right one per product:
  - `dswx_hls` — HLS S30/L30 inputs → DSWx-HLS outputs (with L9 cutoff filter).
  - `dswx_s1` — 4-step RTC-S1 → DSWx-S1 pipeline (CMR survey → input mapping →
    MGRS tile-set resolution → cycle/sensor bucket expansion).
  - `dist_s1` — RTC-S1 → DIST-S1 mapping driven by DIST-S1 ISO-XML metadata
    (optionally augmented by the opera-sds-pcm burst DB).
  - Generic Phase-3 strategies for new products: `forward_map`, `date_count`,
    `delegated_validator`, `db_based`.
- **Output formats**: structured JSON (full report), plain-text granule lists,
  and human-readable summaries.
- **CLI** built on Typer + Rich (`opera-audit duplicates`, `accountability`,
  `accountability-all`, `dashboard`).
- **Streamlit dashboard** with per-product panels, status pills
  (healthy / warning / critical), Altair charts, and per-artifact JSON
  previews.

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

**Run duplicate detection for all products (no product argument):**
```bash
opera-audit duplicates --start 2026-02-01 --end 2026-02-07 --save
```

**Note:** Memory-efficient mode is now **enabled by default**. It's automatically disabled for:
- Static collections
- DISP-S1 with `--check-end-conflicts`
- Products without CCID configured (falls back to short_name query)

To manually disable:
```bash
opera-audit duplicates RTC_S1 --start 2026-01-01 --end 2026-03-01 --no-memory-efficient --save
```

**Run accountability analysis for a specific product:**
```bash
# DSWX-HLS (default strategy: dswx_hls)
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

**Run accountability for all enabled products:**
```bash
opera-audit accountability-all --start 2026-02-01 --end 2026-02-07 --save
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

## Supported Products

| Product           | Duplicates | Accountability strategy |
| ----------------- | :--------: | ----------------------- |
| `DSWX_HLS`        |     ✅     | `dswx_hls`              |
| `RTC_S1`          |     ✅     | —                       |
| `CSLC_S1`         |     ✅     | —                       |
| `DSWX_S1`         |     ✅     | `dswx_s1` (needs MGRS DB) |
| `DIST_S1`         |     ✅     | `dist_s1`               |
| `DISP_S1`         |     ✅     | `delegated_validator`   |
| `TROPO`           |     ✅     | `date_count`            |
| `DIST_ALERT_HLS`  |     ✅     | —                       |
| `CSLC_S1_STATIC`  |     ✅     | —                       |
| `RTC_S1_STATIC`   |     ✅     | —                       |

Products without an accountability strategy in the table are still surveyed
by duplicate detection.

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
└── accountability/
    └── DSWX_HLS/
        ├── 2026-05-11.json
        ├── 2026-05-11_missing.txt
        └── 2026-05-11_summary.txt
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

The dashboard's `load_reports` helper picks up both layouts automatically and
always surfaces the newest report per product.

## Dashboard

```bash
opera-audit dashboard --data-dir ./output
```

### Data Directory Structure

The dashboard's `--data-dir` argument should point to the **parent directory** 
containing a `reports/` subdirectory (created by running commands with `--save`). 

For example, if you ran:
```bash
opera-audit duplicates DSWX_HLS --save --output-dir /my/data
```

Then launch the dashboard with:
```bash
opera-audit dashboard --data-dir /my/data
```

The dashboard expects this structure inside the data directory:
```
/my/data/
└── reports/
    ├── duplicates/
    │   └── PRODUCT_NAME/
    │       └── YYYY-MM-DD.json
    └── accountability/
        └── PRODUCT_NAME/
            ├── YYYY-MM-DD.json          # flat layout (most products)
            └── YYYY-MM-DD/              # nested layout (DSWX_S1, DIST_S1)
                └── summary.json
```

If the dashboard shows "No reports yet", verify that:
1. You ran duplicate/accountability commands with `--save`
2. The `--output-dir` used matches the `--data-dir` you're pointing to
3. The `reports/` subdirectory exists inside your data directory

### Dashboard Tabs

- **Overview** — shadcn metric cards, an Altair bar chart of duplicate rates
  by product, and per-product summary tables for both duplicates and
  accountability with status pills.
- **Duplicates** — per-product detail view with a by-date Altair chart, the
  duplicate granule-ID preview, and JSON / TXT export popovers. DISP-S1
  reports automatically render the end-conflict view when `--check-end-conflicts`
  was used.
- **Accountability** — strategy-aware panels: dedicated UIs for
  `dswx_hls`, `dswx_s1` (tile-set / cycle / sensor breakdown + sibling
  artifact previews) and `dist_s1` (per tile/time-group rows), plus a
  generic panel for `date_count`, `delegated_validator`, `db_based`, and
  `forward_map`.

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
- `strategies/dswx_hls/` — DSWX-HLS accountability (HLS input mapping).
- `strategies/dswx_s1/` — DSWX-S1 accountability (4-step RTC → DSWx pipeline).
- `strategies/dist_s1/` — DIST-S1 accountability (ISO-XML pipeline).
- `strategies/{forward_map,date_count,delegated_validator,db_based}.py` —
  Phase-3 generic strategies for new products.

## Credits

Consolidated from existing code by:
- Riley Kuttruff (duplicate detection, accountability mapping)
- Alvin Nguyen (CMR audit wrapper)

## License

Apache 2.0
