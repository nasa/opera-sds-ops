# OPERA Accountability Framework

A simple Python tool for detecting duplicate OPERA granules and analyzing accountability (input-to-output mapping) for DSWX_HLS products.

## Features

- **Duplicate Detection**: Identifies duplicate granules across all OPERA products
- **Accountability Analysis**: Maps HLS inputs to DSWx-HLS outputs to find missing products
- **Multiple Output Formats**: JSON (full data), text (granule lists), summary (human-readable)
- **CLI Interface**: Simple command-line tool using Typer with Rich output
- **Dashboard**: Streamlit web dashboard for visualization

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
cd opera-accountability
pip install -e .

# Or install with test dependencies
pip install -e ".[test]"
```

### Usage

**Check for duplicates (last 7 days):**
```bash
opera-audit duplicates DSWX_HLS --days-back 7
```

**Check for duplicates (specific date range):**
```bash
opera-audit duplicates RTC_S1 --start 2026-01-01 --end 2026-01-21
```

**Run accountability analysis:**
```bash
opera-audit accountability --days-back 30
```

**Launch dashboard:**
```bash
opera-audit dashboard
# Opens browser to http://localhost:8501
```

**Show version:**
```bash
opera-audit version
```

## Supported Products

- **DSWX_HLS** - Dynamic Surface Water Extent (HLS)
- **RTC_S1** - Radiometric Terrain Corrected (Sentinel-1)
- **CSLC_S1** - Coregistered Single Look Complex (Sentinel-1)
- **DSWX_S1** - Dynamic Surface Water Extent (Sentinel-1)
- **DISP_S1** - Displacement (Sentinel-1)

## Output

Reports are saved to `./output/reports/` with the following structure:

```
output/
├── reports/
│   ├── duplicates/
│   │   ├── DSWX_HLS/
│   │   │   ├── 2026-01-22.json          # Full report
│   │   │   ├── 2026-01-22.txt           # List of duplicates
│   │   │   └── 2026-01-22_summary.txt   # Human-readable summary
│   │   ├── RTC_S1/
│   │   └── ...
│   └── accountability/
│       └── DSWX_HLS/
│           ├── 2026-01-22.json
│           ├── 2026-01-22_missing.txt
│           └── 2026-01-22_summary.txt
```

## Configuration

Edit `config.yaml` to:
- Adjust CMR settings (URL, timeout, page size)
- Modify product patterns and unique field definitions
- Configure output directory

## Testing

```bash
# Run fast unit tests (default - excludes integration tests)
pytest tests/ -v

# Run specific test file
pytest tests/test_duplicates.py -v

# Run integration tests (compares results with CMR - slow, ~5-10 minutes)
pytest tests/test_cmr_integration.py -v -m integration

# Run ALL tests including integration tests
pytest tests/ -v -m ""

# Exclude integration tests explicitly
pytest tests/ -v -m "not integration"
```

### Integration Tests

Integration tests in `tests/test_cmr_integration.py` verify that opera-audit results match independent CMR queries:
- **Duplicate detection**: Compares all duplicates found by opera-audit with CMR analysis
- **Accountability**: Compares all missing products found by opera-audit with CMR analysis

**Note:** These tests are slow (5-10 minutes) and require CMR access. They are skipped by default during normal test runs.

**Adding new test cases:** Edit `TEST_CASES` dict in `tests/test_cmr_integration.py` - no code changes needed!

## Development

This package follows a simple structure:
- `src/opera_accountability/` - Main package code
- `tests/` - Test files and fixtures
- `config.yaml` - Configuration

Key files:
- `cmr.py` - CMR client with retry and pagination
- `duplicates.py` - Duplicate detection logic
- `accountability.py` - Accountability analysis for DSWX_HLS
- `reports.py` - Report generation in multiple formats
- `cli.py` - Command-line interface
- `dashboard.py` - Streamlit dashboard

## Credits

Consolidated from existing code by:
- Riley Kuttruff (duplicate detection, accountability mapping)
- Alvin Nguyen (CMR audit wrapper)

## License

Apache 2.0
