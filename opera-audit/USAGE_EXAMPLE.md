# Usage Examples

## Installation

```bash
cd opera-accountability
uv venv
source .venv/bin/activate
uv pip install -e .
```

## Command Examples

### 1. Check for Duplicates - Last 7 Days

```bash
opera-audit duplicates DSWX_HLS --days-back 7
```

**Output:**
```
╭─────────────────────────────────────────╮
│ OPERA Audit                             │
│                                         │
│ Duplicate Detection                     │
│ Product: DSWX_HLS                       │
│ Venue: PROD                             │
│ Date Range: 2026-01-15 to 2026-01-22   │
│ Output: ./output                        │
╰─────────────────────────────────────────╯

Querying CMR...
Retrieved 1,247 granules from CMR

Analyzing for duplicates...
Found 3 duplicates out of 1,247 granules (0.24%)

Saving reports...

┏━━━━━━━━━━━━━━━━━┳━━━━━━━┓
┃ Metric          ┃ Count ┃
┡━━━━━━━━━━━━━━━━━╇━━━━━━━┩
│ Total Granules  │ 1,247 │
│ Unique Granules │ 1,244 │
│ Duplicates      │     3 │
│ Duplicate Rate  │ 0.24% │
└─────────────────┴───────┘

Files created:
  json: ./output/reports/duplicates/DSWX_HLS/2026-01-22.json
  text: ./output/reports/duplicates/DSWX_HLS/2026-01-22.txt
  summary: ./output/reports/duplicates/DSWX_HLS/2026-01-22_summary.txt

Done!
```

### 2. Check Specific Date Range

```bash
opera-audit duplicates RTC_S1 --start 2026-01-01 --end 2026-01-21 --venue PROD
```

### 3. Check All Products (Future)

```bash
# Run for each product
for product in DSWX_HLS RTC_S1 CSLC_S1 DSWX_S1 DISP_S1; do
    opera-audit duplicates $product --days-back 7
done
```

### 4. Run Accountability Analysis

```bash
opera-audit accountability --days-back 30
```

**Output:**
```
╭─────────────────────────────────────────╮
│ OPERA Audit                             │
│                                         │
│ Accountability Analysis                 │
│ Product: DSWX_HLS                       │
│ Venue: PROD                             │
│ Date Range: 2025-12-23 to 2026-01-22   │
│ Output: ./output                        │
╰─────────────────────────────────────────╯

Querying CMR for DSWx-HLS...
Retrieved 15,380 granules from CMR

Querying CMR for HLS-S30...
Retrieved 22,145 granules from CMR

Querying CMR for HLS-L30...
Retrieved 18,932 granules from CMR

After L9 filtering: 41,077 HLS granules
Mapped DSWx to 15,380 unique HLS inputs
Found 142 HLS granules with no DSWx output

Analyzing accountability...
Saving reports...

┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┓
┃ Metric                 ┃  Count ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━┩
│ Expected HLS Granules  │ 41,077 │
│ Matched DSWx Granules  │ 40,935 │
│ Missing DSWx Outputs   │    142 │
│ Accountability Rate    │ 99.65% │
└────────────────────────┴────────┘

Files created:
  json: ./output/reports/accountability/DSWX_HLS/2026-01-22.json
  text: ./output/reports/accountability/DSWX_HLS/2026-01-22_missing.txt
  summary: ./output/reports/accountability/DSWX_HLS/2026-01-22_summary.txt

Done!
```

### 5. Launch Dashboard

```bash
opera-audit dashboard
```

Opens browser to `http://localhost:8501` showing:
- Overview with metrics across all products
- Duplicates page with charts and tables
- Accountability page with missing granule lists

### 6. Quiet Mode (for Cron Jobs)

```bash
opera-audit duplicates DSWX_HLS --days-back 7 --quiet
```

Only logs warnings/errors, minimal output.

### 7. Verbose Mode (for Debugging)

```bash
opera-audit duplicates DSWX_HLS --days-back 7 --verbose
```

Shows detailed logs including CMR queries, pattern matching, etc.

## Output Files

### Duplicates Report JSON
`./output/reports/duplicates/DSWX_HLS/2026-01-22.json`

```json
{
  "report_metadata": {
    "generated_at": "2026-01-22T10:30:00",
    "product_type": "DSWX_HLS",
    "venue": "PROD",
    "report_type": "duplicates"
  },
  "results": {
    "total": 1247,
    "unique": 1244,
    "duplicates": 3,
    "duplicate_list": [
      "OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0",
      "OPERA_L3_DSWx-HLS_T11SKA_20260116T183045Z_20260116T230000Z_S2A_30_v1.0",
      "OPERA_L3_DSWx-HLS_T12SUD_20260118T182045Z_20260118T235959Z_L9_30_v1.0"
    ],
    "by_date": {
      "2026-01-15": {"total": 415, "unique": 414, "duplicates": 1},
      "2026-01-16": {"total": 421, "unique": 420, "duplicates": 1},
      "2026-01-17": {"total": 411, "unique": 411, "duplicates": 0}
    }
  }
}
```

### Duplicates Text List (DAAC Format)
`./output/reports/duplicates/DSWX_HLS/2026-01-22.txt`

```
OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0
OPERA_L3_DSWx-HLS_T11SKA_20260116T183045Z_20260116T230000Z_S2A_30_v1.0
OPERA_L3_DSWx-HLS_T12SUD_20260118T182045Z_20260118T235959Z_L9_30_v1.0
```

### Summary Text
`./output/reports/duplicates/DSWX_HLS/2026-01-22_summary.txt`

```
OPERA Duplicates Report
==================================================
Product:        DSWX_HLS
Venue:          PROD
Generated:      2026-01-22 10:30:00

SUMMARY
--------------------------------------------------
Total Granules:     1,247
Unique Granules:    1,244
Duplicate Count:    3
Duplicate Rate:     0.24%

Files Generated:
- Full report:     ./output/reports/duplicates/DSWX_HLS/2026-01-22.json
- DAAC list:       ./output/reports/duplicates/DSWX_HLS/2026-01-22.txt
```

## Integration with Cron

### Daily Duplicate Check
```bash
# Add to crontab
0 2 * * * cd /path/to/opera-accountability && source .venv/bin/activate && opera-audit duplicates DSWX_HLS --days-back 1 --quiet >> /var/log/opera-audit.log 2>&1
```

### Weekly Accountability Check
```bash
# Run every Monday
0 3 * * 1 cd /path/to/opera-accountability && source .venv/bin/activate && opera-audit accountability --days-back 7 --quiet >> /var/log/opera-audit.log 2>&1
```

## Python API Usage

You can also use the package programmatically:

```python
from opera_accountability.cmr import query_cmr
from opera_accountability.duplicates import detect_duplicates
from opera_accountability.reports import save_reports
from datetime import datetime, timedelta

# Query CMR
end_date = datetime.now()
start_date = end_date - timedelta(days=7)
granules = query_cmr(
    collection_id='C2617126679-POCLOUD',
    start_date=start_date,
    end_date=end_date,
    venue='PROD'
)

# Detect duplicates
results = detect_duplicates(granules, 'DSWX_HLS')

# Save reports
files = save_reports(results, './output', 'DSWX_HLS', 'duplicates', 'PROD')

print(f"Found {results['duplicates']} duplicates")
print(f"Reports saved to: {files}")
```

## Troubleshooting

### CMR Connection Issues
```bash
# Test with verbose logging
opera-audit duplicates DSWX_HLS --days-back 1 --verbose
```

### Pattern Not Matching
```python
# Test pattern matching
from opera_accountability import CONFIG
import re

pattern = re.compile(CONFIG['products']['DSWX_HLS']['pattern'])
test_granule = 'OPERA_L3_DSWx-HLS_T10TEM_20260115T180931Z_20260115T235959Z_L8_30_v1.0'
match = pattern.match(test_granule)
print(match.groupdict() if match else "No match!")
```

### Check Configuration
```python
from opera_accountability import CONFIG
print(CONFIG['products']['DSWX_HLS'])
```
