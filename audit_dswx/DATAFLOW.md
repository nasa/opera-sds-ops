# Data Flow Diagram

## Visual Overview of audit_dswx_inputs.py

```
┌─────────────────────────────────────────────────────────────────────┐
│                         USER INPUT                                  │
│  --temporal "2026-01-20T00:00:00Z,2026-01-30T23:59:59Z"           │
│  --bbox "-180,60,180,90"  (high latitudes)                         │
│  --max-time-span-minutes 10.0                                      │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    STEP 1: QUERY NASA CMR                          │
│                                                                     │
│  Function: cmr_search_dswx()                                       │
│  Endpoint: https://cmr.earthdata.nasa.gov/search/granules.json    │
│  Filters:                                                          │
│    • Collection: C2949811996-POCLOUD (DSWx-S1)                    │
│    • Temporal: 2026-01-20 to 2026-01-30                           │
│    • Spatial: ≥60°N                                                │
│                                                                     │
│  Returns: Stream of DSWx-S1 granule metadata (paginated)          │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
       ┌───────────────────────────────────────────┐
       │  FOR EACH DSWx-S1 GRANULE (loop)         │
       └───────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              STEP 2: FETCH DETAILED METADATA                       │
│                                                                     │
│  Function: fetch_umm()                                             │
│  Endpoint: .../concepts/{concept_id}.umm_json                      │
│  Input: G1234567890-POCLOUD                                        │
│                                                                     │
│  Returns: UMM-JSON with InputGranules field                        │
│                                                                     │
│  Example InputGranules:                                            │
│    [                                                               │
│      "OPERA_L2_RTC-S1_T056-..._v1.0.h5",                          │
│      "OPERA_L2_RTC-S1_T056-..._v1.0_HH.tif",                      │
│      "OPERA_L2_RTC-S1_T056-..._v1.0_HV.tif",                      │
│      "OPERA_L2_RTC-S1_T056-..._v1.0_mask.tif",                    │
│      ...                                                           │
│    ]                                                               │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│            STEP 3: NORMALIZE INPUT FORMAT                          │
│                                                                     │
│  Convert from:                                                     │
│    {"GranuleUR": "OPERA_L2_RTC-S1_..."}  (dict)                   │
│    OR "OPERA_L2_RTC-S1_..."              (string)                 │
│  To:                                                               │
│    ["OPERA_L2_RTC-S1_...", ...]          (list of strings)        │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│            STEP 4: DEDUPLICATE RTC INPUTS                          │
│                                                                     │
│  Function: dedupe_rtc_inputs()                                     │
│  Regex: RTC_BASE_RE                                                │
│                                                                     │
│  SUBSTEP 4A: DETECT EXACT DUPLICATES                              │
│    • Uses Counter to find identical filenames listed multiple times│
│    • Prints warnings to stderr for any duplicates found            │
│    • Example: Same file listed twice in InputGranules             │
│                                                                     │
│  SUBSTEP 4B: EXTRACT BASE IDENTIFIERS                             │
│  INPUT (4 files = 1 acquisition):                                 │
│    OPERA_L2_RTC-S1_T056-118754-IW2_20260127T130031Z_..._v1.0.h5   │
│    OPERA_L2_RTC-S1_T056-118754-IW2_20260127T130031Z_..._v1.0_HH.tif│
│    OPERA_L2_RTC-S1_T056-118754-IW2_20260127T130031Z_..._v1.0_HV.tif│
│    OPERA_L2_RTC-S1_T056-118754-IW2_20260127T130031Z_..._v1.0_mask.tif│
│                                                                     │
│  OUTPUT (1 unique base):                                           │
│    OPERA_L2_RTC-S1_T056-118754-IW2_20260127T130031Z_..._v1.0      │
│                                                                     │
│  Unparseable entries prefixed with "UNPARSED::" for visibility    │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│         STEP 5: PARSE TRACK NUMBERS & TIMES                        │
│                                                                     │
│  Function: analyze_inputs()                                        │
│  Regexes: RTC_TRACK_RE, ACQ_TIME_RE                               │
│                                                                     │
│  For: OPERA_L2_RTC-S1_T056-118754-IW2_20260127T130031Z_...        │
│                           ^^^              ^^^^^^^^^^^^^^^         │
│                        Track=056       Time=20260127T130031Z       │
│                                                                     │
│  OUTPUT:                                                           │
│    tracks = {'056'}                                                │
│    times = [datetime(2026, 1, 27, 13, 0, 31)]                     │
│    notes = ""                                                      │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│           STEP 6: VALIDATE (CHECK FAILURES)                        │
│                                                                     │
│  CONDITION 1: Mixed tracks?                                        │
│    Check: len(tracks) > 1                                          │
│    Example FAIL: tracks = {'056', '127'}                           │
│    Example PASS: tracks = {'056'}                                  │
│                                                                     │
│  CONDITION 2: Time span too large?                                 │
│    Calculate: max(times) - min(times)                              │
│    Check: span_minutes > 10.0                                      │
│    Example FAIL: 15.3 minutes                                      │
│    Example PASS: 2.5 minutes                                       │
│                                                                     │
│  CONDITION 3: Missing/unparseable data?                            │
│    Check: No InputGranules OR can't parse track/time               │
└─────────────────────────────────────────────────────────────────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
           PASS ▼                        FAIL ▼
    ┌──────────────────┐       ┌──────────────────────┐
    │   SKIP          │       │  RECORD FAILURE      │
    │   (no output)   │       │                      │
    │                 │       │  failures.append(    │
    │                 │       │    Failure(...)      │
    │                 │       │  )                   │
    │                 │       │                      │
    │                 │       │  Print to console:   │
    │                 │       │  ⚠️  FAILURE: ...    │
    └──────────────────┘       └──────────────────────┘
                │                             │
                └──────────────┬──────────────┘
                               ▼
              ┌────────────────────────────┐
              │  NEXT DSWx-S1 GRANULE     │
              │  (back to STEP 2)         │
              └────────────────────────────┘

                               │
                    (end of loop)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  STEP 7: WRITE OUTPUT                              │
│                                                                     │
│  CSV File (dswx_rtc_failures.csv):                                │
│  ┌───────────────────────────────────────────────────────────┐    │
│  │ dswx_granule_ur | tracks_found | acq_time_span | notes    │    │
│  ├───────────────────────────────────────────────────────────┤    │
│  │ OPERA_L3_DSWx.. | 056,127     | 15.300         | Mixed... │    │
│  │ OPERA_L3_DSWx.. | 056         | 12.100         | Time...  │    │
│  └───────────────────────────────────────────────────────────┘    │
│                                                                     │
│  Optional JSON File (dswx_rtc_failures.json):                     │
│  {                                                                 │
│    "dswx_granule_ur": "OPERA_L3_DSWx...",                         │
│    "tracks_found": ["056", "127"],                                │
│    "notes": "Mixed tracks detected"                               │
│  }                                                                 │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  STEP 8: PRINT SUMMARY                             │
│                                                                     │
│  Granules scanned: 150                                            │
│  Failures found: 3                                                │
│  Pass rate: 98.0%                                                 │
│                                                                     │
│  Results written to: dswx_rtc_failures.csv                        │
│                                                                     │
│  ⚠️  3 DSWx-S1 granule(s) failed validation!                      │
│     Review the output CSV for details.                            │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     EXIT WITH CODE                                 │
│                                                                     │
│  return 2 if failures else 0                                      │
│                                                                     │
│  • Code 0: All granules passed ✓                                  │
│  • Code 2: Some granules failed ✗ (for CI detection)              │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Data Transformations

### Transformation 1: RTC Deduplication

```
┌───────────────────────────────────┐
│ RAW CMR InputGranules             │
│ (4 files per acquisition)         │
├───────────────────────────────────┤
│ RTC_T056_..._v1.0.h5             │
│ RTC_T056_..._v1.0_HH.tif         │    dedupe_rtc_inputs()
│ RTC_T056_..._v1.0_HV.tif         │  ──────────────────────────>
│ RTC_T056_..._v1.0_mask.tif       │
│                                   │
│ RTC_T127_..._v1.0.h5             │
│ RTC_T127_..._v1.0_HH.tif         │
│ RTC_T127_..._v1.0_HV.tif         │
│ RTC_T127_..._v1.0_mask.tif       │
└───────────────────────────────────┘

                 │
                 ▼

┌───────────────────────────────────┐
│ DEDUPLICATED RTC Bases            │
│ (1 per acquisition)               │
├───────────────────────────────────┤
│ RTC_T056_..._v1.0                │
│ RTC_T127_..._v1.0                │
└───────────────────────────────────┘
```

### Transformation 2: Track & Time Extraction

```
┌─────────────────────────────────────────────────────┐
│ RTC Base: OPERA_L2_RTC-S1_T056-118754-IW2_          │
│                       20260127T130031Z_              │
│                       20260127T130056Z_S1A_30_v1.0   │
└─────────────────────────────────────────────────────┘
                        │
         ┌──────────────┴──────────────┐
         │                             │
         ▼                             ▼
┌──────────────────┐        ┌─────────────────────┐
│ RTC_TRACK_RE     │        │ ACQ_TIME_RE         │
│ Extracts: "056"  │        │ Extracts:           │
│                  │        │ "20260127T130031Z"  │
└──────────────────┘        └─────────────────────┘
         │                             │
         ▼                             ▼
    Track: "056"              datetime(2026, 1, 27, 13, 0, 31)
```

### Transformation 3: Failure Detection

```
Input Data:
  tracks = {'056', '127'}
  times = [2026-01-27 13:00:31, 2026-01-27 13:10:45]
  threshold = 10.0 minutes

Processing:
  mixed_tracks = len(tracks) > 1
               = len({'056', '127'}) > 1
               = 2 > 1
               = True ✗

  span_minutes = (max(times) - min(times)) / 60
               = (13:10:45 - 13:00:31) / 60
               = 10.23 minutes
  
  time_fail = span_minutes > threshold
            = 10.23 > 10.0
            = True ✗

Result:
  FAILURE (both conditions failed)
  notes = "Mixed tracks detected; Acquisition time span 10.23 min exceeds 10.00"
```

## Memory & Performance

```
Memory Efficient Design:
┌────────────────────────────────────────────────────┐
│ CMR Returns: 10,000 granules                      │
│                                                    │
│ Traditional approach (load all):                  │
│   Memory: ~500 MB                                 │
│                                                    │
│ This script (yield/stream):                       │
│   Memory: ~5 KB per granule                       │
│   Process one at a time                           │
│   ✓ Can handle any size dataset                   │
└────────────────────────────────────────────────────┘

HTTP Session Reuse:
┌────────────────────────────────────────────────────┐
│ Without Session:                                   │
│   • New TCP connection per request                │
│   • ~100ms overhead per request                   │
│   • 10,000 requests = 16 minutes overhead         │
│                                                    │
│ With requests.Session():                          │
│   • Reuse TCP connections (keep-alive)            │
│   • ~1ms overhead per request                     │
│   • 10,000 requests = 10 seconds overhead         │
│   ✓ 100x faster!                                  │
└────────────────────────────────────────────────────┘
```

## Error Handling Flow

```
┌──────────────────┐
│ Make HTTP Request│
└────────┬─────────┘
         │
    ┌────▼────┐
    │ Success?│──Yes──> Return response
    └────┬────┘
         │No
         ▼
    ┌─────────────────┐
    │ Server 5xx OR   │
    │ Network Error?  │
    └────┬────────────┘
         │
         ▼
    ┌──────────────────┐
    │ Retry count < 5? │──No──> Raise RuntimeError
    └────┬─────────────┘
         │Yes
         ▼
    ┌──────────────────┐
    │ Sleep (backoff)  │
    │ 2s, 4s, 8s, 10s  │
    └────┬─────────────┘
         │
         └──> (retry request)
```

## Audit Script Organization

The project includes separate audit scripts for different time ranges:

```
audit_dswx_inputs.py              Main audit script (used by all runners)
├── audit_last_1_year.sh          Last 1 year (365 days from today)
├── audit_last_2_years.sh         Last 2 years (730 days from today)
├── audit_last_5_years.sh         Last 5 years (1825 days from today)
└── run_all_audits.sh             Master script (runs 2-year audit only)

Each script:
  • Automatically calculates current UTC date
  • Computes lookback period (365, 730, or 1825 days)
  • Configures max-pages and sleep intervals based on data volume
  • Outputs results to separate CSV files
  • Uses 5-minute max time span threshold
  • Displays calculated dates before execution for verification
```

### Automatic Date Calculation

Each audit script uses cross-platform date commands to ensure compatibility:

```bash
# Get current date in UTC
END_DATE=$(date -u +"%Y-%m-%dT23:59:59Z")

# Calculate lookback date (Linux/GNU or macOS/BSD compatible)
START_DATE=$(date -u -d "X days ago" +"%Y-%m-%dT00:00:00Z" 2>/dev/null || \
             date -u -v-Xd +"%Y-%m-%dT00:00:00Z" 2>/dev/null)
```

This approach:
- Works on both Linux (GNU date) and macOS (BSD date)
- Automatically updates to current date each time script runs
- No manual date maintenance required
- Displays calculated dates for verification

### Time Range Configurations

```
1 year:     365 days  --max-pages 200  --sleep 0.1s
2 years:    730 days  --max-pages 400  --sleep 0.1s
5 years:   1825 days  --max-pages 1000 --sleep 0.2s

Sleep intervals:
  • 1-2 years: 0.1s (light throttling to avoid CMR rate limits)
  • 5 years:   0.2s (moderate throttling for large datasets)
```
