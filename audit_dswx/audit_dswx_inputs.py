#!/usr/bin/env python3
"""
Audit OPERA DSWx-S1 granules at high latitudes to ensure they are composed from
RTC-S1 inputs from the SAME S1 track and within a small acquisition time spread.

Default checks:
- Latitude filter: bbox = -180,60,180,90 (>=60N)
- Time spread threshold: 10 minutes
- DSWx-S1 collection concept-id: C2949811996-POCLOUD

Outputs:
- CSV listing only FAILED DSWx-S1 granules and their offending RTC inputs.
- Optional JSON with full details.

Example:
  python audit_dswx_inputs.py \
    --temporal "2026-01-20T00:00:00Z,2026-01-30T23:59:59Z" \
    --out failures.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from collections import Counter

import requests


# ==============================================================================
# NASA CMR (Common Metadata Repository) API Endpoints
# ==============================================================================
CMR_BASE = "https://cmr.earthdata.nasa.gov"  # Base URL for CMR API
GRANULES_JSON = f"{CMR_BASE}/search/granules.json"  # Endpoint to search for granules
CONCEPT_UMM_JSON = f"{CMR_BASE}/search/concepts/{{concept_id}}.umm_json"  # Endpoint to fetch detailed metadata

# ==============================================================================
# Regular Expressions for Parsing RTC Granule Identifiers
# ==============================================================================

# Parse Sentinel-1 track number from RTC granule names
# Example: "OPERA_L2_RTC-S1_T056-118754-IW2_..." -> extracts "056"
# The track number (T056) tells us which orbital path the satellite was on
RTC_TRACK_RE = re.compile(r"RTC-S1_T(\d{3})-")

# Extract acquisition time from RTC granule names
# Example: "..._20260127T130031Z_..." -> extracts "20260127T130031Z"
# This is when the satellite captured the data (format: YYYYMMDDThhmmssZ)
ACQ_TIME_RE = re.compile(r"_(\d{8}T\d{6}Z)_")

# Extract the base RTC identifier to deduplicate file variants
# Each RTC acquisition produces multiple files (HH.tif, HV.tif, mask.tif, .h5)
# This regex captures the common base up to version number
# Example: "OPERA_L2_RTC-S1_T056-118754-IW2_20260127T130031Z_20260127T130056Z_S1A_30_v1.0_HH.tif"
#       -> "OPERA_L2_RTC-S1_T056-118754-IW2_20260127T130031Z_20260127T130056Z_S1A_30_v1.0"
RTC_BASE_RE = re.compile(
    r"(OPERA_L2_RTC-S1_T\d{3}-\d+-IW\d_\d{8}T\d{6}Z_\d{8}T\d{6}Z_S1[A-Z]_30_v\d+\.\d+)"
)


@dataclass
class Failure:
    """
    Data class to store information about DSWx-S1 granules that failed validation.

    A DSWx-S1 granule fails if:
    - Its RTC inputs come from different Sentinel-1 tracks (track mixing)
    - The time span between RTC acquisitions exceeds the threshold
    - Metadata is missing or unparseable
    """

    # DSWx-S1 granule name/identifier
    dswx_granule_ur: str
    # CMR concept ID (unique identifier in CMR)
    dswx_concept_id: str
    # Temporal coverage start
    start_time: str
    # Temporal coverage end
    end_time: str
    # Spatial bounding box
    bbox: str
    # List of Sentinel-1 tracks found (e.g., ['056', '127'])
    tracks_found: List[str]
    # Earliest RTC acquisition time
    acq_time_min: str
    # Latest RTC acquisition time
    acq_time_max: str
    # Time difference in minutes
    acq_time_span_minutes: float
    # Deduplicated list of RTC inputs
    rtc_inputs_unique: List[str]
    # Human-readable failure reason
    notes: str


def _request(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 60,
    tries: int = 5,
) -> requests.Response:
    """
    Make an HTTP request with automatic retry logic and exponential backoff.
    
    This helps handle temporary network issues or CMR service hiccups.
    - Retries on 5xx server errors or network exceptions
    - Uses exponential backoff: 2s, 4s, 8s, 10s (max), 10s
    - Gives up after 5 attempts
    """

    last = None
    for i in range(tries):
        try:
            r = session.get(url, params=params, headers=headers, timeout=timeout)
            # If server error (500+), wait and retry
            if r.status_code >= 500:
                time.sleep(min(2 ** (i + 1), 10))  # Exponential backoff, max 10 seconds
                continue
            return r
        except Exception as e:
            # Network error or other exception - retry
            last = e
            time.sleep(min(2 ** (i + 1), 10))
    raise RuntimeError(f"Request failed after {tries} tries: {url}. Last error: {last}")


def cmr_search_dswx(
    session: requests.Session,
    collection_concept_id: str,
    temporal: str,
    bbox: str,
    page_size: int,
    max_pages: int,
) -> Iterable[Dict[str, Any]]:
    """
    Search NASA CMR for DSWx-S1 granules matching spatial and temporal filters.
    
    Yields granule metadata one at a time (memory efficient for large result sets).
    
    Args:
        session: HTTP session for connection reuse
        collection_concept_id: CMR collection ID (e.g., 'C2949811996-POCLOUD')
        temporal: Time range in ISO 8601 format (e.g., '2026-01-20T00:00:00Z,2026-01-30T23:59:59Z')
        bbox: Bounding box as 'W,S,E,N' (e.g., '-180,60,180,90' for >=60N)
        page_size: Number of results per page (CMR limit is typically 2000)
        max_pages: Maximum pages to fetch (safety limit)
    
    Yields:
        dict: CMR granule metadata for each DSWx-S1 granule found
    """

    page_num = 1
    while page_num <= max_pages:
        # Build CMR search parameters
        params = {
            "collection_concept_id": collection_concept_id,  # Which collection to search
            "temporal": temporal,                            # Time filter
            "bounding_box": bbox,                            # Spatial filter
            "page_size": page_size,                          # Results per page
            "page_num": page_num,                            # Current page
        }
        r = _request(session, GRANULES_JSON, params=params, headers={"Accept": "application/json"})
        if r.status_code != 200:
            raise RuntimeError(f"CMR search failed {r.status_code}: {r.text[:300]}")

        data = r.json()
        # CMR returns results in feed.entry array
        items = (data.get("feed", {}) or {}).get("entry", []) or []
        if not items:
            break  # No more results

        # Yield each granule one at a time
        for it in items:
            yield it

        # If we got fewer results than requested, we've reached the end
        if len(items) < page_size:
            break

        page_num += 1


def fetch_umm(session: requests.Session, concept_id: str) -> Dict[str, Any]:
    """
    Fetch the full UMM (Unified Metadata Model) JSON for a specific granule.
    
    UMM metadata contains detailed information including InputGranules,
    which lists the RTC-S1 products used to create each DSWx-S1 granule.
    
    Args:
        session: HTTP session
        concept_id: CMR concept ID (e.g., 'G1234567890-POCLOUD')
    
    Returns:
        dict: Full UMM-JSON metadata
    """

    url = CONCEPT_UMM_JSON.format(concept_id=concept_id)
    # Request UMM-JSON format specifically (not the default CMR format)
    headers = {"Accept": "application/vnd.nasa.cmr.umm+json"}
    r = _request(session, url, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"UMM fetch failed {r.status_code}: {r.text[:300]}")
    return r.json()


def parse_time_utc(token: str) -> datetime:
    """
    Convert a compact time string to a timezone-aware datetime object.
    
    Args:
        token: Time string in format YYYYMMDDThhmmssZ (e.g., '20260127T130031Z')
    
    Returns:
        datetime: Timezone-aware datetime in UTC
    """

    return datetime.strptime(token, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def dedupe_rtc_inputs(input_granules: List[str], *, context: str = "") -> List[str]:
    bases: Set[str] = set()
    leftovers: List[str] = []

    # NEW: detect true duplicates (exact same filename repeated)
    counts = Counter(input_granules)
    dup_files = [g for g, c in counts.items() if c > 1]
    if dup_files:
        label = f" for {context}" if context else ""
        for g in sorted(dup_files):
            print(
                f"\n  ℹ️  Duplicate InputGranule listed {counts[g]} times{label}: {g}",
                file=sys.stderr,
            )

    for g in input_granules:
        m = RTC_BASE_RE.search(g)
        if m:
            bases.add(m.group(1))
        else:
            leftovers.append(g)

    if not bases:
        return sorted(set(input_granules))

    out = sorted(bases)
    if leftovers:
        out.extend([f"UNPARSED::{x}" for x in sorted(set(leftovers))])
    return out


def analyze_inputs(rtc_bases: List[str]) -> Tuple[Set[str], List[datetime], str]:
    """
    Parse track numbers and acquisition times from RTC base identifiers.
    
    This is the core validation logic:
    - Extract all unique track numbers (e.g., '056', '127')
    - Extract all acquisition times to compute time span
    - Collect any parsing errors or warnings
    
    Args:
        rtc_bases: List of deduplicated RTC identifiers
    
    Returns:
        tuple: (set of track IDs, list of acquisition datetimes, notes string)
    """

    tracks: Set[str] = set()          # Unique Sentinel-1 track numbers
    times: List[datetime] = []        # Acquisition times for span calculation
    notes: List[str] = []             # Warnings/errors during parsing

    for b in rtc_bases:
        # Skip unparseable entries (already flagged during deduplication)
        if b.startswith("UNPARSED::"):
            notes.append("Some InputGranules could not be normalized (UNPARSED entries present).")
            continue

        # Extract track number (e.g., T056 -> '056')
        mt = RTC_TRACK_RE.search(b)
        if mt:
            tracks.add(mt.group(1))  # Store just the number part
        else:
            # This shouldn't happen if deduplication worked correctly
            notes.append(f"Could not parse track from '{b}'")

        # Extract acquisition time (e.g., 20260127T130031Z)
        tt = ACQ_TIME_RE.search(b)
        if tt:
            times.append(parse_time_utc(tt.group(1)))  # Convert to datetime object
        else:
            notes.append(f"Could not parse acquisition time from '{b}'")

    # Return unique tracks, all times, and concatenated notes
    return tracks, times, "; ".join(sorted(set(notes)))


def get_bbox_str(cmr_entry: Dict[str, Any]) -> str:
    """
    Extract bounding box from CMR granule entry for reporting purposes.
    
    CMR includes spatial extent in various formats. We just grab the first
    'box' if available to show in the output CSV.
    """

    boxes = cmr_entry.get("boxes") or []
    if boxes and isinstance(boxes, list):
        return str(boxes[0])  # Return first bounding box as string
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--collection",
        default="C2949811996-POCLOUD",
        help="DSWx-S1 collection concept-id (default matches OPERA DSWx-S1 V1)",
    )
    ap.add_argument(
        "--temporal",
        required=True,
        help='Time range "start,end" in ISO8601, e.g. "2026-01-20T00:00:00Z,2026-01-30T23:59:59Z"',
    )
    ap.add_argument(
        "--bbox",
        default="-180,60,180,90",
        help='Bounding box W,S,E,N (default is >=60N: "-180,60,180,90")',
    )
    ap.add_argument(
        "--max-time-span-minutes",
        type=float,
        default=10.0,
        help="Fail if RTC acquisition time span exceeds this (minutes)",
    )
    ap.add_argument("--page-size", type=int, default=200)
    ap.add_argument("--max-pages", type=int, default=10)
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between UMM calls")
    ap.add_argument("--out", default="dswx_rtc_failures.csv", help="Output CSV for failures")
    ap.add_argument("--json-out", default=None, help="Optional JSON output path for failures")
    args = ap.parse_args()

    # ============================================================================
    # Initialize counters and results storage
    # ============================================================================
    failures: List[Failure] = []      # List of granules that failed validation
    total = 0                          # Total number of DSWx-S1 granules processed
    missing_inputgranules = 0          # Count of granules with no InputGranules metadata
    all_time_spans: List[float] = []  # Track all time spans for statistics (debugging)

    print(f"Starting DSWx-S1 audit v2...")
    print(f"  Collection: {args.collection}")
    print(f"  Temporal: {args.temporal}")
    print(f"  Bounding box: {args.bbox}")
    print(f"  Max time span: {args.max_time_span_minutes} minutes")
    print(f"  Output CSV: {args.out}")
    if args.json_out:
        print(f"  Output JSON: {args.json_out}")
    print()

    # ============================================================================
    # Main processing loop: Query CMR and validate each DSWx-S1 granule
    # ============================================================================
    with requests.Session() as s:  # Reuse HTTP connections for better performance
        print("Querying CMR for DSWx-S1 granules...")
        # Iterate through all DSWx-S1 granules matching our filters
        for entry in cmr_search_dswx(
            s,
            collection_concept_id=args.collection,
            temporal=args.temporal,
            bbox=args.bbox,
            page_size=args.page_size,
            max_pages=args.max_pages,
        ):
            total += 1

            # Extract basic metadata from CMR search result
            dswx_ur = entry.get("title") or entry.get("granule_ur") or ""  # Granule name
            print(f"\r[{total}] Processing: {dswx_ur[:80]}...", end="", flush=True)
            dswx_concept_id = entry.get("id") or ""        # CMR concept ID (e.g., G1234567890-POCLOUD)
            start_time = entry.get("time_start") or ""     # Temporal coverage start
            end_time = entry.get("time_end") or ""         # Temporal coverage end
            bbox_str = get_bbox_str(entry)                 # Spatial bounding box

            # ====================================================================
            # Step 1: Fetch detailed UMM metadata to get InputGranules list
            # ====================================================================
            try:
                umm = fetch_umm(s, dswx_concept_id)  # Get full metadata including RTC inputs
            except Exception as e:
                # If we can't fetch metadata, record as failure
                failures.append(
                    Failure(
                        dswx_granule_ur=str(dswx_ur),
                        dswx_concept_id=str(dswx_concept_id),
                        start_time=str(start_time),
                        end_time=str(end_time),
                        bbox=str(bbox_str),
                        tracks_found=[],
                        acq_time_min="",
                        acq_time_max="",
                        acq_time_span_minutes=float("nan"),
                        rtc_inputs_unique=[],
                        notes=f"UMM fetch failed: {e}",
                    )
                )
                continue

            # ====================================================================
            # Step 2: Extract InputGranules (the RTC-S1 inputs used to create this DSWx-S1)
            # ====================================================================
            input_granules = umm.get("InputGranules")
            if not input_granules:
                # No inputs listed = suspicious, mark as failure
                missing_inputgranules += 1
                failures.append(
                    Failure(
                        dswx_granule_ur=str(umm.get("GranuleUR") or dswx_ur),
                        dswx_concept_id=str(dswx_concept_id),
                        start_time=str(start_time),
                        end_time=str(end_time),
                        bbox=str(bbox_str),
                        tracks_found=[],
                        acq_time_min="",
                        acq_time_max="",
                        acq_time_span_minutes=float("nan"),
                        rtc_inputs_unique=[],
                        notes="No InputGranules found in UMM metadata",
                    )
                )
                continue

            # ====================================================================
            # Step 3: Normalize InputGranules format
            # ====================================================================
            # UMM InputGranules can be either:
            #   - Simple strings: "OPERA_L2_RTC-S1_..."
            #   - Dicts with GranuleUR key: {"GranuleUR": "OPERA_L2_RTC-S1_..."}
            # Convert everything to simple strings
            raw_inputs: List[str] = []
            for it in input_granules:
                if isinstance(it, dict) and "GranuleUR" in it:
                    raw_inputs.append(str(it["GranuleUR"]))  # Extract from dict
                else:
                    raw_inputs.append(str(it))  # Already a string

            # ====================================================================
            # Step 4: Deduplicate RTC inputs (remove HH/HV/mask variants)
            # ====================================================================
            # NOTE: context is used only for console logging of duplicates.
            rtc_unique = dedupe_rtc_inputs(raw_inputs, context=str(umm.get("GranuleUR") or dswx_ur))
            
            # ====================================================================
            # Step 5: Parse track numbers and acquisition times from RTC names
            # ====================================================================
            tracks, times, notes = analyze_inputs(rtc_unique)

            # ====================================================================
            # Step 6: Check for failure conditions
            # ====================================================================
            # FAILURE CONDITION 1: Multiple tracks found (track mixing!)
            mixed_tracks = len(tracks) > 1

            # FAILURE CONDITION 2: Time span exceeds threshold
            span_minutes = 0.0
            tmin_str = ""
            tmax_str = ""
            time_fail = False
            if times:
                # Calculate time span between earliest and latest RTC acquisition
                tmin = min(times)
                tmax = max(times)
                span_minutes = (tmax - tmin).total_seconds() / 60.0  # Convert to minutes
                tmin_str = tmin.strftime("%Y-%m-%dT%H:%M:%SZ")       # Format for output
                tmax_str = tmax.strftime("%Y-%m-%dT%H:%M:%SZ")
                # Track time span for statistics
                all_time_spans.append(span_minutes)
                # Check if span exceeds threshold (default 10 minutes)
                time_fail = span_minutes > args.max_time_span_minutes
            else:
                # If we can't parse any times, that's also a problem - mark as failure
                time_fail = True
                notes = (notes + "; " if notes else "") + "No parsable acquisition times found"

            # ====================================================================
            # Step 7: Record failures and report to console
            # ====================================================================
            if mixed_tracks or time_fail:
                # Build detailed failure message
                fail_notes = notes
                if mixed_tracks:
                    fail_notes = (fail_notes + "; " if fail_notes else "") + "Mixed tracks detected"
                if time_fail and times:
                    fail_notes = (fail_notes + "; " if fail_notes else "") + (
                        f"Acquisition time span {span_minutes:.2f} min exceeds {args.max_time_span_minutes:.2f}"
                    )

                # Print immediate alert to console
                print(f"\n  ⚠️  FAILURE: {dswx_ur[:60]}...")
                if mixed_tracks:
                    print(f"      Tracks: {sorted(tracks)}")
                if time_fail:
                    print(f"      Time span: {span_minutes:.2f} min")

                # Store detailed failure information for CSV output
                failures.append(
                    Failure(
                        dswx_granule_ur=str(umm.get("GranuleUR") or dswx_ur),
                        dswx_concept_id=str(dswx_concept_id),
                        start_time=str(start_time),
                        end_time=str(end_time),
                        bbox=str(bbox_str),
                        tracks_found=sorted(tracks),
                        acq_time_min=tmin_str,
                        acq_time_max=tmax_str,
                        acq_time_span_minutes=span_minutes,
                        rtc_inputs_unique=rtc_unique,
                        notes=fail_notes,
                    )
                )

            # Optional: sleep between requests to avoid overwhelming CMR
            if args.sleep > 0:
                time.sleep(args.sleep)

    print("\n\n" + "=" * 60)
    print("AUDIT COMPLETE")
    print("=" * 60)

    # ============================================================================
    # Write results to CSV file
    # ============================================================================
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "dswx_granule_ur",
                "dswx_concept_id",
                "start_time",
                "end_time",
                "bbox",
                "tracks_found",
                "acq_time_min",
                "acq_time_max",
                "acq_time_span_minutes",
                "rtc_inputs_unique",
                "notes",
            ]
        )
        # Write one row per failed granule
        for fail in failures:
            w.writerow(
                [
                    fail.dswx_granule_ur,
                    fail.dswx_concept_id,
                    fail.start_time,
                    fail.end_time,
                    fail.bbox,
                    ",".join(fail.tracks_found),              # Convert list to comma-separated string
                    fail.acq_time_min,
                    fail.acq_time_max,
                    # Only write span if it's a valid number (not NaN)
                    f"{fail.acq_time_span_minutes:.3f}" if fail.acq_time_span_minutes == fail.acq_time_span_minutes else "",
                    " | ".join(fail.rtc_inputs_unique),       # Pipe-separated RTC list
                    fail.notes,
                ]
            )

    # ============================================================================
    # Optionally write JSON output (machine-readable format)
    # ============================================================================
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            # Convert Failure dataclass instances to dicts for JSON serialization
            json.dump([fail.__dict__ for fail in failures], f, indent=2)

    # ============================================================================
    # Print summary statistics
    # ============================================================================
    print(f"\nGranules scanned: {total}")
    print(f"Failures found: {len(failures)}")
    print(f"Pass rate: {((total - len(failures)) / total * 100) if total > 0 else 0:.1f}%")
    if missing_inputgranules > 0:
        print(f"Granules missing InputGranules: {missing_inputgranules}")
    
    # Print time span statistics (debugging)
    if all_time_spans:
        avg_span = sum(all_time_spans) / len(all_time_spans)
        min_span = min(all_time_spans)
        max_span = max(all_time_spans)
        print(f"\n--- RTC Acquisition Time Span Statistics ---")
        print(f"Average: {avg_span:.2f} minutes")
        print(f"Minimum: {min_span:.2f} minutes")
        print(f"Maximum: {max_span:.2f} minutes")
        print(f"Granules with parseable times: {len(all_time_spans)}")
    
    print(f"\nResults written to: {args.out}")
    if args.json_out:
        print(f"JSON output: {args.json_out}")
    
    # Final verdict
    if failures:
        print(f"\n⚠️  {len(failures)} DSWx-S1 granule(s) failed validation!")
        print("   Review the output CSV for details.")
    else:
        print("\n✓ All DSWx-S1 granules passed validation.")

    # ============================================================================
    # Return exit code
    # ============================================================================
    # Exit with code 2 if any failures found (allows CI to detect issues)
    # Exit with code 0 if everything passed
    return 2 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
