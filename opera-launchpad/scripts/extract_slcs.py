#!/usr/bin/env python3
"""Turn a cmr_audit_burst_coverage.py report into the SLC reprocess list OPS works from.

usage: extract_slcs.py [report.jsonl | report.json]

With no argument, uses the newest report the launchpad audit tools wrote, in
either place a report ends up:
  output/<PRODUCT>/audit/<timestamp>/        (collected after the run)
  repos/<PRODUCT>/audit/tools/ops/cmr_audit/ (the tool's own directory)

Reads the chunked JSONL the registry runs produce (one chunk_result line per
day) or a whole-run JSON report. Writes, next to the report:
  slcs-to-reprocess-<product>.txt         one SLC id per line (CMR native id without -SLC), sorted
  slcs-to-reprocess-<product>-detail.csv  slc,date,platform,missing_bursts,fully_missing
and prints what still needs a re-run (skipped chunks, unverified bursts).

The txt-format report of older runs has no SLC ids and cannot be used.
"""
import csv
import glob
import json
import os
import sys
from collections import Counter, defaultdict

LAUNCHPAD = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATTERNS = [
    f"{LAUNCHPAD}/output/*/audit/*/*.json*",
    f"{LAUNCHPAD}/repos/*/audit/tools/ops/cmr_audit/*.json*",
]


def load_report(path):
    """Return (products, meta) with products = {product: {"missing": [...], "unverified": [...]}}."""
    products = defaultdict(lambda: {"missing": [], "unverified": [], "found_count": 0})
    meta = {"failed_chunks": [], "unverified_days": set()}
    with open(path) as f:
        head = f.read(1)
        f.seek(0)
        if head != "{":
            raise ValueError("not JSON")
        if path.endswith(".jsonl"):
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                kind = rec.get("_type")
                if kind == "metadata":
                    meta.update({k: rec[k] for k in ("start_datetime", "end_datetime") if k in rec})
                elif kind == "chunk_result":
                    p = products[rec["product_type"]]
                    p["missing"].extend(rec.get("missing", []))
                    p["unverified"].extend(rec.get("unverified", []))
                    p["found_count"] += rec.get("found_count", 0)
                elif kind == "chunk_error":
                    meta["failed_chunks"].append(rec)
                elif kind == "summary":
                    meta["unverified_days"].update(rec.get("unverified_days", []))
                    meta["summary"] = rec
            if not products and "summary" not in meta:
                raise ValueError("no chunk_result records")
        else:
            results = json.load(f)
            if "products" not in results:
                raise ValueError("no 'products' key")
            meta.update(results.get("metadata", {}))
            meta["unverified_days"] = set(meta.get("unverified_days") or [])
            for product, cov in results["products"].items():
                products[product]["missing"] = cov.get("missing", [])
                products[product]["unverified"] = cov.get("unverified", [])
                products[product]["found_count"] = cov.get("found_count", 0)
    return products, meta


def newest_report():
    cands = sorted((p for pat in PATTERNS for p in glob.glob(pat)), key=os.path.getmtime, reverse=True)
    if not cands:
        sys.exit("no .json/.jsonl files found under the launchpad output/ or repos/*/audit tool dirs")
    for cand in cands:
        try:
            return cand, load_report(cand)
        except (json.JSONDecodeError, ValueError, KeyError) as exc:
            print(f"skipped {cand}: {exc}")
    sys.exit("found report files but none could be read as an audit report")


def main():
    if len(sys.argv) > 1:
        path = sys.argv[1]
        try:
            products, meta = load_report(path)
        except (json.JSONDecodeError, ValueError, KeyError) as exc:
            sys.exit(f"{path} is not a usable audit report ({exc}). A txt-format report has no SLC ids.")
    else:
        path, (products, meta) = newest_report()

    print(f"report: {path}")
    if meta.get("start_datetime"):
        print(f"window: {meta['start_datetime']} .. {meta.get('end_datetime')}")

    for product, cov in products.items():
        by_slc = Counter(m["slc_native_id"] for m in cov["missing"])
        found_slcs = set()  # SLCs with at least one product found are "partial", not fully missing
        rows = []
        for sid, n in sorted(by_slc.items()):
            first = next(m for m in cov["missing"] if m["slc_native_id"] == sid)
            rows.append((sid.replace("-SLC", ""), first["acquisition_time"][:10], first["platform"], n))
        stem = os.path.join(os.path.dirname(os.path.abspath(path)), f"slcs-to-reprocess-{product.lower()}")
        with open(stem + ".txt", "w") as f:
            f.write("\n".join(r[0] for r in rows) + ("\n" if rows else ""))
        with open(stem + "-detail.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["slc", "date", "platform", "missing_bursts", "fully_missing"])
            for sid, day, plat, n in rows:
                w.writerow([sid, day, plat, n, "yes" if sid + "-SLC" not in found_slcs else "unknown"])
        plats = Counter(r[2] for r in rows)
        print(f"{product}: found {cov['found_count']:,} bursts, missing {len(cov['missing']):,} bursts in "
              f"{len(rows)} SLCs {dict(plats)} -> {stem}.txt")
        if cov["unverified"]:
            days = sorted({u["acquisition_time"][:10] for u in cov["unverified"]})
            print(f"  {len(cov['unverified'])} bursts UNVERIFIED (CMR gave no complete answer) on {','.join(days)}: "
                  "not in the list; re-run those days with recheck_dates")

    if meta["failed_chunks"]:
        days = sorted(fc["chunk_start"][:10] for fc in meta["failed_chunks"])
        print(f"{len(days)} day(s) SKIPPED (no complete SLC listing from CMR): {','.join(days)}: re-run with recheck_dates")
    if meta.get("unverified_days"):
        print(f"days with unverified bursts: {','.join(sorted(meta['unverified_days']))}")


if __name__ == "__main__":
    main()
