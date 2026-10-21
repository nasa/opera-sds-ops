#!/usr/bin/env python3
"""Turn a cmr_audit_burst_coverage.py report into the SLC reprocess list OPS works from.

usage: extract_slcs.py [report.jsonl | report.json] [--platform-start PLATFORM=YYYY-MM-DD ...]
                       [--no-platform-start]

With no report argument, uses the newest report the launchpad audit tools
wrote, in either place a report ends up:
  output/<PRODUCT>/audit/<timestamp>/        (collected after the run)
  repos/<PRODUCT>/audit/tools/ops/cmr_audit/ (the tool's own directory)

Reads the chunked JSONL the registry runs produce (one chunk_result line per
day) or a whole-run JSON report. Writes, next to the report:
  slcs-to-reprocess-<product>.txt         one SLC id per line (CMR native id without -SLC), sorted
  slcs-to-reprocess-<product>-detail.csv  slc,date,platform,missing_bursts,fully_missing
                                          (fully_missing=no: some bursts of the scene do have a product)
  slcs-excluded-<product>.csv             the flagged SLCs left off the list, with the reason
and prints what still needs a re-run (skipped chunks, unverified bursts).

Scenes OPS does not produce on purpose are left off the list. The audit flags
them anyway, because it only knows the North America polygon, not production
policy. By default that is every S1D scene sensed before 2026-06-13, the
agreed start of S1D CSLC production (ADT/PST, 2026-10). --platform-start adds
or replaces such a rule (e.g. --platform-start S1D=2026-06-13), and
--no-platform-start drops them all so every flagged SLC is listed.

The txt-format report of older runs has no SLC ids and cannot be used.
"""
import argparse
import csv
import datetime
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

# Platform -> first sensing date OPS produces CSLCs for. Flagged scenes of that
# platform sensed earlier are expected to be absent and are left off the list.
# S1D: CSLC production starts at sensing 2026-06-13 (ADT/PST decision, 2026-10).
DEFAULT_PLATFORM_START = {"S1D": "2026-06-13"}


def load_report(path):
    """Return (products, meta) with products = {product: {"missing": [...], "unverified": [...]}}."""
    products = defaultdict(lambda: {"missing": [], "unverified": [], "found_count": 0, "found_slcs": set()})
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
                    p["found_slcs"].update(m.get("slc_native_id") for m in rec.get("found", []))
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
                products[product]["found_slcs"] = {m.get("slc_native_id") for m in cov.get("found", [])}
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


def platform_starts(rules, use_defaults=True):
    """{platform: 'YYYY-MM-DD'} from the defaults plus --platform-start PLATFORM=YYYY-MM-DD rules.

    A rule for a platform in the defaults replaces the default date. Dates are checked so a typo
    cannot silently exclude everything or nothing.
    """
    starts = dict(DEFAULT_PLATFORM_START) if use_defaults else {}
    for rule in rules or []:
        platform, sep, day = rule.partition("=")
        if not sep or not platform or not day:
            sys.exit(f"--platform-start expects PLATFORM=YYYY-MM-DD, got {rule!r}")
        try:
            datetime.date.fromisoformat(day)
        except ValueError:
            sys.exit(f"--platform-start {rule!r}: {day!r} is not a YYYY-MM-DD date")
        starts[platform.upper()] = day
    return starts


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                     epilog="See the module docstring for the output files and the exclusion rules.")
    parser.add_argument("report", nargs="?", help="audit report (.jsonl or .json); default: the newest one found")
    parser.add_argument("--platform-start", action="append", metavar="PLATFORM=YYYY-MM-DD",
                        help="leave flagged scenes of PLATFORM sensed before this date off the list; repeatable, "
                             "replaces the default for that platform (default: %s)"
                             % ", ".join(f"{p}={d}" for p, d in DEFAULT_PLATFORM_START.items()))
    parser.add_argument("--no-platform-start", action="store_true",
                        help="apply no exclusion rules, list every flagged SLC")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    starts = platform_starts(args.platform_start, use_defaults=not args.no_platform_start)

    if args.report:
        path = args.report
        try:
            products, meta = load_report(path)
        except (json.JSONDecodeError, ValueError, KeyError) as exc:
            sys.exit(f"{path} is not a usable audit report ({exc}). A txt-format report has no SLC ids.")
    else:
        path, (products, meta) = newest_report()

    print(f"report: {path}")
    if meta.get("start_datetime"):
        print(f"window: {meta['start_datetime']} .. {meta.get('end_datetime')}")
    if starts:
        print("left off the list: " + ", ".join(f"{p} sensed before {d}" for p, d in sorted(starts.items())))

    for product, cov in products.items():
        by_slc = Counter(m["slc_native_id"] for m in cov["missing"])
        found_slcs = cov["found_slcs"]  # SLCs with at least one product found are "partial", not fully missing
        rows, excluded = [], []
        for sid, n in sorted(by_slc.items()):
            first = next(m for m in cov["missing"] if m["slc_native_id"] == sid)
            row = (sid.replace("-SLC", ""), first["acquisition_time"][:10], first["platform"], n)
            start = starts.get(row[2])
            if start and row[1] < start:
                excluded.append(row + (f"{row[2]} sensed before {start}",))
            else:
                rows.append(row)
        stem = os.path.join(os.path.dirname(os.path.abspath(path)), f"slcs-to-reprocess-{product.lower()}")
        with open(stem + ".txt", "w") as f:
            f.write("\n".join(r[0] for r in rows) + ("\n" if rows else ""))
        with open(stem + "-detail.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["slc", "date", "platform", "missing_bursts", "fully_missing"])
            for sid, day, plat, n in rows:
                w.writerow([sid, day, plat, n, "no" if sid + "-SLC" in found_slcs else "yes"])
        plats = Counter(r[2] for r in rows)
        print(f"{product}: found {cov['found_count']:,} bursts, missing {len(cov['missing']):,} bursts in "
              f"{len(by_slc)} SLCs; listed {len(rows)} SLCs {dict(plats)} -> {stem}.txt")
        ex_path = os.path.join(os.path.dirname(stem), f"slcs-excluded-{product.lower()}.csv")
        if not excluded and os.path.exists(ex_path):
            os.remove(ex_path)  # left by an earlier run with other rules; the files always describe this run
        if excluded:
            with open(ex_path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["slc", "date", "platform", "missing_bursts", "reason"])
                w.writerows(excluded)
            reasons = Counter(e[4] for e in excluded)
            print(f"  {len(excluded)} SLCs left off the list ({'; '.join(f'{r}: {c}' for r, c in sorted(reasons.items()))})"
                  f" -> {ex_path}")
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
