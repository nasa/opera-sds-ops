"""Launchpad CLI: exit-code reporting, bool params with a value, and the audit registry entries.

Run from opera-launchpad/ with its .venv: `.venv/bin/python -m pytest tests -q`.
"""
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

import cli  # noqa: E402

PYTHON = sys.executable


def _yes():
    prompt = MagicMock()
    prompt.unsafe_ask.return_value = True
    return prompt


def test_exit_code_meanings_defaults_to_zero_and_accepts_string_keys():
    assert cli.exit_code_meanings({}) == {0: "completed"}
    assert cli.exit_code_meanings({"exit_codes": {"0": "ok", 1: "findings"}}) == {0: "ok", 1: "findings"}


def test_a_listed_non_zero_exit_counts_as_a_run(tmp_path):
    with patch.object(cli.questionary, "confirm", return_value=_yes()):
        ran = cli.confirm_and_run([PYTHON, "-c", "import sys; sys.exit(1)"], cwd=tmp_path,
                                  exit_codes={0: "clean", 1: "findings"})
    assert ran is True


def test_an_unlisted_exit_is_still_a_failure(tmp_path):
    with patch.object(cli.questionary, "confirm", return_value=_yes()):
        ran = cli.confirm_and_run([PYTHON, "-c", "import sys; sys.exit(1)"], cwd=tmp_path)
    assert ran is False


def test_bool_param_with_value_passes_the_value():
    params = [{"name": "fresh", "flag": "--clear-cache-namespace", "value": "cmr_slc", "type": "bool"},
              {"name": "plain", "flag": "--verbose", "type": "bool"}]
    with patch.object(cli, "prompt_param", return_value="true"):
        flags, values = cli.build_flags(params)
    assert flags == ["--clear-cache-namespace", "cmr_slc", "--verbose"]
    assert values == {"fresh": "true", "plain": "true"}


def test_audit_entries_run_chunked_and_report_through_exit_codes():
    registry = yaml.safe_load((HERE / "registry.yaml").read_text())
    for product in ("CSLC_S1", "RTC_S1"):
        audit = registry["products"][product]["tools"]["audit"]
        assert audit["fixed_args"][-3:] == ["--low-memory", "--chunk-days", "1"]
        assert set(audit["exit_codes"]) == {0, 1, 2}
        names = {p["name"]: p for p in audit["params"]}
        assert names["output"]["default"].endswith(".jsonl")
        assert "format" not in names
        assert names["recheck_dates"]["flag"] == "--recheck-dates" and not names["recheck_dates"].get("required")
        assert names["fresh_slc_listing"]["value"] == "cmr_slc"
        assert "screen/tmux" in audit["notes"]
    assert "global_land_no_antarctica" in registry["products"]["RTC_S1"]["tools"]["audit"]["params"][2]["default"]


def test_extract_slcs_builds_the_list_from_a_chunked_report(tmp_path):
    report = tmp_path / "cslc_s1_burst_coverage.jsonl"
    miss = {"burst_pattern": "T100-213518-IW2", "acquisition_time": "2026-06-23T13:36:10+00:00", "platform": "S1A",
            "polarization": "VV", "slc_native_id": "S1A_IW_SLC__1SDV_20260623T133605_20260623T133632_065097_083497_2D92-SLC"}
    unv = {**miss, "burst_pattern": "T100-213519-IW2", "reason": "CMR status 500 after 3 attempts"}
    lines = [{"_type": "metadata", "start_datetime": "2026-06-23T00:00:00+00:00", "end_datetime": "2026-06-24T00:00:00+00:00"},
             {"_type": "chunk_result", "chunk_start": "2026-06-23T00:00:00+00:00", "chunk_end": "2026-06-24T00:00:00+00:00",
              "product_type": "CSLC-S1", "found_count": 5, "missing_count": 1, "unverified_count": 1,
              "found": [], "missing": [miss], "unverified": [unv]},
             {"_type": "summary", "total_slcs": 1, "failed_chunks": [], "unverified_days": ["2026-06-23"], "products": {}}]
    report.write_text("\n".join(json.dumps(l) for l in lines) + "\n")

    import subprocess
    out = subprocess.run([PYTHON, str(HERE / "scripts" / "extract_slcs.py"), str(report)], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert (tmp_path / "slcs-to-reprocess-cslc-s1.txt").read_text().splitlines() == [
        "S1A_IW_SLC__1SDV_20260623T133605_20260623T133632_065097_083497_2D92"]
    assert "UNVERIFIED" in out.stdout and "2026-06-23" in out.stdout
    detail = (tmp_path / "slcs-to-reprocess-cslc-s1-detail.csv").read_text().splitlines()
    assert detail[1].startswith("S1A_IW_SLC__1SDV_20260623T133605") and detail[1].endswith(",S1A,1,yes")


def _audit_report(tmp_path, missing):
    """A one-chunk CSLC-S1 report whose missing list is the given (slc_native_id, platform, acquisition_time) items."""
    report = tmp_path / "cslc_s1_burst_coverage.jsonl"
    items = [{"burst_pattern": "T100-213518-IW2", "acquisition_time": t, "platform": p, "polarization": "VV",
              "slc_native_id": s} for s, p, t in missing]
    lines = [{"_type": "metadata", "start_datetime": "2026-06-01T00:00:00+00:00", "end_datetime": "2026-07-01T00:00:00+00:00"},
             {"_type": "chunk_result", "chunk_start": "2026-06-01T00:00:00+00:00", "chunk_end": "2026-07-01T00:00:00+00:00",
              "product_type": "CSLC-S1", "found_count": 0, "missing_count": len(items), "unverified_count": 0,
              "found": [], "missing": items, "unverified": []},
             {"_type": "summary", "total_slcs": len(items), "failed_chunks": [], "unverified_days": [], "products": {}}]
    report.write_text("\n".join(json.dumps(l) for l in lines) + "\n")
    return report


S1D_EARLY = "S1D_IW_SLC__1SDV_20260605T235656_20260605T235726_003108_005629_D716-SLC"
S1D_LATE = "S1D_IW_SLC__1SDV_20260620T235656_20260620T235726_003327_005F00_AB12-SLC"
S1A_EARLY = "S1A_IW_SLC__1SDV_20260605T011255_20260605T011324_064827_082B10_EF7D-SLC"


def _extract(report, *flags):
    import subprocess
    out = subprocess.run([PYTHON, str(HERE / "scripts" / "extract_slcs.py"), str(report), *flags],
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    listed = (report.parent / "slcs-to-reprocess-cslc-s1.txt").read_text().split()
    excluded_csv = report.parent / "slcs-excluded-cslc-s1.csv"
    excluded = excluded_csv.read_text().splitlines()[1:] if excluded_csv.exists() else []
    return out.stdout, listed, excluded


def test_extract_slcs_leaves_s1d_before_the_agreed_start_off_the_list(tmp_path):
    report = _audit_report(tmp_path, [(S1D_EARLY, "S1D", "2026-06-05T23:56:56+00:00"),
                                      (S1D_LATE, "S1D", "2026-06-20T23:56:56+00:00"),
                                      (S1A_EARLY, "S1A", "2026-06-05T01:12:55+00:00")])
    stdout, listed, excluded = _extract(report)
    assert listed == sorted([S1A_EARLY[:-4], S1D_LATE[:-4]])          # the early S1A stays, only S1D has a start date
    assert len(excluded) == 1 and excluded[0].startswith(S1D_EARLY[:-4]) and excluded[0].endswith("S1D sensed before 2026-06-13")
    assert "1 SLCs left off the list (S1D sensed before 2026-06-13: 1)" in stdout
    assert "in 3 SLCs; listed 2 SLCs" in stdout


def test_extract_slcs_platform_start_can_be_moved_or_switched_off(tmp_path):
    report = _audit_report(tmp_path, [(S1D_EARLY, "S1D", "2026-06-05T23:56:56+00:00"),
                                      (S1D_LATE, "S1D", "2026-06-20T23:56:56+00:00")])
    _, listed, excluded = _extract(report, "--platform-start", "s1d=2026-07-01")  # replaces the default, case-insensitive
    assert listed == [] and len(excluded) == 2
    _, listed, excluded = _extract(report, "--no-platform-start")
    assert listed == sorted([S1D_EARLY[:-4], S1D_LATE[:-4]]) and excluded == []
    assert not (tmp_path / "slcs-excluded-cslc-s1.csv").exists()  # the previous run's exclusion file does not linger


def test_extract_slcs_rejects_a_malformed_platform_start(tmp_path):
    report = _audit_report(tmp_path, [(S1D_LATE, "S1D", "2026-06-20T23:56:56+00:00")])
    import subprocess
    for bad in ("S1D", "S1D=2026-13-01", "=2026-06-13"):
        out = subprocess.run([PYTHON, str(HERE / "scripts" / "extract_slcs.py"), str(report), "--platform-start", bad],
                             capture_output=True, text=True)
        assert out.returncode != 0 and "--platform-start" in out.stderr


def test_extract_slcs_marks_a_scene_with_some_products_as_not_fully_missing(tmp_path):
    report = _audit_report(tmp_path, [(S1D_LATE, "S1D", "2026-06-20T23:56:56+00:00"),
                                      (S1A_EARLY, "S1A", "2026-06-05T01:12:55+00:00")])
    lines = [json.loads(l) for l in report.read_text().splitlines()]
    chunk = next(l for l in lines if l["_type"] == "chunk_result")
    chunk["found"] = [{**chunk["missing"][0], "burst_pattern": "T100-213519-IW1"}]  # one burst of S1D_LATE has a product
    chunk["found_count"] = 1
    report.write_text("\n".join(json.dumps(l) for l in lines) + "\n")
    _extract(report)
    detail = {r.split(",")[0]: r.split(",")[-1] for r in (report.parent / "slcs-to-reprocess-cslc-s1-detail.csv").read_text().splitlines()[1:]}
    assert detail == {S1D_LATE[:-4]: "no", S1A_EARLY[:-4]: "yes"}
