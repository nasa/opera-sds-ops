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
