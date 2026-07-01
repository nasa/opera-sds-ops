"""Unit tests for recovery_file.py output format."""

import json
from pathlib import Path

import pytest

from opera_accountability.recovery_file import write_recovery_file, write_recovery_files_by_date


class TestWriteRecoveryFile:
    def test_txt_format_writes_one_id_per_line(self, tmp_path):
        results = {"missing": ["granule_A", "granule_B", "granule_C"]}
        output_path = str(tmp_path / "recovery_DSWX_HLS")

        path = write_recovery_file(results, output_path, "txt")

        assert path.endswith(".txt")
        lines = Path(path).read_text().strip().splitlines()
        assert lines == ["granule_A", "granule_B", "granule_C"]

    def test_json_format_writes_structured_output(self, tmp_path):
        results = {
            "missing": ["granule_A", "granule_B"],
            "strategy": "dswx_hls",
            "product": "DSWX_HLS",
        }
        output_path = str(tmp_path / "recovery_DSWX_HLS")

        path = write_recovery_file(results, output_path, "json")

        assert path.endswith(".json")
        data = json.loads(Path(path).read_text())
        assert data["missing_count"] == 2
        assert data["missing_ids"] == ["granule_A", "granule_B"]
        assert data["strategy"] == "dswx_hls"
        assert data["product"] == "DSWX_HLS"

    def test_empty_missing_list(self, tmp_path):
        results = {"missing": []}
        output_path = str(tmp_path / "recovery_empty")

        path = write_recovery_file(results, output_path, "txt")

        content = Path(path).read_text()
        assert content == ""

    def test_unsupported_format_raises(self, tmp_path):
        results = {"missing": ["id1"]}
        with pytest.raises(ValueError, match="Unsupported format"):
            write_recovery_file(results, str(tmp_path / "out"), "csv")

    def test_missing_key_defaults_to_empty(self, tmp_path):
        results = {}
        output_path = str(tmp_path / "recovery_no_missing")

        path = write_recovery_file(results, output_path, "txt")
        assert Path(path).read_text() == ""


class TestWriteRecoveryFilesByDate:
    def test_date_count_strategy_generates_per_date_files(self, tmp_path):
        results = {
            "date_counts": {
                "2025-01-01": 4,
                "2025-01-02": 2,
                "2025-01-03": 4,
            },
            "expected_per_day": 4,
        }

        files = write_recovery_files_by_date(results, str(tmp_path), "txt")

        assert len(files) == 1
        assert "2025-01-02" in files[0]

    def test_date_count_json_format(self, tmp_path):
        results = {
            "date_counts": {
                "2025-01-01": 0,
            },
            "expected_per_day": 4,
        }

        files = write_recovery_files_by_date(results, str(tmp_path), "json")

        assert len(files) == 1
        data = json.loads(Path(files[0]).read_text())
        assert data["date"] == "2025-01-01"
        assert data["expected"] == 4
        assert data["actual"] == 0
        assert data["missing"] == 4

    def test_fallback_to_missing_list(self, tmp_path):
        results = {"missing": ["id_a", "id_b"]}

        files = write_recovery_files_by_date(results, str(tmp_path), "txt")

        assert len(files) == 1
        content = Path(files[0]).read_text().strip().splitlines()
        assert content == ["id_a", "id_b"]

    def test_no_missing_dates_returns_empty(self, tmp_path):
        results = {
            "date_counts": {"2025-01-01": 4, "2025-01-02": 4},
            "expected_per_day": 4,
        }

        files = write_recovery_files_by_date(results, str(tmp_path), "txt")
        assert files == []
