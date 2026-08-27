"""Unit tests for memory-efficient batched duplicate detection."""

from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest

from opera_accountability.duplicates import detect_duplicates_memory_efficient, _generate_time_chunks


class TestGenerateTimeChunks:
    def test_single_chunk_for_short_range(self):
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 15)
        chunks = list(_generate_time_chunks(start, end, chunk_days=30))
        assert len(chunks) == 1
        assert chunks[0] == (start, end)

    def test_multiple_chunks_for_long_range(self):
        start = datetime(2025, 1, 1)
        end = datetime(2025, 3, 15)
        chunks = list(_generate_time_chunks(start, end, chunk_days=30))
        assert len(chunks) == 3
        assert chunks[0][0] == start
        assert chunks[-1][1] == end

    def test_defaults_to_one_year_if_no_start(self):
        from datetime import timedelta
        end = datetime.now() + timedelta(days=30)
        chunks = list(_generate_time_chunks(None, end, chunk_days=30))
        assert len(chunks) >= 12

    def test_defaults_to_now_if_no_end(self):
        chunks = list(_generate_time_chunks(datetime(2025, 1, 1), None, chunk_days=30))
        assert len(chunks) >= 1


class TestDetectDuplicatesMemoryEfficient:
    @patch("opera_accountability.duplicates.query_cmr")
    def test_basic_deduplication(self, mock_query_cmr):
        mock_query_cmr.return_value = [
            {"umm": {"GranuleUR": "OPERA_L3_DSWx-HLS_T10TEM_20250115T180931Z_20250115T235959Z_L8_30_v1.0"}},
            {"umm": {"GranuleUR": "OPERA_L3_DSWx-HLS_T10TEM_20250115T180931Z_20250116T003045Z_L8_30_v1.0"}},
            {"umm": {"GranuleUR": "OPERA_L3_DSWx-HLS_T11SKA_20250115T183045Z_20250115T230000Z_S2A_30_v1.0"}},
        ]

        result = detect_duplicates_memory_efficient(
            "DSWX_HLS",
            start_date=datetime(2025, 1, 15),
            end_date=datetime(2025, 1, 16),
            venue="PROD",
            chunk_days=30
        )

        assert result["total"] == 3
        assert result["unique"] == 2
        assert result["duplicates"] == 1

    @patch("opera_accountability.duplicates.query_cmr")
    def test_no_granules_returns_zeros(self, mock_query_cmr):
        mock_query_cmr.return_value = []

        result = detect_duplicates_memory_efficient(
            "DSWX_HLS",
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 2),
            venue="PROD",
        )

        assert result["total"] == 0
        assert result["unique"] == 0
        assert result["duplicates"] == 0

    @patch("opera_accountability.duplicates.query_cmr")
    def test_cross_chunk_deduplication(self, mock_query_cmr):
        g1 = {"umm": {"GranuleUR": "OPERA_L3_DSWx-HLS_T10TEM_20250115T180931Z_20250115T235959Z_L8_30_v1.0"}}
        g2 = {"umm": {"GranuleUR": "OPERA_L3_DSWx-HLS_T11SKA_20250215T183045Z_20250215T230000Z_S2A_30_v1.0"}}
        mock_query_cmr.side_effect = [[g1], [g1, g2]]

        result = detect_duplicates_memory_efficient(
            "DSWX_HLS",
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 3, 1),
            venue="PROD",
            chunk_days=30
        )

        assert result["unique"] == 2
        assert result["duplicates"] == 0

    def test_raises_on_invalid_venue(self):
        with pytest.raises(ValueError, match="No CCID configured"):
            detect_duplicates_memory_efficient(
                "DSWX_HLS",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                venue="UAT",
            )

    @patch("opera_accountability.duplicates.query_cmr")
    def test_parse_failures_tracked(self, mock_query_cmr):
        mock_query_cmr.return_value = [
            {"umm": {"GranuleUR": "NOT_A_VALID_GRANULE_ID"}},
            {"umm": {"GranuleUR": "OPERA_L3_DSWx-HLS_T10TEM_20250115T180931Z_20250115T235959Z_L8_30_v1.0"}},
        ]

        result = detect_duplicates_memory_efficient(
            "DSWX_HLS",
            start_date=datetime(2025, 1, 15),
            end_date=datetime(2025, 1, 16),
            venue="PROD",
        )

        assert result["parse_failures"] == 1
        assert result["unique"] == 1
