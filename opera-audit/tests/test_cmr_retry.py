"""Unit tests for ``opera_accountability.cmr._fatal_code``.

Regression coverage for the transport-error case: when the CMR client
raises ``ConnectionError`` / ``Timeout`` / DNS failures mid-pagination,
the exception has ``response is None``. The earlier implementation
unconditionally dereferenced ``err.response.status_code`` and crashed the
retry loop with ``AttributeError`` — which we observed on a real
DSWx-S1 accountability run partway through ~150k RTC-S1 granules.
"""

from __future__ import annotations

import asyncio

import requests

from opera_accountability import cmr
from opera_accountability.cmr import _fatal_code


def _http_error_with_status(status: int) -> requests.exceptions.HTTPError:
    """Build an HTTPError carrying a real ``Response`` with ``status``."""
    response = requests.models.Response()
    response.status_code = status
    err = requests.exceptions.HTTPError(response=response)
    return err


def test_fatal_code_keeps_retrying_on_transient_status_codes():
    for status in (429, 500, 502, 503, 504):
        assert _fatal_code(_http_error_with_status(status)) is False, (
            f"Status {status} is documented as retryable but _fatal_code "
            f"returned True (giving up)."
        )


def test_fatal_code_gives_up_on_permanent_status_codes():
    for status in (400, 404, 422):
        assert _fatal_code(_http_error_with_status(status)) is True, (
            f"Status {status} should be terminal but _fatal_code returned "
            f"False (would retry forever)."
        )


def test_fatal_code_retries_when_response_is_none():
    """ConnectionError / Timeout / DNS failures have ``response=None``.

    Before the fix this raised ``AttributeError: 'NoneType' object has no
    attribute 'status_code'`` from inside the backoff retry handler,
    aborting the loop. The function must instead return ``False`` so the
    backoff library keeps retrying until ``max_time`` is reached.
    """
    err = requests.exceptions.ConnectionError("connection reset by peer")
    assert err.response is None
    assert _fatal_code(err) is False


def test_fatal_code_retries_on_timeout_with_no_response():
    err = requests.exceptions.Timeout("read timed out")
    assert err.response is None
    assert _fatal_code(err) is False


def test_query_by_native_id_patterns_uses_search_after(monkeypatch):
    calls = []

    def fake_request(url, params, headers=None):
        calls.append((url, params, headers or {}))
        if len(calls) == 1:
            return ([{"id": "first"}], "next-token")
        return ([{"id": "second"}], None)

    monkeypatch.setattr(cmr, "_do_cmr_request", fake_request)
    results = cmr.query_cmr_by_native_id_patterns(
        "C123-TEST",
        ["OPERA_L2_RTC-S1_T001-000001-IW1_*"],
        venue="PROD",
    )

    assert results == [{"id": "first"}, {"id": "second"}]
    assert calls[0][1]["native-id"] == ["OPERA_L2_RTC-S1_T001-000001-IW1_*"]
    assert calls[0][1]["options[native-id][pattern]"] == "true"
    assert calls[1][2] == {"CMR-Search-After": "next-token"}


def test_query_cmr_post_uses_search_after(monkeypatch):
    calls = []

    def fake_request(url, data, headers=None):
        calls.append((url, data, headers or {}))
        if len(calls) == 1:
            return ({"items": [{"id": "first"}]}, "post-token")
        return ({"items": [{"id": "second"}]}, None)

    monkeypatch.setattr(cmr, "_do_cmr_post_request", fake_request)
    results = cmr.query_cmr_post("provider=ASF", url="https://cmr.test")

    assert results == [{"id": "first"}, {"id": "second"}]
    assert "page_size=2000" in calls[0][1]
    assert calls[1][2] == {"CMR-Search-After": "post-token"}


def test_async_cmr_post_items_uses_search_after(monkeypatch):
    calls = []

    async def fake_post(session, url, data, headers):
        del session, url, data
        calls.append(dict(headers))
        if len(calls) == 1:
            return ({"hits": 3, "items": [{"id": 1}, {"id": 2}]}, "next")
        return ({"hits": 3, "items": [{"id": 3}]}, None)

    monkeypatch.setattr(cmr, "_async_post_json", fake_post)
    monkeypatch.setitem(cmr.CONFIG["cmr"], "page_size", 2)
    results = asyncio.run(
        cmr.async_cmr_post_items("https://cmr.test", "provider=ASF", object())
    )

    assert results == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert calls[1]["CMR-Search-After"] == "next"
