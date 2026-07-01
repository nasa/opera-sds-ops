"""Unit tests for ``opera_accountability.cmr._fatal_code``.

Regression coverage for the transport-error case: when the CMR client
raises ``ConnectionError`` / ``Timeout`` / DNS failures mid-pagination,
the exception has ``response is None``. The earlier implementation
unconditionally dereferenced ``err.response.status_code`` and crashed the
retry loop with ``AttributeError`` — which we observed on a real
DSWx-S1 accountability run partway through ~150k RTC-S1 granules.
"""

from __future__ import annotations

import requests

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
