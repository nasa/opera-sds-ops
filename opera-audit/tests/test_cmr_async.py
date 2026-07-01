"""Unit tests for the async CMR client module."""

import pytest

from opera_accountability.cmr_async import params_to_request_body, paramss_to_request_body


class TestParamsToRequestBody:
    def test_simple_params(self):
        params = {"ShortName": "OPERA_L3_DSWx-HLS", "provider": "POCLOUD"}
        body = params_to_request_body(params)
        assert "&ShortName=OPERA_L3_DSWx-HLS" in body
        assert "&provider=POCLOUD" in body

    def test_iterable_params_get_bracket_suffix(self):
        params = {"native-id": ["id1", "id2", "id3"]}
        body = params_to_request_body(params)
        assert "&native-id[]=id1" in body
        assert "&native-id[]=id2" in body
        assert "&native-id[]=id3" in body

    def test_already_bracketed_key(self):
        params = {"native-id[]": ["id1", "id2"]}
        body = params_to_request_body(params)
        assert "&native-id[]=id1" in body
        assert "&native-id[]=id2" in body
        assert "native-id[][]" not in body

    def test_none_token_skipped(self):
        params = {"token": None, "ShortName": "test"}
        body = params_to_request_body(params)
        assert "token" not in body
        assert "&ShortName=test" in body

    def test_token_with_value_included(self):
        params = {"token": "abc123", "ShortName": "test"}
        body = params_to_request_body(params)
        assert "&token=abc123" in body

    def test_empty_params(self):
        assert params_to_request_body({}) == ""


class TestParamssToRequestBody:
    def test_multiple_param_sets(self):
        paramss = [
            {"ShortName": "A"},
            {"ShortName": "B"},
        ]
        bodies = paramss_to_request_body(paramss)
        assert len(bodies) == 2
        assert "&ShortName=A" in bodies[0]
        assert "&ShortName=B" in bodies[1]
