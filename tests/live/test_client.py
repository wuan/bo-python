# -*- coding: utf8 -*-

"""Offline tests for the JSON-RPC response normalization.

These tests do not talk to the network and therefore run even when no live
endpoint is configured. They guard against regressions such as the public
service's legacy pre-v1 array response being treated as a mapping
(``AttributeError: 'list' object has no attribute 'get'``).
"""

import json
from unittest.mock import Mock

from assertpy import assert_that

from .client import JsonRpcClient, normalize_jsonrpc_response


class TestNormalizeLegacyArrayResponse:
    """pre-v1 deployments answer with a bare JSON array."""

    def test_unwraps_single_result(self):
        normalized = normalize_jsonrpc_response([{"count": 7}], request_id=1)
        assert_that(normalized).is_equal_to(
            {"jsonrpc": "2.0", "id": 1, "result": {"count": 7}}
        )

    def test_unwraps_grid_result(self):
        grid = {"r": [], "t": "20260101T00:00:00"}
        normalized = normalize_jsonrpc_response([grid], request_id=2)
        assert_that(normalized["result"]).is_same_as(grid)

    def test_unwraps_none_result(self):
        normalized = normalize_jsonrpc_response([None], request_id=3)
        assert_that(normalized["result"]).is_none()

    def test_unwraps_fault(self):
        fault = {"faultCode": 8002, "faultString": "boom"}
        normalized = normalize_jsonrpc_response([fault], request_id=4)
        assert_that(normalized["result"]).is_equal_to(fault)

    def test_empty_array_yields_none(self):
        normalized = normalize_jsonrpc_response([], request_id=5)
        assert_that(normalized["result"]).is_none()


class TestNormalizeVersions:
    """v1 and v2 deployments answer with a JSON object."""

    def test_keeps_v2_response(self):
        response = {"jsonrpc": "2.0", "result": {"count": 1}, "id": 9}
        assert_that(normalize_jsonrpc_response(response, request_id=1)).is_same_as(
            response
        )

    def test_defaults_v1_response(self):
        response = {"result": {"count": 2}, "error": None, "id": 10}
        normalized = normalize_jsonrpc_response(response, request_id=1)
        assert_that(normalized["jsonrpc"]).is_equal_to("2.0")
        assert_that(normalized["result"]).is_equal_to({"count": 2})

    def test_scalar_response_is_wrapped(self):
        normalized = normalize_jsonrpc_response(True, request_id=6)
        assert_that(normalized["result"]).is_true()


class TestCallEnvelopeRequestShape:
    """Offline checks that ``call_envelope`` reproduces the legacy dialects.

    These guard the live protocol tests: if the request helper stopped
    omitting the ``jsonrpc`` member (or ignored the zero id), the live
    assertions would silently stop exercising the legacy path.
    """

    @staticmethod
    def _capture_payload(**kwargs):
        client = JsonRpcClient("http://example.invalid/")
        response = Mock()
        response.content = b"[]"
        response.json.return_value = []
        client._session = Mock()
        client._session.post.return_value = response
        client.call_envelope("get_strikes_grid", (1, 2), **kwargs)
        data = client._session.post.call_args.kwargs["data"]
        return json.loads(data)

    def test_default_request_is_versioned(self):
        payload = self._capture_payload()
        assert_that(payload["jsonrpc"]).is_equal_to("2.0")
        assert_that(payload["id"]).is_equal_to(1)

    def test_legacy_request_omits_version_and_uses_zero_id(self):
        payload = self._capture_payload(request_id=0, version_field=None)
        assert_that(payload).does_not_contain_key("jsonrpc")
        assert_that(payload["id"]).is_equal_to(0)

    def test_explicit_version_with_zero_id(self):
        payload = self._capture_payload(request_id=0, version_field="2.0")
        assert_that(payload["jsonrpc"]).is_equal_to("2.0")
        assert_that(payload["id"]).is_equal_to(0)
