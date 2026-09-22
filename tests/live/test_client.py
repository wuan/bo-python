# -*- coding: utf8 -*-

"""Offline tests for the JSON-RPC response normalization.

These tests do not talk to the network and therefore run even when no live
endpoint is configured. They guard against regressions such as the public
service's legacy pre-v1 array response being treated as a mapping
(``AttributeError: 'list' object has no attribute 'get'``).
"""

from assertpy import assert_that

from .client import normalize_jsonrpc_response


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
