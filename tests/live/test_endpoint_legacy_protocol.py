# -*- coding: utf8 -*-

"""Legacy JSON-RPC protocol verification for the live endpoint.

The Blitzortung Android client predates JSON-RPC 1.0: it sends a request with
no ``jsonrpc`` version field and a fixed ``id`` of ``0``, and expects the
bare-array pre-1.0 response envelope.  The public service must keep answering
that dialect for the deployed client versions, while still honouring requests
that are explicitly versioned.

These checks are deliberately separate from the response-format suite, which
normalizes the envelope away and can therefore not observe which dialect the
service chose.
"""

import pytest

from .endpoints import (
    LOCAL_BASELINE,
    REQUESTED_MINUTE_LENGTH,
    VALID_CONTENT_TYPE,
    VALID_USER_AGENT,
    get_result,
    is_grid_response,
)

pytestmark = pytest.mark.live

# Request id emitted by the legacy Android client (see JsonRpcClient.kt).
LEGACY_REQUEST_ID = 0

_GRID_PARAMS = (REQUESTED_MINUTE_LENGTH, LOCAL_BASELINE, 0, 1, 0)


def _grid_envelope(rpc_client, **kwargs):
    """Call ``get_strikes_grid`` and return the raw (non-normalized) envelope."""
    return rpc_client.call_envelope(
        "get_strikes_grid",
        _GRID_PARAMS,
        user_agent=VALID_USER_AGENT,
        content_type=VALID_CONTENT_TYPE,
        **kwargs,
    )


class TestLegacyZeroIdEnvelope:
    """An unversioned request with ``id`` 0 keeps the pre-1.0 bare array."""

    def test_zero_id_gets_bare_array(self, rpc_client):
        envelope = _grid_envelope(
            rpc_client, request_id=LEGACY_REQUEST_ID, version_field=None
        )
        assert isinstance(envelope, list), (
            "a legacy Android request (no jsonrpc field, id=0) must be answered "
            f"with the pre-1.0 array envelope, got {envelope!r}"
        )
        assert is_grid_response(get_result(envelope)), (
            f"legacy array does not contain a grid response: {envelope!r}"
        )


class TestVersionedEnvelopes:
    """Explicitly versioned requests get the matching response envelope."""

    def test_non_zero_id_gets_v1_object(self, rpc_client):
        envelope = _grid_envelope(rpc_client, request_id=1, version_field=None)
        assert isinstance(envelope, dict), (
            f"a JSON-RPC 1.0 request must get an object envelope, got {envelope!r}"
        )
        assert envelope.get("id") == 1, (
            f"v1 response does not echo the request id: {envelope!r}"
        )
        assert is_grid_response(envelope.get("result")), (
            f"v1 envelope does not contain a grid response: {envelope!r}"
        )

    def test_explicit_jsonrpc_2_zero_id_gets_v2_object(self, rpc_client):
        # The explicit ``jsonrpc`` member must win over the legacy zero-id
        # compatibility handling.
        envelope = _grid_envelope(rpc_client, request_id=0, version_field="2.0")
        assert isinstance(envelope, dict), (
            f"a JSON-RPC 2.0 request must get an object envelope, got {envelope!r}"
        )
        assert envelope.get("jsonrpc") == "2.0", (
            f"v2 response does not declare the version: {envelope!r}"
        )
        assert is_grid_response(envelope.get("result")), (
            f"v2 envelope does not contain a grid response: {envelope!r}"
        )
