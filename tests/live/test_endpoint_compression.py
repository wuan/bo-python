# -*- coding: utf8 -*-

"""
Live endpoint payload compression verification.

The service compresses responses with gzip when *all* of the following hold:

* the request advertises ``Accept-Encoding: gzip``;
* the uncompressed response is at least 1000 bytes;
* the client's Android version is newer than ``MAX_COMPATIBLE_ANDROID_VERSION``
  (older clients have a broken decompressor, so the service strips the
  ``Accept-Encoding`` request header before rendering).

The regular format tests never see any of this because ``requests``
transparently decompresses bodies.  These tests use the raw client to inspect
the ``Content-Encoding`` header and the still-compressed bytes.
"""

import json

import pytest

from .client import normalize_jsonrpc_response
from .endpoints import (
    GLOBAL_MIN_BASELINE,
    REQUESTED_MINUTE_LENGTH,
    VALID_CONTENT_TYPE,
    VALID_USER_AGENT,
    Endpoint,
    get_result,
    is_grid_response,
)


# Every test in this module talks to the live endpoint and is therefore skipped
# unless a target URL is configured.
pytestmark = pytest.mark.live


# Below this uncompressed size the service skips gzip (see
# txjsonrpc_ng.web.render.Renderer.handle_compression).
COMPRESSION_THRESHOLD = 1000

# Oldest client version that can handle gzip responses
# (blitzortung.service.base.Server.MAX_COMPATIBLE_ANDROID_VERSION).
MAX_COMPATIBLE_ANDROID_VERSION = 177

# UAs on either side of the compatibility cut-off.
OLD_CLIENT_USER_AGENTS = (
    pytest.param("bo-android-1", id="version-1"),
    pytest.param("bo-android-100", id="version-100"),
    pytest.param(f"bo-android-{MAX_COMPATIBLE_ANDROID_VERSION}", id="max-version"),
)
NEW_CLIENT_USER_AGENTS = (
    pytest.param(f"bo-android-{MAX_COMPATIBLE_ANDROID_VERSION + 1}", id="first-new"),
    pytest.param(VALID_USER_AGENT, id="version-190"),
)

# A request that reliably yields a large payload on a busy service; used to
# exercise the compression path.  A small health check is used as the
# below-threshold counterpart.
LARGE_ENDPOINT = Endpoint(
    "get_global_strikes_grid",
    (REQUESTED_MINUTE_LENGTH, GLOBAL_MIN_BASELINE, 0, 0),
    global_data=True,
)
SMALL_ENDPOINT = Endpoint("check", ())


@pytest.fixture(scope="session")
def raw_fetch(rpc_client):
    """Return a cached ``(endpoint, user_agent, accept_encoding) -> RawResponse``.

    Caching keeps the number of live requests proportional to the number of
    distinct combinations actually asserted on.
    """
    cache = {}

    def fetch(endpoint, user_agent, accept_encoding):
        key = (endpoint, user_agent, accept_encoding)
        if key not in cache:
            headers = {}
            if accept_encoding is not None:
                headers["Accept-Encoding"] = accept_encoding
            cache[key] = rpc_client.post_raw(
                endpoint.method,
                endpoint.params,
                user_agent=user_agent,
                content_type=VALID_CONTENT_TYPE,
                headers=headers,
            )
        return cache[key]

    return fetch


def _decode_json(raw):
    """Decode a response body as JSON (identity transfer only)."""
    return json.loads(raw.decoded_body())


class TestGzipNegotiation:
    """Compression is applied only when requested and worthwhile."""

    def test_identity_encoding_is_not_compressed(self, raw_fetch):
        raw = raw_fetch(LARGE_ENDPOINT, VALID_USER_AGENT, "identity")
        assert raw.content_encoding == "", (
            f"identity request was compressed: {raw.content_encoding!r}"
        )
        assert get_result(_decode_json(raw)) is not None, (
            f"identity response is not valid JSON-RPC: {raw.body[:120]!r}"
        )

    def test_large_response_is_compressed_when_requested(self, raw_fetch):
        uncompressed = raw_fetch(LARGE_ENDPOINT, VALID_USER_AGENT, "identity")
        if len(uncompressed.body) < COMPRESSION_THRESHOLD:
            pytest.skip(
                f"payload below {COMPRESSION_THRESHOLD} byte compression threshold"
            )
        compressed = raw_fetch(LARGE_ENDPOINT, VALID_USER_AGENT, "gzip")
        assert compressed.content_encoding == "gzip", (
            f"expected gzip, got {compressed.content_encoding!r}"
        )

    def test_small_response_is_not_compressed(self, raw_fetch):
        raw = raw_fetch(SMALL_ENDPOINT, VALID_USER_AGENT, "gzip")
        assert len(raw.body) < COMPRESSION_THRESHOLD, (
            f"small endpoint unexpectedly returned {len(raw.body)} bytes"
        )
        assert raw.content_encoding == "", (
            f"small response was compressed: {raw.content_encoding!r}"
        )

    def test_compressed_body_decodes_to_valid_grid_response(self, raw_fetch):
        compressed = raw_fetch(LARGE_ENDPOINT, VALID_USER_AGENT, "gzip")
        if compressed.content_encoding != "gzip":
            pytest.skip("large endpoint was not compressed in this deployment")

        payload = json.loads(compressed.decoded_body())
        normalized = normalize_jsonrpc_response(payload, request_id=1)
        assert is_grid_response(get_result(normalized)), (
            f"decompressed body is not a grid response: {normalized!r}"
        )


class TestLegacyClientCompatibility:
    """Old Android clients never receive gzip-encoded responses."""

    @pytest.mark.parametrize("user_agent", OLD_CLIENT_USER_AGENTS)
    def test_old_clients_are_never_compressed(self, raw_fetch, user_agent):
        raw = raw_fetch(LARGE_ENDPOINT, user_agent, "gzip")
        assert raw.content_encoding == "", (
            f"{user_agent}: old client received {raw.content_encoding!r} encoding"
        )
        # The body must be plain, parseable JSON even for a large payload.
        assert get_result(_decode_json(raw)) is not None, (
            f"{user_agent}: response is not valid JSON-RPC"
        )

    @pytest.mark.parametrize("user_agent", NEW_CLIENT_USER_AGENTS)
    def test_new_clients_receive_gzip_for_large_payload(self, raw_fetch, user_agent):
        uncompressed = raw_fetch(LARGE_ENDPOINT, user_agent, "identity")
        if len(uncompressed.body) < COMPRESSION_THRESHOLD:
            pytest.skip(
                f"payload below {COMPRESSION_THRESHOLD} byte compression threshold"
            )
        compressed = raw_fetch(LARGE_ENDPOINT, user_agent, "gzip")
        assert compressed.content_encoding == "gzip", (
            f"{user_agent}: expected gzip, got {compressed.content_encoding!r}"
        )
