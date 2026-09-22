# -*- coding: utf8 -*-

"""
Live endpoint access limit verification.

These tests run against a *live* Blitzortung JSON-RPC webservice and assert the
documented access limits:

* every data endpoint must refuse to serve data unless the ``User-Agent`` header
  matches ``bo-android-<int number>``;
* every data endpoint must refuse to serve data unless the request
  ``Content-Type`` is exactly ``text/json``;
* the global data endpoint must not serve data for a grid baseline below
  ``25000``.

The endpoint signals "blocked" by returning an empty JSON object (``{}``).
A successful grid request always contains the raster payload keys below, even
when there happen to be no strikes in the requested interval.  The tests use
that shape to tell "blocked" apart from "valid but empty".
"""

import pytest

from .endpoints import (
    DATA_ENDPOINTS,
    ENDPOINTS,
    GLOBAL_ENDPOINTS,
    GLOBAL_MIN_BASELINE,
    VALID_CONTENT_TYPE,
    VALID_USER_AGENT,
    assert_blocked,
    endpoint_params,
    get_result,
    is_grid_response,
)


# Every test in this module talks to the live endpoint and is therefore skipped
# unless a target URL is configured.
pytestmark = pytest.mark.live


INVALID_USER_AGENTS = (
    pytest.param("Mozilla/5.0 (Linux; Android 13)", id="browser"),
    pytest.param("", id="empty"),
    pytest.param(None, id="missing"),
    pytest.param("bo-android", id="no-version-suffix"),
    pytest.param("bo-android-", id="empty-version"),
    pytest.param("bo-android-abc", id="non-numeric-version"),
    pytest.param("bo-android--5", id="negative-version"),
    pytest.param("bo-android-0", id="zero-version"),
    pytest.param("bo_android_150", id="wrong-separator"),
    pytest.param("BO-ANDROID-150", id="uppercase"),
    pytest.param("python-requests/2.32", id="default-client"),
)

INVALID_CONTENT_TYPES = (
    pytest.param("application/json", id="application-json"),
    pytest.param("text/html", id="text-html"),
    pytest.param("text/plain", id="text-plain"),
    pytest.param(None, id="missing"),
)


# ---------------------------------------------------------------------------
# Sanity check that the endpoint is reachable at all.
# ---------------------------------------------------------------------------


class TestLiveEndpointReachability:
    """The endpoint is reachable and answers unauthenticated health checks."""

    def test_check_endpoint_responds(self, rpc_client):
        payload = rpc_client.call("check")
        result = get_result(payload)
        assert isinstance(result, dict), f"unexpected health response: {payload!r}"
        assert "count" in result


# ---------------------------------------------------------------------------
# Positive control: the suite can observe a successful grid response.
# ---------------------------------------------------------------------------


class TestValidRequestsAreServed:
    """Valid headers receive a grid response, proving the limits are testable."""

    @pytest.mark.parametrize("endpoint", endpoint_params(DATA_ENDPOINTS))
    def test_valid_request_returns_grid_structure(self, rpc_client, endpoint):
        payload = rpc_client.call(
            endpoint.method,
            endpoint.params,
            user_agent=VALID_USER_AGENT,
            content_type=VALID_CONTENT_TYPE,
        )
        result = get_result(payload)
        assert is_grid_response(result), (
            f"{endpoint.method} did not return a grid response for a valid "
            f"request: {payload!r}"
        )


# ---------------------------------------------------------------------------
# User agent limit.
# ---------------------------------------------------------------------------


class TestUserAgentLimit:
    """Every endpoint must reject requests whose user agent is not bo-android-<int>."""

    @pytest.mark.parametrize("endpoint", endpoint_params(ENDPOINTS))
    @pytest.mark.parametrize("user_agent", INVALID_USER_AGENTS)
    def test_invalid_user_agent_is_blocked(self, rpc_client, endpoint, user_agent):
        payload = rpc_client.call(
            endpoint.method,
            endpoint.params,
            user_agent=user_agent,
            content_type=VALID_CONTENT_TYPE,
        )
        assert_blocked(payload, f"{endpoint.method} with user agent {user_agent!r}")

    @pytest.mark.parametrize("endpoint", endpoint_params(DATA_ENDPOINTS))
    def test_minimal_numeric_user_agent_is_accepted(self, rpc_client, endpoint):
        """``bo-android-1`` is the smallest valid version and must be accepted."""
        payload = rpc_client.call(
            endpoint.method,
            endpoint.params,
            user_agent="bo-android-1",
            content_type=VALID_CONTENT_TYPE,
        )
        assert is_grid_response(get_result(payload)), (
            f"{endpoint.method} rejected the minimal valid user agent: {payload!r}"
        )


# ---------------------------------------------------------------------------
# Content type limit.
# ---------------------------------------------------------------------------


class TestContentTypeLimit:
    """Every endpoint must reject requests whose content type is not text/json."""

    @pytest.mark.parametrize("endpoint", endpoint_params(ENDPOINTS))
    @pytest.mark.parametrize("content_type", INVALID_CONTENT_TYPES)
    def test_invalid_content_type_is_blocked(self, rpc_client, endpoint, content_type):
        payload = rpc_client.call(
            endpoint.method,
            endpoint.params,
            user_agent=VALID_USER_AGENT,
            content_type=content_type,
        )
        assert_blocked(payload, f"{endpoint.method} with content type {content_type!r}")


# ---------------------------------------------------------------------------
# Global grid baseline limit.
# ---------------------------------------------------------------------------


class TestGlobalBaselineLimit:
    """Global endpoints must not serve data for a baseline below 25000."""

    @pytest.mark.parametrize("endpoint", endpoint_params(GLOBAL_ENDPOINTS))
    @pytest.mark.parametrize("baseline", [0, 1000, 5000, 10000, 24999])
    def test_baseline_below_limit_is_blocked(self, rpc_client, endpoint, baseline):
        params = (60, baseline, 0, 0)
        payload = rpc_client.call(
            endpoint.method,
            params,
            user_agent=VALID_USER_AGENT,
            content_type=VALID_CONTENT_TYPE,
        )
        assert_blocked(payload, f"{endpoint.method} with baseline {baseline}")

    @pytest.mark.parametrize("endpoint", endpoint_params(GLOBAL_ENDPOINTS))
    @pytest.mark.parametrize("baseline", [GLOBAL_MIN_BASELINE, 50000])
    def test_baseline_at_or_above_limit_is_served(self, rpc_client, endpoint, baseline):
        params = (60, baseline, 0, 0)
        payload = rpc_client.call(
            endpoint.method,
            params,
            user_agent=VALID_USER_AGENT,
            content_type=VALID_CONTENT_TYPE,
        )
        assert is_grid_response(get_result(payload)), (
            f"{endpoint.method} blocked baseline {baseline}, which is at or above "
            f"the limit: {payload!r}"
        )


# ---------------------------------------------------------------------------
# Documented always-blocked endpoint.
# ---------------------------------------------------------------------------


class TestGetStrikesEndpoint:
    """The legacy ``get_strikes`` endpoint is blocked even for valid requests."""

    def test_get_strikes_never_serves_data(self, rpc_client):
        payload = rpc_client.call(
            "get_strikes",
            (60, 0),
            user_agent=VALID_USER_AGENT,
            content_type=VALID_CONTENT_TYPE,
        )
        assert_blocked(payload, "get_strikes with valid headers")
