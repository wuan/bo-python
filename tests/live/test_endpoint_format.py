# -*- coding: utf8 -*-

"""
Live endpoint response-format verification.

These tests run against a *live* Blitzortung JSON-RPC webservice and validate
the *content* of successful responses.  They deliberately do **not** exercise
the access limits (user agent, content type, global baseline); those are
covered by ``test_endpoint_limits.py``.

Every request uses valid headers and a well-formed grid so that a live service
is expected to answer with real data.
"""

import datetime

import pytest

from .endpoints import (
    DATA_ENDPOINTS,
    GRID_RESPONSE_KEYS,
    HISTOGRAM_BIN_SIZE,
    REGION_DATA_ENDPOINTS,
    REQUESTED_MINUTE_LENGTH,
    TIMESTAMP_FORMAT,
    VALID_CONTENT_TYPE,
    VALID_USER_AGENT,
    endpoint_params,
    get_result,
)


# Every test in this module talks to the live endpoint and is therefore skipped
# unless a target URL is configured.
pytestmark = pytest.mark.live


@pytest.fixture(scope="session")
def grid_response_for(rpc_client):
    """Return a function that fetches and caches one grid response per endpoint.

    Caching keeps the number of live requests proportional to the number of
    endpoints instead of the number of assertions.
    """
    cache = {}

    def fetch(endpoint):
        if endpoint not in cache:
            payload = rpc_client.call(
                endpoint.method,
                endpoint.params,
                user_agent=VALID_USER_AGENT,
                content_type=VALID_CONTENT_TYPE,
            )
            cache[endpoint] = get_result(payload)
        return cache[endpoint]

    return fetch


def _is_int(value):
    """Return ``True`` for plain integers (excluding ``bool``)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value):
    """Return ``True`` for real numbers (excluding ``bool``)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


class TestJsonRpcEnvelope:
    """The JSON-RPC envelope of a successful call is well formed."""

    def test_response_has_result_and_id(self, rpc_client):
        payload = rpc_client.call("check")
        assert "result" in payload, f"missing result: {payload!r}"
        assert payload.get("id") is not None, f"missing id: {payload!r}"

    def test_response_is_not_an_error(self, rpc_client):
        payload = rpc_client.call("check")
        assert not payload.get("error"), f"unexpected error: {payload!r}"


class TestCheckEndpointFormat:
    """The ``check`` health endpoint returns a counter."""

    def test_returns_count_mapping(self, rpc_client):
        result = get_result(rpc_client.call("check"))
        assert isinstance(result, dict), f"expected mapping, got {result!r}"
        assert "count" in result, f"missing count: {result!r}"
        assert _is_int(result["count"]), f"count must be int: {result!r}"

    def test_count_increases_between_calls(self, rpc_client):
        first = get_result(rpc_client.call("check"))["count"]
        second = get_result(rpc_client.call("check"))["count"]
        assert second > first, f"count did not increase: {first} -> {second}"


class TestGridResponseFormat:
    """Successful grid endpoints return the documented raster payload."""

    @pytest.mark.parametrize("endpoint", endpoint_params(DATA_ENDPOINTS))
    def test_returns_mapping_with_all_grid_keys(self, grid_response_for, endpoint):
        result = grid_response_for(endpoint)
        assert isinstance(result, dict), (
            f"{endpoint.method} did not return a mapping: {result!r}"
        )
        missing = GRID_RESPONSE_KEYS - set(result)
        assert not missing, f"{endpoint.method} is missing keys {sorted(missing)}"

    @pytest.mark.parametrize("endpoint", endpoint_params(DATA_ENDPOINTS))
    def test_raster_entries_are_integer_quadruples(self, grid_response_for, endpoint):
        raster = grid_response_for(endpoint)["r"]
        assert isinstance(raster, list), f"{endpoint.method}: r must be a list"
        for entry in raster:
            assert isinstance(entry, (list, tuple)), (
                f"{endpoint.method}: raster entry is not a sequence: {entry!r}"
            )
            assert len(entry) == 4, (
                f"{endpoint.method}: raster entry must have 4 values: {entry!r}"
            )
            assert all(_is_int(value) for value in entry), (
                f"{endpoint.method}: raster values must be ints: {entry!r}"
            )

    @pytest.mark.parametrize("endpoint", endpoint_params(REGION_DATA_ENDPOINTS))
    def test_raster_coordinates_within_declared_dimensions(
        self, grid_response_for, endpoint
    ):
        # Region-bound endpoints address cells relative to the grid origin, so
        # both coordinates must fall inside the declared bin counts.  The
        # global endpoint uses offsets relative to the dateline/equator and is
        # intentionally excluded here.
        result = grid_response_for(endpoint)
        x_bins = result["xc"]
        y_bins = result["yc"]
        for entry in result["r"]:
            x_coordinate, y_coordinate = entry[0], entry[1]
            assert 0 <= x_coordinate < x_bins, (
                f"{endpoint.method}: x={x_coordinate} outside [0, {x_bins})"
            )
            assert 0 <= y_coordinate < y_bins, (
                f"{endpoint.method}: y={y_coordinate} outside [0, {y_bins})"
            )

    @pytest.mark.parametrize("endpoint", endpoint_params(DATA_ENDPOINTS))
    def test_cell_dimensions_are_positive(self, grid_response_for, endpoint):
        result = grid_response_for(endpoint)
        for key in ("xd", "yd"):
            assert _is_number(result[key]), f"{endpoint.method}: {key} not numeric"
            assert result[key] > 0, f"{endpoint.method}: {key} must be positive"

    @pytest.mark.parametrize("endpoint", endpoint_params(DATA_ENDPOINTS))
    def test_bin_counts_are_positive_integers(self, grid_response_for, endpoint):
        result = grid_response_for(endpoint)
        for key in ("xc", "yc"):
            assert _is_int(result[key]), f"{endpoint.method}: {key} must be int"
            assert result[key] > 0, f"{endpoint.method}: {key} must be positive"

    @pytest.mark.parametrize("endpoint", endpoint_params(DATA_ENDPOINTS))
    def test_grid_origin_and_extent_are_numeric(self, grid_response_for, endpoint):
        result = grid_response_for(endpoint)
        for key in ("x0", "y1"):
            assert _is_number(result[key]), (
                f"{endpoint.method}: {key} not numeric: {result[key]!r}"
            )

    @pytest.mark.parametrize("endpoint", endpoint_params(DATA_ENDPOINTS))
    def test_timestamp_matches_documented_format(self, grid_response_for, endpoint):
        timestamp = grid_response_for(endpoint)["t"]
        assert isinstance(timestamp, str), f"{endpoint.method}: t must be a string"
        parsed = datetime.datetime.strptime(timestamp, TIMESTAMP_FORMAT)
        now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
        assert abs((now - parsed).total_seconds()) < 24 * 3600, (
            f"{endpoint.method}: timestamp {timestamp} is not recent"
        )

    @pytest.mark.parametrize("endpoint", endpoint_params(DATA_ENDPOINTS))
    def test_duration_matches_requested_minutes(self, grid_response_for, endpoint):
        duration = grid_response_for(endpoint)["dt"]
        assert _is_int(duration), f"{endpoint.method}: dt must be int"
        assert duration == REQUESTED_MINUTE_LENGTH * 60, (
            f"{endpoint.method}: dt={duration} does not match requested "
            f"{REQUESTED_MINUTE_LENGTH} minutes"
        )

    @pytest.mark.parametrize("endpoint", endpoint_params(DATA_ENDPOINTS))
    def test_histogram_is_non_negative_integer_list(self, grid_response_for, endpoint):
        histogram = grid_response_for(endpoint)["h"]
        assert isinstance(histogram, list), f"{endpoint.method}: h must be a list"
        assert all(_is_int(value) and value >= 0 for value in histogram), (
            f"{endpoint.method}: histogram values must be non-negative ints: {histogram!r}"
        )

    @pytest.mark.parametrize("endpoint", endpoint_params(DATA_ENDPOINTS))
    def test_histogram_bucket_count_matches_duration(self, grid_response_for, endpoint):
        histogram = grid_response_for(endpoint)["h"]
        expected_buckets = REQUESTED_MINUTE_LENGTH // HISTOGRAM_BIN_SIZE
        assert len(histogram) == expected_buckets, (
            f"{endpoint.method}: expected {expected_buckets} histogram buckets, "
            f"got {len(histogram)}"
        )


class TestDeprecatedGetStrikes:
    """The legacy ``get_strikes`` endpoint returns no payload.

    It is kept here as a functional check of the current service behaviour.
    """

    def test_returns_no_payload(self, rpc_client):
        payload = rpc_client.call(
            "get_strikes",
            (60, 0),
            user_agent=VALID_USER_AGENT,
            content_type=VALID_CONTENT_TYPE,
        )
        assert get_result(payload) is None, f"unexpected payload: {payload!r}"
