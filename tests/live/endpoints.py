# -*- coding: utf8 -*-

"""Shared endpoint descriptions and response helpers for the live suite.

The definitions here are used by both the access-limit tests
(``test_endpoint_limits.py``) and the response-format tests
(``test_endpoint_format.py``).
"""

from dataclasses import dataclass

import pytest


# Exact values that the live endpoint must enforce.
VALID_USER_AGENT = "bo-android-190"
VALID_CONTENT_TYPE = "text/json"
GLOBAL_MIN_BASELINE = 25000

# The raster payload keys of a successful grid response
# (see ``strike_grid.build_grid_response``).
GRID_RESPONSE_KEYS = frozenset({"r", "xd", "yd", "x0", "y1", "xc", "yc", "t", "dt", "h"})

# Timestamp layout used by the ``t`` field.
TIMESTAMP_FORMAT = "%Y%m%dT%H:%M:%S"

# Histogram bucket size in minutes (see ``HistogramQuery``).
HISTOGRAM_BIN_SIZE = 5


@dataclass(frozen=True)
class Endpoint:
    """Description of a JSON-RPC endpoint and how it should behave."""

    method: str
    params: tuple
    global_data: bool = False
    # Kept for completeness: the ``get_strikes`` endpoint is currently blocked
    # for every caller regardless of the request headers.
    always_blocked: bool = False


# The grid baseline used for the "happy path" requests of the non-global
# endpoints (must be >= the local minimum enforced by the service).
LOCAL_BASELINE = 10000

# Minute length requested by the format tests; the grid response duration and
# histogram bucket count are derived from it.
REQUESTED_MINUTE_LENGTH = 60

# ---------------------------------------------------------------------------
# Cache-busting local-grid positions.
# ---------------------------------------------------------------------------
#
# The service caches a grid result under ``(creator, args..., kwargs...)``, so
# an identical request is answered from the cache and never recomputed.  To
# exercise a genuinely cold path a run can walk distinct ``get_local_strikes_grid``
# positions instead: every position is a separate cache key.  The position grid
# spans the globe in ``data_area``-degree steps, giving 180 / data_area latitude
# steps and 360 / data_area longitude steps; with the default 5-degree data area
# that is 36 x 72 = 2592 distinct positions.
CACHE_BUST_METHOD = "get_local_strikes_grid"
CACHE_BUST_BASELINE = 5000
CACHE_BUST_DATA_AREA = 5
CACHE_BUST_LATITUDE_STEPS = 180 // CACHE_BUST_DATA_AREA
CACHE_BUST_LONGITUDE_STEPS = 360 // CACHE_BUST_DATA_AREA
CACHE_BUST_POSITIONS = CACHE_BUST_LATITUDE_STEPS * CACHE_BUST_LONGITUDE_STEPS

ENDPOINTS = (
    Endpoint("get_strikes", (60, 0), always_blocked=True),
    Endpoint("get_strikes_grid", (REQUESTED_MINUTE_LENGTH, LOCAL_BASELINE, 0, 1, 0)),
    Endpoint("get_strikes_raster", (REQUESTED_MINUTE_LENGTH, LOCAL_BASELINE, 0, 1)),
    Endpoint("get_strokes_raster", (REQUESTED_MINUTE_LENGTH, LOCAL_BASELINE, 0, 1)),
    # x/y must describe a valid local grid (latitude within [-90, 90]);
    # (5, 5) is a safe, well-covered location with the default data area.
    Endpoint(
        "get_local_strikes_grid",
        (5, 5, LOCAL_BASELINE, REQUESTED_MINUTE_LENGTH, 0, 0),
    ),
    Endpoint(
        "get_global_strikes_grid",
        (REQUESTED_MINUTE_LENGTH, GLOBAL_MIN_BASELINE, 0, 0),
        global_data=True,
    ),
)

GLOBAL_ENDPOINTS = tuple(endpoint for endpoint in ENDPOINTS if endpoint.global_data)
DATA_ENDPOINTS = tuple(endpoint for endpoint in ENDPOINTS if not endpoint.always_blocked)
# Global grid coordinates are relative offsets that may be negative, so the
# "coordinates inside the declared bin range" check only applies to the
# region-bound endpoints.
REGION_DATA_ENDPOINTS = tuple(
    endpoint for endpoint in DATA_ENDPOINTS if not endpoint.global_data
)


def endpoint_params(endpoints):
    """Build readable pytest parameters for an iterable of endpoints."""
    return [pytest.param(endpoint, id=endpoint.method) for endpoint in endpoints]


def cache_bust_position(index):
    """Map a running ``index`` onto a distinct local-grid position.

    ``get_local_strikes_grid`` addresses its center as 1-based ``x``/``y``
    cells of ``data_area`` degrees (see ``LocalGrid``).  The positions tile the
    globe from -180/+90 degrees onwards so that consecutive indices move by one
    cell and cycle after :data:`CACHE_BUST_POSITIONS` calls.
    """
    index %= CACHE_BUST_POSITIONS
    longitude_index = index % CACHE_BUST_LONGITUDE_STEPS
    latitude_index = index // CACHE_BUST_LONGITUDE_STEPS
    x = longitude_index - CACHE_BUST_LONGITUDE_STEPS // 2 + 1
    y = latitude_index - CACHE_BUST_LATITUDE_STEPS // 2 + 1
    return x, y


def cache_bust_params(index, minute_length=REQUESTED_MINUTE_LENGTH, minute_offset=0, count_threshold=0):
    """Build ``get_local_strikes_grid`` params for the ``index``-th position."""
    x, y = cache_bust_position(index)
    return (x, y, CACHE_BUST_BASELINE, minute_length, minute_offset, count_threshold)


def get_result(payload):
    """Return the JSON-RPC result, or ``None`` when the call failed.

    Accepts both the normalized (dict) form and the legacy pre-v1 array form
    that some deployments return.
    """
    if isinstance(payload, list):
        result = payload[0] if payload else None
        # A legacy fault is delivered as the single array element.
        return None if isinstance(result, dict) and "faultCode" in result else result
    if isinstance(payload, dict):
        if payload.get("error"):
            return None
        return payload.get("result")
    return None


def is_grid_response(result):
    """Return ``True`` when ``result`` has the shape of a grid response."""
    return isinstance(result, dict) and GRID_RESPONSE_KEYS.issubset(result.keys())


def assert_blocked(payload, context):
    """Assert that the endpoint did not serve grid data."""
    result = get_result(payload)
    assert not is_grid_response(result), (
        f"{context}: endpoint unexpectedly served grid data: {result!r}"
    )
