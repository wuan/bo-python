# -*- coding: utf8 -*-

"""
Live endpoint test suite configuration.

This suite verifies a *running* Blitzortung JSON-RPC webservice against the
access limits that the public endpoint must enforce (user agent, content type
and global grid baseline).  It never runs as part of the regular unit test
suite unless a target URL is provided, so it stays safe for CI and offline
development.

Enable it with either::

    BLITZORTUNG_LIVE_URL=http://127.0.0.1:8080/ poetry run pytest tests/live
    poetry run pytest tests/live --live-url=http://127.0.0.1:8080/

The endpoint also enforces a request ``Content-Type`` of ``text/json`` and a
``User-Agent`` matching ``bo-android-<int>``; the suite can therefore only
observe the public behaviour of the service.
"""

import os

import pytest

from .client import JsonRpcClient


def pytest_addoption(parser):
    """Register live-endpoint specific command line options."""
    parser.addoption(
        "--live-url",
        action="store",
        default=None,
        help="Base URL of a live Blitzortung JSON-RPC endpoint to verify.",
    )
    parser.addoption(
        "--live-timeout",
        action="store",
        type=float,
        default=15.0,
        help="Timeout in seconds for live endpoint requests (default: 15).",
    )


def pytest_configure(config):
    """Register the ``live`` marker."""
    config.addinivalue_line(
        "markers",
        "live: verifies a live Blitzortung JSON-RPC endpoint",
    )


def _resolve_live_url(config):
    """Return the configured live endpoint URL, or ``None``."""
    url = config.getoption("--live-url") or os.environ.get("BLITZORTUNG_LIVE_URL")
    return url.rstrip("/") + "/" if url else None


def pytest_collection_modifyitems(config, items):
    """Skip tests that need an endpoint when none is configured.

    Offline tests (for example the response-normalization tests) do not carry
    the ``live`` marker and keep running without a target URL.
    """
    if _resolve_live_url(config):
        return
    skip = pytest.mark.skip(
        reason="live endpoint not configured "
        "(set BLITZORTUNG_LIVE_URL or pass --live-url)"
    )
    for item in items:
        if item.get_closest_marker("live") is not None:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def live_url(request):
    """URL of the live endpoint under test."""
    url = _resolve_live_url(request.config)
    assert url, "live endpoint URL is not configured"
    return url


@pytest.fixture(scope="session")
def live_timeout(request):
    """Per-request timeout for the live endpoint."""
    return request.config.getoption("--live-timeout")


@pytest.fixture(scope="session")
def rpc_client(live_url, live_timeout):
    """A JSON-RPC client bound to the live endpoint."""
    return JsonRpcClient(live_url, timeout=live_timeout)
