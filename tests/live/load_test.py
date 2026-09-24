# -*- coding: utf8 -*-

"""Concurrent load-test driver for a live Blitzortung JSON-RPC endpoint.

This script is the load-testing counterpart to the correctness checks under
``tests/live/``.  It reuses the same :class:`~tests.live.client.JsonRpcClient`
and endpoint definitions, but instead of asserting a single response it drives
the endpoint with a configurable number of concurrent workers and reports
throughput and latency percentiles::

    python tests/live/load_test.py --url http://127.0.0.1:8080/ \
        --method get_local_strikes_grid --concurrency 20 --duration 30

The target can also be provided through ``BLITZORTUNG_LIVE_URL``.  Run with
``--requests`` to send a fixed total number of calls instead of a timed run.
The script exits with a non-zero status when any request fails at the
transport level.

The service caches a grid result by method and parameters, so repeated
identical requests are answered from the cache and never reach the database.
Pass ``--cache-bust`` to walk distinct ``get_local_strikes_grid`` positions
(one per request) and measure the cold path instead.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from tests.live.client import JsonRpcClient  # pylint: disable=wrong-import-position
from tests.live.endpoints import (  # pylint: disable=wrong-import-position
    CACHE_BUST_METHOD,
    CACHE_BUST_POSITIONS,
    ENDPOINTS,
    VALID_CONTENT_TYPE,
    VALID_USER_AGENT,
    cache_bust_params,
    get_result,
    is_grid_response,
)

DEFAULT_METHOD = "get_local_strikes_grid"
DEFAULT_CONCURRENCY = 10
DEFAULT_DURATION = 30.0
DEFAULT_WARMUP = 2.0
DEFAULT_TIMEOUT = 15.0

ENDPOINT_BY_METHOD = {endpoint.method: endpoint for endpoint in ENDPOINTS}


@dataclass(frozen=True)
class _Request:
    """The static part of every request sent by the load test."""

    method: str
    params: tuple
    user_agent: str
    content_type: str


class _Budget:
    """Thread-safe total request budget.

    ``total`` of ``None`` disables the budget (used for timed runs).
    """

    def __init__(self, total: int | None) -> None:
        self._lock = threading.Lock()
        self._remaining = total

    def take(self) -> bool:
        """Reserve one request, returning ``False`` once the budget is spent."""
        with self._lock:
            if self._remaining is None:
                return True
            if self._remaining <= 0:
                return False
            self._remaining -= 1
            return True


class _Counter:
    """Thread-safe monotonic counter handing out cache-bust positions."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value = 0

    def next(self) -> int:
        """Return the next index, wrapping after one full globe sweep."""
        with self._lock:
            value = self._value % CACHE_BUST_POSITIONS
            self._value += 1
            return value


def _is_valid(method: str, payload) -> bool:
    """Return whether a response has the shape expected for ``method``."""
    result = get_result(payload)
    endpoint = ENDPOINT_BY_METHOD.get(method)

    if method == "check":
        return isinstance(result, dict) and "count" in result
    if endpoint is not None and endpoint.always_blocked:
        return result is None
    if endpoint is not None:
        return is_grid_response(result)
    return result is not None


def _worker(url, timeout, request, deadline, budget, positions=None):
    """Drive requests until the deadline or request budget is exhausted."""
    client = JsonRpcClient(url, timeout=timeout)
    samples = []
    while budget.take():
        if deadline is not None and time.monotonic() >= deadline:
            break
        params = (
            cache_bust_params(positions.next())
            if positions is not None
            else request.params
        )
        start = time.perf_counter()
        try:
            payload = client.call(
                request.method,
                params,
                user_agent=request.user_agent,
                content_type=request.content_type,
            )
            latency = time.perf_counter() - start
            samples.append((latency, _is_valid(request.method, payload), None))
        except Exception as error:  # pylint: disable=broad-except
            latency = time.perf_counter() - start
            samples.append(
                (latency, False, "%s: %s" % (type(error).__name__, error))
            )
    return samples


def _percentile(ordered, fraction):
    """Return the nearest-rank percentile of an already sorted sequence."""
    if not ordered:
        return 0.0
    index = int(round(fraction * (len(ordered) - 1)))
    return ordered[index]


def _resolve_params(method: str, raw_params: str | None) -> tuple:
    """Determine the JSON-RPC params for ``method``.

    An explicit ``--params`` JSON array always wins; otherwise the well-known
    parameters of a live-suite endpoint are used.
    """
    if raw_params is not None:
        params = json.loads(raw_params)
        if not isinstance(params, list):
            raise SystemExit("--params must be a JSON array, e.g. '[60, 10000, 0, 1, 0]'")
        return tuple(params)

    endpoint = ENDPOINT_BY_METHOD.get(method)
    if endpoint is not None:
        return endpoint.params
    if method == "check":
        return ()
    raise SystemExit("unknown method %r; pass --params explicitly" % method)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a concurrent load test against a live Blitzortung JSON-RPC endpoint.",
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("BLITZORTUNG_LIVE_URL"),
        help="Base URL of the live endpoint (default: $BLITZORTUNG_LIVE_URL).",
    )
    parser.add_argument(
        "--method",
        default=DEFAULT_METHOD,
        help="JSON-RPC method to call (default: %(default)s).",
    )
    parser.add_argument(
        "--params",
        default=None,
        help="Params as a JSON array; defaults to the live-suite values for --method.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="Number of concurrent workers (default: %(default)s).",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=DEFAULT_DURATION,
        help="Measured run length in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=None,
        help="Fixed total request budget; replaces --duration when set.",
    )
    parser.add_argument(
        "--warmup",
        type=float,
        default=DEFAULT_WARMUP,
        help="Warm-up time in seconds before measuring (default: %(default)s).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help="Per-request timeout in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--user-agent",
        default=VALID_USER_AGENT,
        help="User-Agent header to send (default: %(default)s).",
    )
    parser.add_argument(
        "--content-type",
        default=VALID_CONTENT_TYPE,
        help="Content-Type header to send (default: %(default)s).",
    )
    parser.add_argument(
        "--cache-bust",
        action="store_true",
        help=(
            "Rotate the %s position on every request so responses are not "
            "served from the server cache." % CACHE_BUST_METHOD
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Print the summary as JSON instead of a table.",
    )
    return parser


def _warmup(request, url, timeout, seconds):
    """Send requests for ``seconds`` and discard the results.

    Always performs at least one request so connectivity is verified.
    """
    client = JsonRpcClient(url, timeout=timeout)
    deadline = time.monotonic() + seconds
    count = 0
    while True:
        client.call(
            request.method,
            request.params,
            user_agent=request.user_agent,
            content_type=request.content_type,
        )
        count += 1
        if seconds <= 0 or time.monotonic() >= deadline:
            return count


def _run(request, url, timeout, concurrency, duration, total_requests, positions=None):
    """Run the measured phase and return ``(samples, elapsed)``."""
    budget = _Budget(total_requests)
    deadline = None if total_requests is not None else time.monotonic() + duration

    elapsed = 0.0
    executor = ThreadPoolExecutor(max_workers=concurrency)
    try:
        start = time.perf_counter()
        futures = [
            executor.submit(_worker, url, timeout, request, deadline, budget, positions)
            for _ in range(concurrency)
        ]
        samples = []
        for future in futures:
            samples.extend(future.result())
        elapsed = time.perf_counter() - start
    finally:
        executor.shutdown(wait=True)
    return samples, elapsed


def _summarize(samples, elapsed):
    """Aggregate raw samples into the reported metrics."""
    latencies = sorted(sample[0] for sample in samples)
    errors = [sample for sample in samples if sample[2] is not None]
    invalid = [sample for sample in samples if sample[2] is None and not sample[1]]
    total = len(samples)
    valid = total - len(errors) - len(invalid)

    return {
        "total": total,
        "valid": valid,
        "invalid": len(invalid),
        "errors": len(errors),
        "error_samples": [sample[2] for sample in errors[:10]],
        "throughput": total / elapsed if elapsed > 0 else 0.0,
        "elapsed": elapsed,
        "latency_ms": {
            "min": _percentile(latencies, 0.0) * 1000,
            "mean": (sum(latencies) / total * 1000) if total else 0.0,
            "p50": _percentile(latencies, 0.50) * 1000,
            "p90": _percentile(latencies, 0.90) * 1000,
            "p95": _percentile(latencies, 0.95) * 1000,
            "p99": _percentile(latencies, 0.99) * 1000,
            "max": (latencies[-1] * 1000) if latencies else 0.0,
        },
    }


def _print_report(url, request, args, summary):
    """Print the human-readable report."""
    latency = summary["latency_ms"]
    print(f"target:      {url}")
    print(f"method:      {request.method}")
    if args.cache_bust:
        print(f"params:      rotating {CACHE_BUST_POSITIONS} local-grid positions")
    else:
        print(f"params:      {list(request.params)}")
    print(f"concurrency: {args.concurrency}")
    if args.requests is None:
        print(f"duration:    {args.duration:.1f}s")
    else:
        print(f"requests:    {args.requests}")
    print()
    print(f"completed:   {summary['total']} requests in {summary['elapsed']:.2f}s")
    print(f"throughput:  {summary['throughput']:.1f} req/s")
    print(
        f"responses:   {summary['valid']} valid, "
        f"{summary['invalid']} unexpected, {summary['errors']} failed"
    )
    print(
        "latency(ms): "
        f"min {latency['min']:.1f}  "
        f"mean {latency['mean']:.1f}  "
        f"p50 {latency['p50']:.1f}  "
        f"p90 {latency['p90']:.1f}  "
        f"p95 {latency['p95']:.1f}  "
        f"p99 {latency['p99']:.1f}  "
        f"max {latency['max']:.1f}"
    )
    for sample in summary["error_samples"]:
        print(f"  error: {sample}")


def main(argv=None) -> int:
    """Entry point; returns the process exit code."""
    args = _build_parser().parse_args(argv)

    if not args.url:
        raise SystemExit(
            "no endpoint URL configured; set BLITZORTUNG_LIVE_URL or pass --url"
        )
    if args.concurrency < 1:
        raise SystemExit("--concurrency must be >= 1")
    if args.requests is not None and args.requests < 1:
        raise SystemExit("--requests must be >= 1")
    if args.requests is None and args.duration <= 0:
        raise SystemExit("--duration must be > 0 when --requests is not set")
    if args.cache_bust and args.method != CACHE_BUST_METHOD:
        raise SystemExit("--cache-bust only applies to %s" % CACHE_BUST_METHOD)

    url = args.url.rstrip("/") + "/"
    params = _resolve_params(args.method, args.params)
    request = _Request(args.method, params, args.user_agent, args.content_type)
    positions = _Counter() if args.cache_bust else None

    try:
        _warmup(request, url, args.timeout, args.warmup)
    except Exception as error:  # pylint: disable=broad-except
        raise SystemExit(
            "warm-up request to %s failed: %s: %s"
            % (url, type(error).__name__, error)
        ) from error

    samples, elapsed = _run(
        request, url, args.timeout, args.concurrency, args.duration, args.requests, positions
    )
    summary = _summarize(samples, elapsed)

    if args.as_json:
        print(json.dumps(summary, indent=2))
    else:
        _print_report(url, request, args, summary)

    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
