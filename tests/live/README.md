# Live endpoint test suite

This suite verifies a **running** Blitzortung JSON-RPC webservice. It is not
part of the regular unit test run: when no endpoint is configured the network
tests are skipped, while the offline tests still run.

The suite is split into three modules:

| Module | Purpose |
| --- | --- |
| `test_endpoint_limits.py` | Access limits (user agent, content type, global baseline). |
| `test_endpoint_format.py` | Response format / data consistency, using valid requests. |
| `test_endpoint_compression.py` | gzip negotiation and legacy-client compatibility. |
| `test_client.py` | Offline regression tests for JSON-RPC response normalization. |
| `endpoints.py` | Shared endpoint definitions, constants and helpers. |

## Covered limits

| Limit | Expectation |
| --- | --- |
| User agent | Every data endpoint must return no data unless `User-Agent` matches `bo-android-<int number>` (version `> 0`). |
| Content type | Every data endpoint must return no data unless the request `Content-Type` is exactly `text/json`. |
| Global baseline | The global endpoint (`get_global_strikes_grid`) must return no data for a grid baseline below `25000`. |

A blocked request is answered with an empty JSON object (`{}`). A successful
grid request always contains the raster keys (`r`, `xd`, `yd`, `x0`, `y1`,
`xc`, `yc`, `t`, `dt`, `h`) even when no strikes fall inside the requested
interval. The tests use this response shape to distinguish "blocked" from
"valid but currently empty".

## Response format checks

The format suite (`test_endpoint_format.py`) sends valid requests and verifies
that each successful response is well formed. It does not test the access
limits. For every grid endpoint it checks that:

* all raster payload keys are present;
* `r` is a list of 4-tuples of integers;
* region-bound endpoints report coordinates inside the declared `xc`/`yc`
  bin counts (the global endpoint uses relative offsets and is excluded);
* cell dimensions `xd`/`yd` are positive numbers and `xc`/`yc` positive
  integers;
* `t` matches `%Y%m%dT%H:%M:%S` and is recent, `dt` equals the requested
  duration, and `h` is a non-negative integer list of the expected length;
* the `check` endpoint returns an increasing integer `count`.

## Payload compression

The compression suite sends raw requests (bypassing the transparent
decompression that `requests` performs) and checks the `Content-Encoding`
header. The service compresses a response with gzip only when the request
advertises `Accept-Encoding: gzip`, the uncompressed body is at least `1000`
bytes, and the Android client version is greater than
`MAX_COMPATIBLE_ANDROID_VERSION` (`177`). Older clients have the
`Accept-Encoding` header stripped before rendering and must always receive
plain, parseable JSON. The suite verifies all of these, including the
version boundary (`177` vs `178`).

## Endpoints exercised

* `get_strikes` (documented as always blocked)
* `get_strikes_grid`
* `get_strikes_raster`
* `get_strokes_raster`
* `get_local_strikes_grid`
* `get_global_strikes_grid` (global baseline limit)

## Running

Provide the target URL either through the environment:

```bash
BLITZORTUNG_LIVE_URL=http://127.0.0.1:8080/ poetry run pytest tests/live -v
```

or as a command line option:

```bash
poetry run pytest tests/live --live-url=http://127.0.0.1:8080/ -v
```

Optional settings:

* `--live-timeout=<seconds>` — per-request timeout (default `15`).

Only the `live` marker is used, so the suite can also be selected explicitly:

```bash
poetry run pytest -m live --live-url=http://127.0.0.1:8080/
```

## Protocol dialects

Different deployments answer in different JSON-RPC flavours depending on the
`txjsonrpc` version:

* pre-v1 (no `id`): a bare array, e.g. `[{"count": 4}]`
* v1: `{"result": ..., "error": null, "id": ...}`
* v2: `{"jsonrpc": "2.0", "result": ..., "id": ...}`

`client.normalize_jsonrpc_response` unwraps all of them to the same
`{"result": ...}` shape, so the limit assertions work against the public
service as well. The offline tests in `test_client.py` cover this and run even
when no endpoint URL is configured.

## Notes

* The global baseline threshold of `25000` and the accepted user agent /
  content type are defined as constants at the top of
  `test_endpoint_limits.py` so they can be kept in sync with the service.
* The suite only observes public HTTP behaviour; it never touches the database
  or internal service objects.
