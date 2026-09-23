# Database Access Performance Plan

This document tracks the work to improve database access performance in the
blitzortung Python service and CLI tools.

Legend for status: `[ ]` = todo, `[~]` = in progress, `[x]` = done.

## Baseline (already implemented)

- [x] **Batch inserts** (`Strike.insert_many`) replace the per-row `INSERT`
  round trip in `bo-import` and `bo-update` using
  `psycopg2.extras.execute_values`. This turns N round trips into ~N/1000.

## Phase 0 - Measure

- [x] **0.1 Micro-benchmarks** for the DB layer in
  `tests/db/test_db_benchmark.py` (powered by `pytest-benchmark`).  Covers
  `insert_many` vs per-row `insert`, `select_strike_keys` vs `select`, and the
  grid/histogram aggregate queries.  Benchmarks are opt-in: they are marked
  with `pytest.mark.benchmark` and skipped by default via `addopts` in
  `pyproject.toml`.  Run them with `poetry run pytest -m benchmark <path>` and
  tune the row counts with `BLITZORTUNG_BENCHMARK_STRIKES` /
  `BLITZORTUNG_BENCHMARK_INSERT_COUNT` / `BLITZORTUNG_BENCHMARK_ROUNDS`.
- [x] **0.2 Query instrumentation**: report returned row counts as gauges so
  slow query shapes become visible alongside timings.
- [x] **0.3 Seed data generator**: a fixture that inserts a configurable number
  of synthetic strikes using `insert_many`, so performance tests run against a
  realistic table size instead of a handful of rows.

## Phase 1 - Low-risk wins

- [x] **1.1 Batch inserts** (see baseline above).
- [x] **1.2 Narrow projection for URL-updater dedup**: `get_existing_strike_keys`
  used to select whole strikes and build full `Strike` objects only to compute
  `(timestamp, x, y, lateral_error)`. It now uses
  `Strike.select_strike_keys` with a narrow projection.
- [~] **1.3 Dense grid query computes the transform once**: `GridQuery` /
  `GlobalGridQuery` called `ST_Transform(geog::geometry, srid)` twice per row
  (once for X, once for Y).  A `LATERAL` sub-select was tried so the geometry
  would be transformed once per row, but it was **reverted: no measurable
  improvement**.  `EXPLAIN (ANALYZE, BUFFERS)` showed the planner inlines the
  `LATERAL` and the resulting plan still evaluates `ST_Transform` twice; the
  timings were equal within noise (see [Measurements](#measurements)).
- [x] **1.4 Conditional connection cancel/reset**: `Base.__init__` only calls
  `conn.cancel()` when the borrowed connection is actually executing a query.
- [x] **1.5 Configurable pool sizes**: `db/min_connections` and
  `db/max_connections` configure the psycopg2 `ThreadedConnectionPool`.
  `db/connection_count` already configures the minimum size of the txpostgres
  service pool (txpostgres has no maximum-size option).

## Phase 2 - Deeper changes

- [x] **2.1 Server-side/named cursor** for large `select` result sets so the
  client does not buffer every row.  `Base.execute_many` accepts a
  `server_side` flag; `select` and `select_strike_keys` stream through a named
  cursor with `itersize = Base.fetch_size` (5000 rows per round trip).  The
  trade-off is documented by `test_bench_select` vs `test_bench_select_client_side`:
  server-side cursors bound peak client memory but add latency.  Measurements
  show this is **not** negligible at `fetch_size = 5000`: ~10% slower at 20k
  rows and ~24% slower at 100k rows (see [Measurements](#measurements)).  The
  memory benefit has not been measured, so this is a trade-off, not a
  confirmed win.
- [~] **2.2 Prepared statements / statement caching**: investigated and
  deferred.  The webservice path runs through `txpostgres`, which exposes only
  `runQuery`/`runOperation` and has no prepared-statement API; psycopg2 (unlike
  psycopg3) also lacks a server-side `PREPARE` API.  The queries are simple and
  plan-parse cost is negligible versus I/O, while plan caching across pooled,
  reconnecting connections adds real risk, so no change was made.
- [~] **2.3 Index & maintenance review**: investigated but **not deployed**.  The
  production `strikes` table only has `strikes_pkey`, `strikes_timestamp`,
  `strikes_region_timestamp` and `strikes_timestamp_geog`.  Candidate additions
  (a BRIN index on `timestamp`, extra composite indexes and autovacuum tuning)
  are recorded but commented out in `docs/schema/proposed-indexes.sql`.  They
  are unproven (range scans are already served by the btree `strikes_timestamp`
  index) and untested against a production plan, so they must be validated with
  `EXPLAIN (ANALYZE, BUFFERS)` before being promoted to the canonical
  `docs/schema/strikes.sql`.  The canonical schema now mirrors production and
  the test suite fails if it drifts (`PRODUCTION_INDEXES` in
  `tests/db/test_db.py`).
- [x] **2.4 Grid query region handling**: local grid queries previously omitted
  the region filter, but the bounding boxes of adjacent regions overlap (e.g.
  Europe and Africa), so a strike on a border was counted in both regional
  grids.  `Strike.grid_query` now applies `region = %(region)s` when a region is
  given, and `StrikeGridQuery.create` passes `grid_parameters.region`.  Global
  and local (region-less) grids are unchanged.

## Phase 3 - Structural

- [ ] **3.1 Push dedup into the database** via a unique constraint and
  `INSERT ... ON CONFLICT DO NOTHING` (schema change + backfill required).

## Measurements

All numbers below were collected with `pytest-benchmark` via
`tests/db/test_db_benchmark.py`, against the production table/index set (the
`db_strikes` fixture applies `docs/schema/strikes.sql`).  Host timings, so the
absolute numbers are not meaningful on their own; they are only comparable
within one run.

Batch insert, 2000 rows (`BLITZORTUNG_BENCHMARK_STRIKES=20000`):

| Benchmark | Mean |
| --- | ---: |
| `insert_many` (batched) | ~55 ms |
| per-row `insert` | ~616 ms |

Narrow projection, 20k rows: `select_strike_keys` ~102 ms vs full
`select_client_side` ~130 ms (large part of it is avoiding full `Strike`
object construction).

Server-side cursor trade-off (full `select`, same result set, only the cursor
type differs):

| Rows | server-side | client-side | delta |
| ---: | ---: | ---: | ---: |
| 20k | ~143 ms | ~130 ms | +10% |
| 100k | ~730 ms | ~588 ms | +24% |

Grid query before/after the now-reverted `LATERAL` change, 50k rows, 25 rounds:

| Query | Mean | Median |
| --- | ---: | ---: |
| grid, pre-change (double `ST_Transform`) | ~36.3 ms | ~36.2 ms |
| grid, `LATERAL` (reverted) | ~37.2 ms | ~36.7 ms |

`EXPLAIN (ANALYZE, BUFFERS)` confirmed the two plans are the same shape and
both still evaluate `ST_Transform` twice per row, so the `LATERAL` wrapper
added overhead without reducing the transform work.  The histogram query was
never changed by this work and measured flat (~13.7 ms before vs ~14.3 ms
after at 50k rows, within noise).

## How to measure / compare

Benchmarks are skipped by default.  Run them explicitly:

```bash
# all benchmarks
poetry run pytest -m benchmark tests

# DB benchmarks with a realistic table size
BLITZORTUNG_BENCHMARK_STRIKES=20000 BLITZORTUNG_BENCHMARK_ROUNDS=10 \
  poetry run pytest -m benchmark tests/db/test_db_benchmark.py -q
```

The DB benchmark file compares the optimised and unoptimised paths directly
(`insert_many` vs per-row `insert`, `select_strike_keys` vs `select`).  The
`db_strikes` fixture applies `docs/schema/strikes.sql` verbatim, so benchmarks
run against the production table and index set; keep that file in sync with the
live database.  To track a change over time, save a baseline and diff against
it:

```bash
poetry run pytest -m benchmark tests/db/test_db_benchmark.py --benchmark-save=before
# ... make the change ...
poetry run pytest -m benchmark tests/db/test_db_benchmark.py \
  --benchmark-compare=before --benchmark-compare-fail=mean:10%
```

Runs are stored in `./.benchmarks/` (git-ignored).
