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
- [x] **1.3 Dense grid query computes the transform once**: `GridQuery` /
  `GlobalGridQuery` previously called `ST_Transform(geog::geometry, srid)`
  twice per row (once for X, once for Y). The grid queries now use a
  `LATERAL` sub-select so the geometry is transformed once per row.
- [x] **1.4 Conditional connection cancel/reset**: `Base.__init__` only calls
  `conn.cancel()` when the borrowed connection is actually executing a query.
- [x] **1.5 Configurable pool sizes**: `db/min_connections` and
  `db/max_connections` configure the psycopg2 `ThreadedConnectionPool`.
  `db/connection_count` already configures the minimum size of the txpostgres
  service pool (txpostgres has no maximum-size option).

## Phase 2 - Deeper changes

- [ ] **2.1 Server-side/named cursor** for large `select` result sets so the
  client does not buffer every row.
- [ ] **2.2 Prepared statements / statement caching** for the webservice query
  path.
- [ ] **2.3 Index & maintenance review** (BRIN on `timestamp`, autovacuum /
  `ANALYZE`).
- [ ] **2.4 Grid query region handling**: verify whether the region filter is
  intentionally omitted from grid queries; add it if the product requires it.

## Phase 3 - Structural

- [ ] **3.1 Push dedup into the database** via a unique constraint and
  `INSERT ... ON CONFLICT DO NOTHING` (schema change + backfill required).

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
(`insert_many` vs per-row `insert`, `select_strike_keys` vs `select`).  To
track a change over time, save a baseline and diff against it:

```bash
poetry run pytest -m benchmark tests/db/test_db_benchmark.py --benchmark-save=before
# ... make the change ...
poetry run pytest -m benchmark tests/db/test_db_benchmark.py \
  --benchmark-compare=before --benchmark-compare-fail=mean:10%
```

Runs are stored in `./.benchmarks/` (git-ignored).
