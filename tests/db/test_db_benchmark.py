# -*- coding: utf8 -*-

"""

   Copyright 2025 Andreas Würl

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

"""

import datetime
import os

import pytest

import blitzortung

# Benchmarks are only collected when explicitly selected, see pyproject.toml.
pytestmark = pytest.mark.benchmark

# Number of rows seeded per benchmark.  Kept small by default so the test
# suite stays fast; override with BLITZORTUNG_BENCHMARK_STRIKES for a
# meaningful measurement.
SEED_COUNT = int(os.environ.get("BLITZORTUNG_BENCHMARK_STRIKES", "2000"))
ROUNDS = int(os.environ.get("BLITZORTUNG_BENCHMARK_ROUNDS", "5"))
INSERT_COUNT = int(os.environ.get("BLITZORTUNG_BENCHMARK_INSERT_COUNT", "1000"))


def _make_strikes(count: int):
    base_time = datetime.datetime.now(datetime.UTC)
    strikes = []
    for index in range(count):
        builder = blitzortung.builder.strike.Strike()
        builder.set_timestamp(base_time + datetime.timedelta(microseconds=index))
        builder.set_x(10.0 + (index % 100) * 0.01)
        builder.set_y(45.0 + (index % 100) * 0.01)
        builder.set_lateral_error(index % 10)
        builder.set_station_count(index % 50)
        strikes.append(builder.build())
    return strikes


def test_bench_select_strike_keys(db_strikes, seed_strikes, benchmark):
    """Narrow de-duplication projection used by the URL updater."""
    time_interval = seed_strikes(SEED_COUNT)

    benchmark.pedantic(
        lambda: list(db_strikes.select_strike_keys(time_interval=time_interval)),
        rounds=ROUNDS,
        iterations=1,
    )


def test_bench_select(db_strikes, seed_strikes, benchmark):
    """Full strike select (builds complete Strike objects, server-side)."""
    time_interval = seed_strikes(SEED_COUNT)

    benchmark.pedantic(
        lambda: list(db_strikes.select(time_interval=time_interval)),
        rounds=ROUNDS,
        iterations=1,
    )


def test_bench_select_client_side(db_strikes, seed_strikes, benchmark):
    """Full strike select buffering every row on the client (baseline)."""
    time_interval = seed_strikes(SEED_COUNT)
    query = db_strikes.query_builder.select_query(
        db_strikes.full_table_name, db_strikes.srid, time_interval=time_interval)

    benchmark.pedantic(
        lambda: list(db_strikes.execute_many(
            str(query), query.get_parameters(), db_strikes.strike_mapper.create_object,
            server_side=False, timezone=db_strikes.tz)),
        rounds=ROUNDS,
        iterations=1,
    )


def test_bench_grid_query(db_strikes, seed_strikes, grid_factory, benchmark):
    """Dense grid aggregation query."""
    time_interval = seed_strikes(SEED_COUNT)
    grid = grid_factory.get_for(100000)

    benchmark.pedantic(
        lambda: db_strikes.select_grid(grid, 0, time_interval=time_interval),
        rounds=ROUNDS,
        iterations=1,
    )


def test_bench_histogram_query(db_strikes, seed_strikes, benchmark):
    """Histogram aggregation query."""
    time_interval = seed_strikes(SEED_COUNT)

    benchmark.pedantic(
        lambda: db_strikes.select_histogram(time_interval),
        rounds=ROUNDS,
        iterations=1,
    )


def test_bench_insert_one_by_one(db_strikes, benchmark):
    """Per-row INSERT round trips, as used before batching."""
    strikes = _make_strikes(INSERT_COUNT)

    def run():
        for strike in strikes:
            db_strikes.insert(strike)
        db_strikes.commit()

    benchmark.pedantic(run, rounds=ROUNDS, iterations=1)


def test_bench_insert_many(db_strikes, benchmark):
    """Batched INSERT using execute_values."""
    strikes = _make_strikes(INSERT_COUNT)

    def run():
        db_strikes.insert_many(strikes)
        db_strikes.commit()

    benchmark.pedantic(run, rounds=ROUNDS, iterations=1)
