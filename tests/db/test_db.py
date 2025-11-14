import datetime
import os
import random
from typing import Optional
from zoneinfo import ZoneInfo

import psycopg2
import pytest
from assertpy import assert_that
from psycopg2.pool import ThreadedConnectionPool
from testcontainers.postgres import PostgresContainer

import blitzortung
from blitzortung.service.general import create_time_interval

image = "postgis/postgis:18-3.6"
postgres = PostgresContainer(image)


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)
    os.environ["DB_CONN"] = postgres.get_connection_url()
    os.environ["DB_HOST"] = postgres.get_container_host_ip()
    os.environ["DB_PORT"] = str(postgres.get_exposed_port(5432))
    os.environ["DB_USERNAME"] = postgres.username
    os.environ["DB_PASSWORD"] = postgres.password
    os.environ["DB_NAME"] = postgres.dbname


@pytest.fixture
def connection_string() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    username = os.getenv("DB_USERNAME", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    database = os.getenv("DB_NAME", "postgres")
    return f"host={host} dbname={database} user={username} password={password} port={port}"


@pytest.fixture
def conn(connection_string):
    with psycopg2.connect(connection_string) as connection:
        yield connection


@pytest.fixture
def connection_pool(connection_string):
    connection_pool = psycopg2.pool.ThreadedConnectionPool(4, 50, connection_string)
    yield connection_pool
    connection_pool.closeall()


class BaseForTest(blitzortung.db.table.Base):
    def __init__(self, db_connection_pool):
        super(BaseForTest, self).__init__(db_connection_pool)

    def create_object_instance(self, result):
        return result

    def insert(self, *args):
        return args

    def select(self, *args):
        return args


class TestBase:

    @pytest.fixture
    def base(self, connection_pool):
        return BaseForTest(connection_pool)

    def test_is_connected(self, base):
        assert base.is_connected()

        base.conn.close()

        assert not base.is_connected()

    def test_full_table_name(self, base):
        assert_that(base.full_table_name).is_equal_to("")
        assert_that(base.schema_name).is_equal_to("")

        base.table_name = "foo"

        assert_that(base.full_table_name).is_equal_to("foo")

        base.schema_name = "bar"

        assert_that(base.full_table_name).is_equal_to('"bar"."foo"')
        assert_that(base.schema_name).is_equal_to("bar")

    def test_get_timezone(self, base):
        assert_that(base.get_timezone()).is_equal_to(datetime.timezone.utc)

    @pytest.fixture
    def cet_timezone(self):
        return ZoneInfo("Europe/Berlin")

    def test_fix_timezone(self, base, cet_timezone):
        assert_that(base.fix_timezone(None)).is_none()

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=cet_timezone)
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0, tzinfo=ZoneInfo("UTC"))

        assert_that(base.fix_timezone(time)).is_equal_to(utc_time)

    def test_from_bare_utc_to_timezone(self, base, cet_timezone):
        base.set_timezone(cet_timezone)

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=cet_timezone)
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0)

        assert_that(base.from_bare_utc_to_timezone(utc_time)).is_equal_to(time)

    def test_from_timezone_to_bare_utc(self, base, cet_timezone):
        base.set_timezone(cet_timezone)

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=cet_timezone)
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0)

        assert_that(base.from_timezone_to_bare_utc(time)).is_equal_to(utc_time)


def test_db_version(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT version()""")
        foo = cur.fetchone()
        print(foo)


def test_db_version_pool(connection_pool):
    conn = connection_pool.getconn()
    with conn.cursor() as cur:
        cur.execute("""SELECT version()""")
        foo = cur.fetchone()
        print(foo)
    connection_pool.putconn(conn)


@pytest.fixture
def strikes(connection_pool):
    conn = connection_pool.getconn()

    with conn.cursor() as cur:
        cur.execute("""
                    CREATE TABLE strikes
                    (
                        id          bigserial,
                        "timestamp" timestamptz,
                        nanoseconds SMALLINT,
                        geog        GEOGRAPHY(Point),
                        PRIMARY KEY (id)
                    );
                    ALTER TABLE strikes
                        ADD COLUMN altitude SMALLINT;
                    ALTER TABLE strikes
                        ADD COLUMN region SMALLINT;
                    ALTER TABLE strikes
                        ADD COLUMN amplitude REAL;
                    ALTER TABLE strikes
                        ADD COLUMN error2d SMALLINT;
                    ALTER TABLE strikes
                        ADD COLUMN stationcount SMALLINT;
                    CREATE INDEX strikes_geog ON strikes USING gist(geog);
                    CREATE INDEX strikes_timestamp ON strikes USING btree("timestamp");
                    """)
    conn.commit()

    query_builder = blitzortung.db.query_builder.Strike()
    strike_builder = blitzortung.builder.strike.Strike()
    mapper = blitzortung.db.mapper.Strike(strike_builder)

    strike = blitzortung.db.table.Strike(connection_pool, query_builder, mapper)
    yield strike
    strike.close()

    with conn.cursor() as cur:
        cur.execute("""DROP TABLE strikes""")
    conn.commit()

    connection_pool.putconn(conn)


@pytest.fixture
def strike_factory(now):
    def factory(x: float, y: float, offset: Optional[datetime.timedelta] = None) -> blitzortung.data.Strike:
        strike_builder = blitzortung.builder.strike.Strike()
        offset = offset if offset is not None else datetime.timedelta()

        strike_builder.set_timestamp(datetime.datetime.now(datetime.UTC) - offset)
        strike_builder.set_x(x)
        strike_builder.set_y(y)
        strike_builder.set_lateral_error(0.5)
        return strike_builder.build()

    return factory


def test_empty_query(strikes, strike_factory, now):
    result = strikes.select()
    print("result", list(result))

    assert len(list(result)) == 0


def test_insert_with_rollback(strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    strikes.insert(strike)
    strikes.rollback()

    result = strikes.select(time_interval=time_interval)

    assert len(list(result)) == 0


def test_insert_without_commit(strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    strikes.insert(strike)

    result = strikes.select(time_interval=time_interval)

    assert len(list(result)) == 1


def test_insert_and_select_strike(strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    strikes.insert(strike)
    strikes.commit()

    result = strikes.select(time_interval=time_interval)

    assert len(list(result)) == 1


def test_get_latest_time(strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    strikes.insert(strike)
    strikes.commit()

    result = strikes.get_latest_time()

    assert result == strike.timestamp


def test_get_latest_time_with_region(strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    strikes.insert(strike, 5)
    strikes.commit()

    result = strikes.get_latest_time()

    assert result == strike.timestamp


def test_get_latest_time_with_region_match(strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    strikes.insert(strike, 5)
    strikes.commit()

    result = strikes.get_latest_time(5)

    assert result == strike.timestamp


def test_get_latest_time_with_region_mismatch(strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    strikes.insert(strike, 5)
    strikes.commit()

    result = strikes.get_latest_time(4)

    assert result is None

def test_bench_insert(strike_factory, strikes, benchmark):
    strike = strike_factory(11, 49)
    def insert():
        strikes.insert(strike, 1)

    benchmark.pedantic(insert, args=(), rounds=10, iterations=20)

def test_bench_select(strike_factory, strikes, benchmark):
    for i in range(100):
        strike = strike_factory(11 + random.randrange(-100, 100, 1) / 100, 49 + random.randrange(-100, 100, 1) / 100)
        strikes.insert(strike, 1)

    def select():
        result = strikes.select()
        assert len(list(result)) == 100

    benchmark.pedantic(select, args=(), rounds=10, iterations=100)

def test_bench_select_grid(strike_factory, grid_factory, strikes, time_interval, benchmark):
    for i in range(100):
        strike = strike_factory(11 + random.randrange(-100, 100, 1) / 100, 49 + random.randrange(-100, 100, 1) / 100)
        strikes.insert(strike, 1)

    grid = grid_factory.get_for(5000)

    def select():
        result = strikes.select_grid(grid, 0, time_interval=time_interval)
        assert len(list(result)) <= 100

    benchmark.pedantic(select, args=(), rounds=10, iterations=100)

@pytest.mark.parametrize("raster_size,expected", [
    (100000, (1, 1, 1, 0)),
    (50000, (2, 1, 1, 0)),
    (25000, (4, 2, 1, 0)),
    (10000, (11, 6, 1, 0)),
    (5000, (23, 11, 1, 0))])
def test_grid_query(strikes, strike_factory, grid_factory, time_interval, utm_eu, raster_size, expected):
    strikes.insert(strike_factory(11.5, 49.5))
    strikes.commit()

    grid = grid_factory.get_for(raster_size)

    result = strikes.select_grid(grid, 0, time_interval=time_interval)

    assert result == (expected,)

def test_issues_with_missing_data_on_grid_query(strikes, strike_factory, local_grid_factory, time_interval, utm_eu):
    grid_size = 100000
    for i in range(10):
        strikes.insert(strike_factory(-90 + i,15))
    strikes.commit()

    # get_local_strikes_grid(-6, 0, 60, 10000, 0, >= 0, 15)
    local_grid1 = local_grid_factory(-6,0,15)
    grid1 = local_grid1.get_for(grid_size)
    result1 = strikes.select_grid(grid1, 0, time_interval=time_interval)
    assert len(result1) == 10

    # get_local_strikes_grid(-5, 0, 60, 10000, 0, >=0, 15)
    local_grid2 = local_grid_factory(-5,0,15)
    grid2 = local_grid2.get_for(grid_size)
    result2 = strikes.select_grid(grid2, 0, time_interval=time_interval)
    assert len(result2) == 10

def test_grid_query_with_count_threshold(strikes, strike_factory, grid_factory, time_interval, utm_eu):
    strikes.insert(strike_factory(11.5, 49.5))
    strikes.insert(strike_factory(12.5, 49.5))
    strikes.insert(strike_factory(12.5, 49.5))
    strikes.commit()

    grid = grid_factory.get_for(10000)

    result = strikes.select_grid(grid, 1, time_interval=time_interval)

    assert result == ((19, 6, 2, 0),)

@pytest.mark.parametrize("raster_size,expected", [
    (100000, (8, 139, 1, 0)),
    (50000, (17, 277, 1, 0)),
    (25000, (36, 553, 1, 0)),
    (10000, (90, 1383, 1, 0)),
    (5000, (181, 2766, 1, 0))])
def test_global_grid_query(strikes, strike_factory, global_grid_factory, time_interval, utm_eu, raster_size, expected):
    strikes.insert(strike_factory(11.5, 49.5))
    strikes.commit()

    grid = global_grid_factory.get_for(raster_size)

    result = strikes.select_global_grid(grid, 0, time_interval=time_interval)

    assert result == (expected,)


def test_empty_query2(strikes, strike_factory):
    result = strikes.select()

    assert len(list(result)) == 0


@pytest.mark.parametrize("minute_offset,expected", [
    (15, [3, 2, 1, 0, 0, 0]),
    (0, [6, 5, 4, 3, 2, 1]),
    (-15, [0, 8, 7, 6, 5, 4]),
    (-30, [0, 0, 0, 0, 8, 7])
])
def test_histogram_query(strikes, strike_factory, minute_offset, expected):
    for offset in range(8):
        timedelta = datetime.timedelta(minutes=offset * 5, seconds=1)
        print(timedelta)
        for _ in range(offset + 1):
            strikes.insert(strike_factory(11.5, 49.5, timedelta))
    strikes.commit()

    query_time_interval = create_time_interval(30, minute_offset)

    histogram = strikes.select_histogram(query_time_interval)

    assert histogram == expected
