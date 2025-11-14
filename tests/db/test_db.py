import datetime
import os
from typing import Optional
from zoneinfo import ZoneInfo

import psycopg2
import pytest
from assertpy import assert_that
from psycopg2.pool import ThreadedConnectionPool
from testcontainers.postgres import PostgresContainer

import blitzortung
from blitzortung.service.general import create_time_interval






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

    def test_fix_timezone(self, base):
        assert_that(base.fix_timezone(None)).is_none()

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("CET"))
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0, tzinfo=ZoneInfo("UTC"))

        assert_that(base.fix_timezone(time)).is_equal_to(utc_time)

    def test_from_bare_utc_to_timezone(self, base):
        base.set_timezone(ZoneInfo("CET"))

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("CET"))
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0)

        assert_that(base.from_bare_utc_to_timezone(utc_time)).is_equal_to(time)

    def test_from_timezone_to_bare_utc(self, base):
        base.set_timezone(ZoneInfo("CET"))

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("CET"))
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0)

        assert_that(base.from_timezone_to_bare_utc(time)).is_equal_to(utc_time)


def test_db_version(connection_string):
    with psycopg2.connect(connection_string) as connection:
        with connection.cursor() as cur:
            cur.execute("""SELECT version()""")
            result = cur.fetchone()
            assert result is not None
            assert len(result) == 1
            assert result[0].startswith("PostgreSQL")


def test_db_version_pool(connection_pool):
    conn = connection_pool.getconn()
    with conn.cursor() as cur:
        cur.execute("""SELECT version()""")
        foo = cur.fetchone()
        print(foo)
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


def test_empty_query(db_strikes, strike_factory, now):
    result = db_strikes.select()
    print("result", list(result))

    assert len(list(result)) == 0


def test_insert_with_rollback(db_strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    db_strikes.insert(strike)
    db_strikes.rollback()

    result = db_strikes.select(time_interval=time_interval)

    assert len(list(result)) == 0


def test_insert_without_commit(db_strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    db_strikes.insert(strike)

    result = db_strikes.select(time_interval=time_interval)

    assert len(list(result)) == 1


def test_insert_and_select_strike(db_strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    db_strikes.insert(strike)
    db_strikes.commit()

    result = db_strikes.select(time_interval=time_interval)

    assert len(list(result)) == 1


def test_get_latest_time(db_strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    db_strikes.insert(strike)
    db_strikes.commit()

    result = db_strikes.get_latest_time()

    assert result == strike.timestamp


def test_get_latest_time_with_region(db_strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    db_strikes.insert(strike, 5)
    db_strikes.commit()

    result = db_strikes.get_latest_time()

    assert result == strike.timestamp


def test_get_latest_time_with_region_match(db_strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    db_strikes.insert(strike, 5)
    db_strikes.commit()

    result = db_strikes.get_latest_time(5)

    assert result == strike.timestamp


def test_get_latest_time_with_region_mismatch(db_strikes, strike_factory, time_interval):
    strike = strike_factory(11, 49)
    db_strikes.insert(strike, 5)
    db_strikes.commit()

    result = db_strikes.get_latest_time(4)

    assert result is None


@pytest.mark.parametrize("raster_size,expected", [
    (100000, (1, 1, 1, 0)),
    (50000, (2, 1, 1, 0)),
    (25000, (4, 2, 1, 0)),
    (10000, (11, 6, 1, 0)),
    (5000, (23, 11, 1, 0))])
def test_grid_query(db_strikes, strike_factory, grid_factory, time_interval, utm_eu, raster_size, expected):
    db_strikes.insert(strike_factory(11.5, 49.5))
    db_strikes.commit()

    grid = grid_factory.get_for(raster_size)

    result = db_strikes.select_grid(grid, 0, time_interval=time_interval)

    assert result == (expected,)

def test_issues_with_missing_data_on_grid_query(db_strikes, strike_factory, local_grid_factory, time_interval, utm_eu):
    grid_size = 100000
    for i in range(10):
        db_strikes.insert(strike_factory(-90 + i, 15))
    db_strikes.commit()

    # get_local_strikes_grid(-6, 0, 60, 10000, 0, >= 0, 15)
    local_grid1 = local_grid_factory(-6,0,15)
    grid1 = local_grid1.get_for(grid_size)
    result1 = db_strikes.select_grid(grid1, 0, time_interval=time_interval)
    assert len(result1) == 10

    # get_local_strikes_grid(-5, 0, 60, 10000, 0, >=0, 15)
    local_grid2 = local_grid_factory(-5,0,15)
    grid2 = local_grid2.get_for(grid_size)
    result2 = db_strikes.select_grid(grid2, 0, time_interval=time_interval)
    assert len(result2) == 10

def test_grid_query_with_count_threshold(db_strikes, strike_factory, grid_factory, time_interval, utm_eu):
    db_strikes.insert(strike_factory(11.5, 49.5))
    db_strikes.insert(strike_factory(12.5, 49.5))
    db_strikes.insert(strike_factory(12.5, 49.5))
    db_strikes.commit()

    grid = grid_factory.get_for(10000)

    result = db_strikes.select_grid(grid, 1, time_interval=time_interval)

    assert result == ((19, 6, 2, 0),)

@pytest.mark.parametrize("raster_size,expected", [
    (100000, (8, 139, 1, 0)),
    (50000, (17, 277, 1, 0)),
    (25000, (36, 553, 1, 0)),
    (10000, (90, 1383, 1, 0)),
    (5000, (181, 2766, 1, 0))])
def test_global_grid_query(db_strikes, strike_factory, global_grid_factory, time_interval, utm_eu, raster_size, expected):
    db_strikes.insert(strike_factory(11.5, 49.5))
    db_strikes.commit()

    grid = global_grid_factory.get_for(raster_size)

    result = db_strikes.select_global_grid(grid, 0, time_interval=time_interval)

    assert result == (expected,)


def test_empty_query2(db_strikes, strike_factory):
    result = db_strikes.select()

    assert len(list(result)) == 0


@pytest.mark.parametrize("minute_offset,expected", [
    (15, [3, 2, 1, 0, 0, 0]),
    (0, [6, 5, 4, 3, 2, 1]),
    (-15, [0, 8, 7, 6, 5, 4]),
    (-30, [0, 0, 0, 0, 8, 7])
])
def test_histogram_query(db_strikes, strike_factory, minute_offset, expected):
    for offset in range(8):
        timedelta = datetime.timedelta(minutes=offset * 5, seconds=1)
        print(timedelta)
        for _ in range(offset + 1):
            db_strikes.insert(strike_factory(11.5, 49.5, timedelta))
    db_strikes.commit()

    query_time_interval = create_time_interval(30, minute_offset)

    histogram = db_strikes.select_histogram(query_time_interval)

    assert histogram == expected
