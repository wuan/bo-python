# -*- coding: utf8 -*-

"""

   Copyright 2014-2016 Andreas Würl

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

import pytest
import shapely
from assertpy import assert_that

import blitzortung
import blitzortung.db.query_builder
from blitzortung.db.query import TimeInterval
from blitzortung.geom import Grid


@pytest.fixture
def start_time(end_time, interval_duration):
    return end_time - interval_duration


@pytest.fixture
def interval_duration(end_time):
    return datetime.timedelta(minutes=10)


@pytest.fixture
def end_time():
    return datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)


@pytest.fixture
def srid():
    return 1234


class TestStrike:

    @pytest.fixture
    def query_builder(self):
        return blitzortung.db.query_builder.Strike()

    def test_select_query(self, query_builder, start_time, end_time, srid):
        query = query_builder.select_query("<table_name>", srid,
                                                time_interval=TimeInterval(start_time, end_time))

        assert_that(str(query)).is_equal_to(
            "SELECT id, \"timestamp\", nanoseconds, ST_X(ST_Transform(geog::geometry, %(srid)s))"
            " AS x, ST_Y(ST_Transform(geog::geometry, %(srid)s)) AS y, altitude, amplitude, error2d, stationcount "
            "FROM <table_name> WHERE \"timestamp\" >= %(start_time)s AND \"timestamp\" < %(end_time)s")
        parameters = query.get_parameters()
        assert_that(parameters.keys()).contains('srid', 'start_time', 'end_time')
        assert_that(parameters['start_time']).is_equal_to(start_time)
        assert_that(parameters['end_time']).is_equal_to(end_time)
        assert_that(parameters['srid']).is_equal_to(srid)

    def test_grid_query(self, query_builder, start_time, end_time, srid):
        grid = Grid(11.0, 12.0, 51.0, 52.0, 0.1, 0.2, srid)
        query = query_builder.grid_query("<table_name>", grid, count_threshold=0,
                                              time_interval=TimeInterval(start_time, end_time))

        assert_that(str(query)).is_equal_to(
            "SELECT TRUNC((ST_X(ST_Transform(geog::geometry, %(srid)s)) - %(xmin)s) / %(xdiv)s)::integer AS rx, "
            "TRUNC((ST_Y(ST_Transform(geog::geometry, %(srid)s)) - %(ymin)s) / %(ydiv)s)::integer AS ry, "
            "count(*) AS strike_count, max(\"timestamp\") as \"timestamp\" FROM <table_name> "
            "WHERE ST_GeomFromWKB(%(envelope)s, %(envelope_srid)s) && geog AND "
            "\"timestamp\" >= %(start_time)s AND \"timestamp\" < %(end_time)s GROUP BY rx, ry")
        parameters = query.get_parameters()
        assert_that(parameters.keys()).contains('xmin', 'ymin', 'xdiv', 'ydiv', 'envelope', 'envelope_srid', 'srid',
                                                'start_time', 'end_time')
        assert_that(parameters.keys()).does_not_contain('count_threshold')
        assert_that(parameters['xmin']).is_equal_to(11.0)
        assert_that(parameters['ymin']).is_equal_to(51.0)
        assert_that(parameters['xdiv']).is_equal_to(0.1)
        assert_that(parameters['ydiv']).is_equal_to(0.2)
        assert_that(parameters['envelope']).is_not_none()
        assert_that(parameters['envelope_srid']).is_equal_to(srid)
        assert_that(parameters['start_time']).is_equal_to(start_time)
        assert_that(parameters['end_time']).is_equal_to(end_time)
        assert_that(parameters['srid']).is_equal_to(srid)

    def test_grid_query_spanning_equator(self, query_builder, start_time, end_time, srid):
        grid = Grid(11.0, 18.0, -5.0, 5.0, 0.25, 0.5, srid)
        query = query_builder.grid_query("<table_name>", grid, count_threshold=0,
                                              time_interval=TimeInterval(start_time, end_time))

    def test_grid_query_with_count_threshold(self, query_builder, start_time, end_time, srid):
        grid = Grid(11.0, 12.0, 51.0, 52.0, 0.1, 0.2, srid)
        query = query_builder.grid_query("<table_name>", grid, count_threshold=5,
                                              time_interval=TimeInterval(start_time, end_time))

        parameters = query.get_parameters()
        assert_that(parameters.keys()).contains('count_threshold', 'xmin', 'ymin', 'xdiv', 'ydiv', 'envelope',
                                                'envelope_srid',
                                                'srid', 'start_time', 'end_time')
        assert_that(parameters['count_threshold']).is_equal_to(5)

        assert_that(str(query)).is_equal_to(
            "SELECT TRUNC((ST_X(ST_Transform(geog::geometry, %(srid)s)) - %(xmin)s) / %(xdiv)s)::integer AS rx, "
            "TRUNC((ST_Y(ST_Transform(geog::geometry, %(srid)s)) - %(ymin)s) / %(ydiv)s)::integer AS ry, "
            "count(*) AS strike_count, max(\"timestamp\") as \"timestamp\" FROM <table_name> "
            "WHERE ST_GeomFromWKB(%(envelope)s, %(envelope_srid)s) && geog AND "
            "\"timestamp\" >= %(start_time)s AND \"timestamp\" < %(end_time)s "
            "GROUP BY rx, ry HAVING count(*) > %(count_threshold)s")


class TestStrikeCluster:

    @pytest.fixture
    def query_builder(self):
        return blitzortung.db.query_builder.StrikeCluster()

    def test_select_query(self, query_builder, start_time, end_time, interval_duration, srid):
        query = query_builder.select_query("<table_name>", srid, end_time, interval_duration, 6,
                                                interval_duration)

        assert_that(str(query)).is_equal_to(
            "SELECT id, \"timestamp\", ST_Transform(geog::geometry, %(srid)s) as geom, strike_count FROM <table_name> WHERE \"timestamp\" in %(timestamps)s AND interval_seconds=%(interval_seconds)s")
        parameters = query.get_parameters()
        assert_that(parameters.keys()).contains('timestamps', 'srid', 'interval_seconds')
        assert_that(tuple(parameters['timestamps'])).is_equal_to(
            tuple(str(end_time - interval_duration * i) for i in range(0, 6)))
        assert_that(parameters['srid']).is_equal_to(srid)
        assert_that(parameters['interval_seconds']).is_equal_to(interval_duration.total_seconds())
