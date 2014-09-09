# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import unittest
import datetime
from hamcrest import assert_that, is_, equal_to, contains, contains_inanyorder, not_none
from nose.tools import raises
import pytz

import blitzortung
from blitzortung.db.query import TimeInterval
import blitzortung.db.query_builder
from blitzortung.geom import Grid


class StrikeTest(unittest.TestCase):
    def setUp(self):
        self.query_builder = blitzortung.db.query_builder.Strike()
        self.end_time = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC, second=0, microsecond=0)
        self.start_time = self.end_time - datetime.timedelta(minutes=10)
        self.srid = 1234

    def test_select_query(self):
        query = self.query_builder.select_query("<table_name>", self.srid, TimeInterval(self.start_time, self.end_time))

        assert_that(str(query),
                    is_("SELECT id, \"timestamp\", nanoseconds, ST_X(ST_Transform(geog::geometry, %(srid)s))"
                        " AS x, ST_Y(ST_Transform(geog::geometry, %(srid)s)) AS y, altitude, amplitude, error2d, stationcount "
                        "FROM <table_name> WHERE \"timestamp\" >= %(start_time)s AND \"timestamp\" < %(end_time)s"))
        parameters = query.get_parameters()
        assert_that(parameters.keys(), contains_inanyorder('srid', 'start_time', 'end_time'))
        assert_that(parameters['start_time'], is_(self.start_time))
        assert_that(parameters['end_time'], is_(self.end_time))
        assert_that(parameters['srid'], is_(self.srid))

    def test_grid_query(self):
        grid = Grid(11.0, 12.0, 51.0, 52.0, 0.1, 0.2, self.srid)
        query = self.query_builder.grid_query("<table_name>", grid, TimeInterval(self.start_time, self.end_time))

        assert_that(str(query), is_(
            "SELECT TRUNC((ST_X(ST_Transform(geog::geometry, %(srid)s)) - %(xmin)s) / %(xdiv)s)::integer AS rx, "
            "TRUNC((ST_Y(ST_Transform(geog::geometry, %(srid)s)) - %(ymin)s) / %(ydiv)s)::integer AS ry, "
            "count(*) AS count, max(\"timestamp\") as \"timestamp\" FROM <table_name> "
            "WHERE ST_GeomFromWKB(%(envelope)s, %(envelope_srid)s) && geog AND "
            "\"timestamp\" >= %(start_time)s AND \"timestamp\" < %(end_time)s GROUP BY rx, ry"))
        parameters = query.get_parameters()
        assert_that(parameters.keys(),
                    contains_inanyorder('xmin', 'ymin', 'xdiv', 'ydiv', 'envelope', 'envelope_srid', 'srid',
                                        'start_time', 'end_time'))
        assert_that(parameters['xmin'], is_(11.0))
        assert_that(parameters['ymin'], is_(51.0))
        assert_that(parameters['xdiv'], is_(0.1))
        assert_that(parameters['ydiv'], is_(0.2))
        assert_that(parameters['envelope'], is_(not_none()))
        assert_that(parameters['envelope_srid'], is_(self.srid))
        assert_that(parameters['start_time'], is_(self.start_time))
        assert_that(parameters['end_time'], is_(self.end_time))
        assert_that(parameters['srid'], is_(self.srid))


class StrikeClusterTest(unittest.TestCase):
    def setUp(self):
        self.query_builder = blitzortung.db.query_builder.StrikeCluster()
        self.end_time = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC, second=0, microsecond=0)
        self.interval_duration = datetime.timedelta(minutes=10)
        self.srid = "<srid>"

    def test_select_query(self):
        query = self.query_builder.select_query("<table_name>", self.srid, self.end_time, self.interval_duration, 6,
                                                self.interval_duration)

        assert_that(str(query), is_(
            "SELECT id, \"timestamp\", ST_Transform(geog::geometry, %(srid)s) as geom, strike_count FROM <table_name> WHERE \"timestamp\" in (%(timestamps)s) AND interval_seconds=%(interval_seconds)s"))
        parameters = query.get_parameters()
        assert_that(parameters.keys(), contains_inanyorder('timestamps', 'srid', 'interval_seconds'))
        assert_that(parameters['timestamps'],
                    is_(','.join([str(self.end_time - self.interval_duration * i) for i in range(0, 6)])))
        assert_that(parameters['srid'], is_(self.srid))
        assert_that(parameters['interval_seconds'], is_(self.interval_duration.total_seconds()))
