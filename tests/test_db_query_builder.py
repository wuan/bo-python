# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import unittest
import datetime
from hamcrest import assert_that, is_, equal_to, contains, contains_inanyorder
from nose.tools import raises
import pytz

import blitzortung
import blitzortung.db.query_builder


class StrikeClusterTest(unittest.TestCase):
    def setUp(self):
        self.query_builder = blitzortung.db.query_builder.StrikeCluster()
        self.end_time = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC, second=0, microsecond=0)
        self.interval_duration = datetime.timedelta(minutes=10)
        self.srid = "<srid>"

    def test_select_query(self):
        query = self.query_builder.select_query("<table_name>", self.srid, self.end_time, self.interval_duration, 6,
                                                self.interval_duration)

        assert_that(str(query), is_("SELECT id, \"timestamp\", stroke_count, ST_Transform(geog::geometry, %(srid)s) FROM <table_name> WHERE \"timestamp\" in (%(timestamps)s) AND interval_seconds=%(interval_seconds)s"))
        parameters = query.get_parameters()
        assert_that(parameters.keys(), contains_inanyorder('timestamps', 'srid', 'interval_seconds'))
        assert_that(list(parameters['timestamps']),
                    is_([self.end_time - self.interval_duration * i for i in range(0, 6)]))
        assert_that(parameters['srid'], is_(self.srid))
        assert_that(parameters['interval_seconds'], is_(self.interval_duration.total_seconds()))
