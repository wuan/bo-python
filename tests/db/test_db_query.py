# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2015 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

from unittest import TestCase
import datetime
from hamcrest import assert_that, is_, equal_to
from nose.tools import raises
import shapely.wkb

import blitzortung
import blitzortung.db.query


class IdIntervalTest(TestCase):
    def test_nothing_set(self):
        id_interval = blitzortung.db.query.IdInterval()

        self.assertEqual(id_interval.start, None)
        self.assertEqual(id_interval.end, None)
        self.assertEqual(str(id_interval), "[ : ]")

    def test_start_set(self):
        id_interval = blitzortung.db.query.IdInterval(1234)

        self.assertEqual(id_interval.start, 1234)
        self.assertEqual(id_interval.end, None)
        self.assertEqual(str(id_interval), "[1234 : ]")

    def test_start_and_stop_set(self):
        id_interval = blitzortung.db.query.IdInterval(1234, 5678)

        self.assertEqual(id_interval.start, 1234)
        self.assertEqual(id_interval.end, 5678)
        self.assertEqual(str(id_interval), "[1234 : 5678]")

    @raises(ValueError)
    def test_exception_when_start_is_not_integer(self):
        blitzortung.db.query.IdInterval("asdf")

    @raises(ValueError)
    def test_exception_when_end_is_not_integer(self):
        blitzortung.db.query.IdInterval(1, "asdf")


class TimeIntervalTest(TestCase):
    def test_nothing_set(self):
        id_interval = blitzortung.db.query.TimeInterval()

        self.assertEqual(id_interval.start, None)
        self.assertEqual(id_interval.end, None)
        self.assertEqual(str(id_interval), "[ : ]")

    def test_start_set(self):
        id_interval = blitzortung.db.query.TimeInterval(datetime.datetime(2010, 11, 20, 11, 30, 15))

        self.assertEqual(id_interval.start, datetime.datetime(2010, 11, 20, 11, 30, 15))
        self.assertEqual(id_interval.end, None)
        self.assertEqual(str(id_interval), "[2010-11-20 11:30:15 : ]")

    def test_start_and_stop_set(self):
        id_interval = blitzortung.db.query.TimeInterval(datetime.datetime(2010, 11, 20, 11, 30, 15),
                                                        datetime.datetime(2010, 12, 5, 23, 15, 59))

        self.assertEqual(id_interval.start, datetime.datetime(2010, 11, 20, 11, 30, 15))
        self.assertEqual(id_interval.end, datetime.datetime(2010, 12, 5, 23, 15, 59))
        self.assertEqual(str(id_interval), "[2010-11-20 11:30:15 : 2010-12-05 23:15:59]")

    def test_get_duration(self):
        id_interval = blitzortung.db.query.TimeInterval(datetime.datetime(2010, 11, 20, 11, 30, 15),
                                                        datetime.datetime(2010, 11, 20, 11, 40, 15))

        self.assertEqual(id_interval.duration, datetime.timedelta(minutes=10))

    @raises(ValueError)
    def test_exception_when_start_is_not_integer(self):
        blitzortung.db.query.TimeInterval("asdf")

    @raises(ValueError)
    def test_exception_when_end_is_not_integer(self):
        blitzortung.db.query.TimeInterval(datetime.datetime.utcnow(), "asdf")


class QueryTest(TestCase):
    def setUp(self):
        self.query = blitzortung.db.query.Query()

    def test_initialization(self):
        self.assertEqual(str(self.query), "")

    def test_add_group_by(self):
        self.query.add_group_by('bar')
        self.assertEqual(str(self.query), "GROUP BY bar")
        self.query.add_group_by('baz')
        self.assertEqual(str(self.query), "GROUP BY bar, baz")

    def test_add_group_with_having_condition(self):
        self.query.add_group_by('bar')
        self.query.add_group_having("foo > 1")

        self.assertEqual(str(self.query), "GROUP BY bar HAVING foo > 1")

    def test_add_having_has_no_effect_without_group(self):
        self.query.add_group_having("foo > 1")

        self.assertEqual(str(self.query), "")

    def test_add_condition(self):
        self.query.add_condition("qux")
        self.assertEqual(str(self.query), "WHERE qux")

        self.query.add_condition("quux")
        self.assertEqual(str(self.query), "WHERE qux AND quux")

    def test_add_condition_with_parameters(self):
        self.query.add_condition("column LIKE %(name)s", name='<name>')
        assert_that(str(self.query), is_(equal_to("WHERE column LIKE %(name)s")))
        assert_that(self.query.get_parameters(), is_(equal_to({'name': '<name>'})))

        self.query.add_condition("other LIKE %(type)s", type='<type>')
        assert_that(str(self.query), is_(equal_to("WHERE column LIKE %(name)s AND other LIKE %(type)s")))
        assert_that(self.query.get_parameters(), is_(equal_to({'name': '<name>', 'type': '<type>'})))

    def test_add_order(self):
        self.query.set_order(blitzortung.db.query.Order("bar"))
        self.assertEqual(str(self.query), "ORDER BY bar")

    def test_add_order_with_multiple_elements(self):
        self.query.set_order(blitzortung.db.query.Order("bar"), blitzortung.db.query.Order("baz", True))
        self.assertEqual(str(self.query), "ORDER BY bar, baz DESC")

    def test_add_order_with_multiple_mixed_elements(self):
        self.query.set_order("bar", blitzortung.db.query.Order("baz", True))
        self.assertEqual(str(self.query), "ORDER BY bar, baz DESC")

    def test_set_limit(self):
        self.query.set_limit(10)
        self.assertEqual(str(self.query), "LIMIT 10")

    def test_parse_args_with_id_interval(self):
        self.query.set_default_conditions(id_interval=blitzortung.db.query.IdInterval(10, 15))

        assert_that(str(self.query), is_(equal_to("WHERE id >= %(start_id)s AND id < %(end_id)s")))
        assert_that(self.query.get_parameters(), is_(equal_to({'start_id': 10, 'end_id': 15})))

    def test_parse_args_with_time_interval(self):
        self.query.set_default_conditions(time_interval=blitzortung.db.query.TimeInterval(
            datetime.datetime(2013, 10, 9, 17, 20),
            datetime.datetime(2013, 10, 11, 6, 30)))

        assert_that(str(self.query), is_(equal_to(
            "WHERE \"timestamp\" >= %(start_time)s AND \"timestamp\" < %(end_time)s"
        )))
        assert_that(self.query.get_parameters(), is_(equal_to({
            'start_time': datetime.datetime(2013, 10, 9, 17, 20),
            'end_time': datetime.datetime(2013, 10, 11, 6, 30)})))

    def test_parse_args_with_order(self):
        self.query.set_default_conditions(order='test')

        assert_that(str(self.query), is_(equal_to(
            "ORDER BY test"
        )))

    def test_parse_args_with_limit(self):
        self.query.set_default_conditions(limit=10)

        assert_that(str(self.query), is_(equal_to(
            "LIMIT 10"
        )))


class SelectQueryTest(TestCase):
    def setUp(self):
        self.query = blitzortung.db.query.SelectQuery()
        self.query.set_table_name("foo")

    def test_initialization(self):
        self.assertEqual(str(self.query), "SELECT FROM foo")

    def test_set_table_name(self):
        self.assertEqual(str(self.query), "SELECT FROM foo")

    def test_set_columns(self):
        self.query.set_columns('bar', 'baz')
        self.assertEqual(str(self.query), "SELECT bar, baz FROM foo")

    def test_add_column(self):
        self.query.add_column('bar')
        self.assertEqual(str(self.query), "SELECT bar FROM foo")
        self.query.add_column('baz')
        self.assertEqual(str(self.query), "SELECT bar, baz FROM foo")

    def test_add_condition(self):
        self.query.add_condition("qux")
        self.assertEqual(str(self.query), "SELECT FROM foo WHERE qux")

        self.query.add_condition("quux")
        self.assertEqual(str(self.query), "SELECT FROM foo WHERE qux AND quux")

    def test_add_parameters(self):
        assert_that(self.query.get_parameters(), is_(equal_to({})))

        self.query.add_parameters(foo='bar', baz='qux')

        assert_that(self.query.get_parameters(), is_(equal_to({'foo': 'bar', 'baz': 'qux'})))


class GridQueryTest(TestCase):
    def test_with_raster(self):
        raster = blitzortung.geom.Grid(-10, 20, 15, 35, 1.5, 1)

        query = blitzortung.db.query.GridQuery(raster)
        query.set_table_name('strikes')

        assert_that(str(query), is_(equal_to(
            'SELECT '
            'TRUNC((ST_X(ST_Transform(geog::geometry, %(srid)s)) - %(xmin)s) / %(xdiv)s)::integer AS rx, '
            'TRUNC((ST_Y(ST_Transform(geog::geometry, %(srid)s)) - %(ymin)s) / %(ydiv)s)::integer AS ry, '
            'count(*) AS strike_count, '
            'max("timestamp") as "timestamp" '
            'FROM strikes '
            'WHERE ST_GeomFromWKB(%(envelope)s, %(envelope_srid)s) && geog '
            'GROUP BY rx, ry')))

        parameters = query.get_parameters()
        assert_that(parameters['xmin'], is_(equal_to(-10)))
        assert_that(parameters['xdiv'], is_(equal_to(1.5)))
        assert_that(parameters['ymin'], is_(equal_to(15)))
        assert_that(parameters['ydiv'], is_(equal_to(1)))
        assert_that(parameters['srid'], is_(equal_to(4326)))
        assert_that(parameters['envelope_srid'], is_(equal_to(4326)))
        envelope = shapely.wkb.loads(parameters['envelope'].adapted)
        assert_that(envelope.bounds, is_(equal_to((-10, 15, 20, 35))))

