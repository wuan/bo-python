# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import unittest
import datetime

import pytz
from mock import Mock, call
from hamcrest import assert_that, is_, equal_to, none
import psycopg2

import blitzortung
import blitzortung.db.table


class BaseForTest(blitzortung.db.table.Base):
    def __init__(self, db_connection_pool):
        super(BaseForTest, self).__init__(db_connection_pool)

    def create_object_instance(self, result):
        return result

    def insert(self, *args):
        return args

    def select(self, *args):
        return args


class BaseTest(unittest.TestCase):
    def setUp(self):
        self.connection_pool = Mock()
        self.connection = self.connection_pool.getconn()
        self.cursor = self.connection.cursor()

        psycopg2.extensions = Mock()

        self.cursor.__enter__ = Mock(return_value=self.cursor)
        self.cursor.__exit__ = Mock(return_value=False)

        self.base = BaseForTest(self.connection_pool)

    def test_initialize(self):
        expected_calls = [call.getconn(),
                          call.getconn().cursor(),
                          call.getconn(),
                          call.getconn().cancel(),
                          call.getconn().reset(),
                          call.getconn().set_client_encoding('UTF8'),
                          call.getconn().cursor(),
                          call.getconn().cursor().execute_many("SET TIME ZONE 'UTC'"),
                          call.getconn().cursor(cursor_factory=psycopg2.extras.DictCursor),
                          call.getconn().cursor().close()]
        self.connection_pool.has_calls(expected_calls)

    def test_is_connected(self):
        self.connection.closed = True
        assert_that(self.base.is_connected(), is_(False))

        self.connection.closed = False
        assert_that(self.base.is_connected())

        self.base.conn = None
        assert_that(self.base.is_connected(), is_(False))

    def test_table_name(self):
        assert_that(self.base.get_table_name(), is_(""))

        self.base.set_table_name("foo")

        assert_that(self.base.get_table_name(), is_("foo"))

    def test_full_table_name(self):
        assert_that(self.base.get_full_table_name(), is_(""))
        assert_that(self.base.get_schema_name(), is_(""))

        self.base.set_table_name("foo")

        assert_that(self.base.get_full_table_name(), is_("foo"))

        self.base.set_schema_name("bar")

        assert_that(self.base.get_full_table_name(), is_('"bar"."foo"'))
        assert_that(self.base.get_schema_name(), is_("bar"))

    def test_srid(self):
        assert_that(self.base.get_srid(), is_(equal_to(4326)))

        self.base.set_srid(1234)

        assert_that(self.base.get_srid(), is_(equal_to(1234)))

    def test_get_timezone(self):
        assert_that(self.base.get_timezone(), is_(equal_to(pytz.UTC)))

    def test_fix_timezone(self):
        assert_that(self.base.fix_timezone(None), is_(none()))

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=pytz.timezone("CET"))
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0, tzinfo=pytz.timezone("UTC"))

        assert_that(self.base.fix_timezone(time), is_(utc_time))

    def test_from_bare_utc_to_timezone(self):
        self.base.set_timezone(pytz.timezone("CET"))

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=pytz.timezone("CET"))
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0)

        assert_that(self.base.from_bare_utc_to_timezone(utc_time), is_(equal_to(time)))

    def test_from_timezone_to_bare_utc(self):
        self.base.set_timezone(pytz.timezone("CET"))

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=pytz.timezone("CET"))
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0)

        assert_that(self.base.from_timezone_to_bare_utc(time), is_(equal_to(utc_time)))

    def test_commit(self):
        self.connection.commit.assert_not_called()

        self.base.commit()

        self.connection.commit.assert_called_once_with()

    def test_rollback(self):
        self.connection.rollback.assert_not_called()

        self.base.rollback()

        self.connection.rollback.assert_called_once_with()
