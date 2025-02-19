# -*- coding: utf8 -*-

"""

Copyright 2014-2022 Andreas Würl

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

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

try:
    import psycopg2
except ImportError:
    from blitzortung.db import create_psycopg2_dummy

    psycopg2 = create_psycopg2_dummy()

from assertpy import assert_that
from mock import Mock, call

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


class BaseTest(object):
    def setup_method(self):
        self.connection_pool = Mock()
        self.connection = self.connection_pool.getconn()
        self.cursor = self.connection.cursor()

        psycopg2.extensions = Mock()

        self.cursor.__enter__ = Mock(return_value=self.cursor)
        self.cursor.__exit__ = Mock(return_value=False)

        self.base = BaseForTest(self.connection_pool)

    def test_initialize(self):
        expected_calls = [
            call.getconn(),
            call.getconn().cursor(),
            call.getconn(),
            call.getconn().cancel(),
            call.getconn().reset(),
            call.getconn().set_client_encoding("UTF8"),
            call.getconn().cursor(),
            call.getconn().cursor().execute_many("SET TIME ZONE 'UTC'"),
            call.getconn().cursor(cursor_factory=psycopg2.extras.DictCursor),
            call.getconn().cursor().close(),
        ]
        self.connection_pool.has_calls(expected_calls)

    def test_is_connected(self):
        self.connection.closed = True
        assert_that(self.base.is_connected()).is_equal_to(False)

        self.connection.closed = False
        assert_that(self.base.is_connected()).is_true()

        self.base.conn = None
        assert_that(self.base.is_connected()).is_false()

    def test_full_table_name(self):
        assert_that(self.base.full_table_name).is_equal_to("")
        assert_that(self.base.schema_name).is_equal_to("")

        self.base.table_name = "foo"

        assert_that(self.base.full_table_name).is_equal_to("foo")

        self.base.schema_name = "bar"

        assert_that(self.base.full_table_name).is_equal_to('"bar"."foo"')
        assert_that(self.base.schema_name).is_equal_to("bar")

    def test_get_timezone(self):
        assert_that(self.base.get_timezone()).is_equal_to(datetime.timezone.utc)

    def test_fix_timezone(self):
        assert_that(self.base.fix_timezone(None)).is_none()

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("CET"))
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0, tzinfo=ZoneInfo("UTC"))

        assert_that(self.base.fix_timezone(time)).is_equal_to(utc_time)

    def test_from_bare_utc_to_timezone(self):
        self.base.set_timezone(ZoneInfo("CET"))

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("CET"))
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0)

        assert_that(self.base.from_bare_utc_to_timezone(utc_time)).is_equal_to(time)

    def test_from_timezone_to_bare_utc(self):
        self.base.set_timezone(ZoneInfo("CET"))

        time = datetime.datetime(2013, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("CET"))
        utc_time = datetime.datetime(2013, 1, 1, 11, 0, 0)

        assert_that(self.base.from_timezone_to_bare_utc(time)).is_equal_to(utc_time)

    def test_commit(self):
        self.connection.commit.assert_not_called()

        self.base.commit()

        self.connection.commit.assert_called_once_with()

    def test_rollback(self):
        self.connection.rollback.assert_not_called()

        self.base.rollback()

        self.connection.rollback.assert_called_once_with()
