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
import shapely.wkb
from assertpy import assert_that

import blitzortung
import blitzortung.db.query


class TestIdInterval:
    """Test suite for IdInterval class."""

    def test_nothing_set(self):
        """Test IdInterval with no arguments."""
        id_interval = blitzortung.db.query.IdInterval()

        assert id_interval.start is None
        assert id_interval.end is None
        assert str(id_interval) == "[ : ]"

    def test_start_set(self):
        """Test IdInterval with start only."""
        id_interval = blitzortung.db.query.IdInterval(1234)

        assert id_interval.start == 1234
        assert id_interval.end is None
        assert str(id_interval) == "[1234 : ]"

    def test_start_and_stop_set(self):
        """Test IdInterval with start and end."""
        id_interval = blitzortung.db.query.IdInterval(1234, 5678)

        assert id_interval.start == 1234
        assert id_interval.end == 5678
        assert str(id_interval) == "[1234 : 5678]"

    def test_exception_when_start_is_not_integer(self):
        """Test raising exception for non-integer start."""
        with pytest.raises(ValueError):
            blitzortung.db.query.IdInterval("asdf")

    def test_exception_when_end_is_not_integer(self):
        """Test raising exception for non-integer end."""
        with pytest.raises(ValueError):
            blitzortung.db.query.IdInterval(1, "asdf")


class TestTimeInterval:
    """Test suite for TimeInterval class."""

    def test_nothing_set(self):
        """Test TimeInterval with no arguments."""
        interval = blitzortung.db.query.TimeInterval()

        assert interval.start is None
        assert interval.end is None
        assert str(interval) == "[ : ]"

    def test_start_set(self):
        """Test TimeInterval with start only."""
        interval = blitzortung.db.query.TimeInterval(
            datetime.datetime(2010, 11, 20, 11, 30, 15)
        )

        assert interval.start == datetime.datetime(2010, 11, 20, 11, 30, 15)
        assert interval.end is None
        assert str(interval) == "[2010-11-20 11:30:15 : ]"

    def test_start_and_stop_set(self):
        """Test TimeInterval with start and end."""
        interval = blitzortung.db.query.TimeInterval(
            datetime.datetime(2010, 11, 20, 11, 30, 15),
            datetime.datetime(2010, 12, 5, 23, 15, 59),
        )

        assert interval.start == datetime.datetime(2010, 11, 20, 11, 30, 15)
        assert interval.end == datetime.datetime(2010, 12, 5, 23, 15, 59)
        assert str(interval) == "[2010-11-20 11:30:15 : 2010-12-05 23:15:59]"

    def test_get_duration(self):
        """Test duration calculation."""
        interval = blitzortung.db.query.TimeInterval(
            datetime.datetime(2010, 11, 20, 11, 30, 15),
            datetime.datetime(2010, 11, 20, 11, 40, 15),
        )

        assert interval.duration == datetime.timedelta(minutes=10)

    def test_get_minutes(self):
        """Test minutes calculation."""
        interval = blitzortung.db.query.TimeInterval(
            datetime.datetime(2010, 11, 20, 11, 30, 15),
            datetime.datetime(2010, 11, 20, 11, 40, 15),
        )

        result = interval.minutes()

        assert isinstance(result, int)
        assert result == 10

    def test_get_minutes_fails_with_incomplete_range(self):
        """Test minutes raises error for incomplete range."""
        interval1 = blitzortung.db.query.TimeInterval(
            None, datetime.datetime(2010, 11, 20, 11, 40, 15)
        )

        with pytest.raises(ValueError):
            interval1.minutes()

        interval2 = blitzortung.db.query.TimeInterval(
            datetime.datetime(2010, 11, 20, 11, 30, 15), None
        )
        with pytest.raises(ValueError):
            interval2.minutes()

        interval3 = blitzortung.db.query.TimeInterval(None, None)
        with pytest.raises(ValueError):
            interval3.minutes()

    def test_exception_when_start_is_not_datetime(self):
        """Test raising exception for non-datetime start."""
        with pytest.raises(ValueError):
            blitzortung.db.query.TimeInterval("asdf")

    def test_exception_when_end_is_not_datetime(self):
        """Test raising exception for non-datetime end."""
        with pytest.raises(ValueError):
            blitzortung.db.query.TimeInterval(
                datetime.datetime.now(datetime.timezone.utc), "asdf"
            )


class TestQuery:
    """Test suite for Query class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.query = blitzortung.db.query.Query()

    def test_initialization(self):
        """Test Query initialization."""
        assert str(self.query) == ""

    def test_add_group_by(self):
        """Test adding GROUP BY clause."""
        self.query.add_group_by("bar")
        assert str(self.query) == "GROUP BY bar"
        self.query.add_group_by("baz")
        assert str(self.query) == "GROUP BY bar, baz"

    def test_add_group_with_having_condition(self):
        """Test GROUP BY with HAVING clause."""
        self.query.add_group_by("bar")
        self.query.add_group_having("foo > 1")

        assert str(self.query) == "GROUP BY bar HAVING foo > 1"

    def test_add_having_has_no_effect_without_group(self):
        """Test HAVING without GROUP BY has no effect."""
        self.query.add_group_having("foo > 1")

        assert str(self.query) == ""

    def test_add_condition(self):
        """Test adding WHERE conditions."""
        self.query.add_condition("qux")
        assert str(self.query) == "WHERE qux"

        self.query.add_condition("quux")
        assert str(self.query) == "WHERE qux AND quux"

    def test_add_condition_with_parameters(self):
        """Test adding conditions with parameters."""
        self.query.add_condition("column LIKE %(name)s", name="<name>")
        assert_that(str(self.query)).is_equal_to("WHERE column LIKE %(name)s")
        assert_that(self.query.get_parameters()).is_equal_to({"name": "<name>"})

        self.query.add_condition("other LIKE %(type)s", type="<type>")
        assert_that(str(self.query)).is_equal_to(
            "WHERE column LIKE %(name)s AND other LIKE %(type)s"
        )
        assert_that(self.query.get_parameters()).is_equal_to(
            {"name": "<name>", "type": "<type>"}
        )

    def test_add_order(self):
        """Test setting ORDER BY clause."""
        self.query.set_order(blitzortung.db.query.Order("bar"))
        assert str(self.query) == "ORDER BY bar"

    def test_add_order_with_multiple_elements(self):
        """Test ORDER BY with multiple columns."""
        self.query.set_order(
            blitzortung.db.query.Order("bar"), blitzortung.db.query.Order("baz", True)
        )
        assert str(self.query) == "ORDER BY bar, baz DESC"

    def test_add_order_with_multiple_mixed_elements(self):
        """Test ORDER BY with mixed element types."""
        self.query.set_order("bar", blitzortung.db.query.Order("baz", True))
        assert str(self.query) == "ORDER BY bar, baz DESC"

    def test_set_limit(self):
        """Test setting LIMIT clause."""
        self.query.set_limit(10)
        assert str(self.query) == "LIMIT 10"

    def test_parse_args_with_id_interval(self):
        """Test default conditions with ID interval."""
        self.query.set_default_conditions(
            id_interval=blitzortung.db.query.IdInterval(10, 15)
        )

        assert_that(str(self.query)).is_equal_to(
            "WHERE id >= %(start_id)s AND id < %(end_id)s"
        )
        assert_that(self.query.get_parameters()).is_equal_to(
            {"start_id": 10, "end_id": 15}
        )

    def test_parse_args_with_time_interval(self):
        """Test default conditions with time interval."""
        self.query.set_default_conditions(
            time_interval=blitzortung.db.query.TimeInterval(
                datetime.datetime(2013, 10, 9, 17, 20),
                datetime.datetime(2013, 10, 11, 6, 30),
            )
        )

        assert_that(str(self.query)).is_equal_to(
            'WHERE "timestamp" >= %(start_time)s AND "timestamp" < %(end_time)s'
        )
        assert_that(self.query.get_parameters()).is_equal_to(
            {
                "start_time": datetime.datetime(2013, 10, 9, 17, 20),
                "end_time": datetime.datetime(2013, 10, 11, 6, 30),
            }
        )

    def test_parse_args_with_order(self):
        """Test default conditions with order."""
        self.query.set_default_conditions(order="test")

        assert_that(str(self.query)).is_equal_to("ORDER BY test")

    def test_parse_args_with_limit(self):
        """Test default conditions with limit."""
        self.query.set_default_conditions(limit=10)

        assert_that(str(self.query)).is_equal_to("LIMIT 10")


class TestSelectQuery:
    """Test suite for SelectQuery class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.query = blitzortung.db.query.SelectQuery()
        self.query.set_table_name("foo")

    def test_initialization(self):
        """Test SelectQuery initialization."""
        assert str(self.query) == "SELECT FROM foo"

    def test_set_table_name(self):
        """Test setting table name."""
        assert str(self.query) == "SELECT FROM foo"

    def test_set_columns(self):
        """Test setting columns."""
        self.query.set_columns("bar", "baz")
        assert str(self.query) == "SELECT bar, baz FROM foo"

    def test_add_column(self):
        """Test adding columns."""
        self.query.add_column("bar")
        assert str(self.query) == "SELECT bar FROM foo"
        self.query.add_column("baz")
        assert str(self.query) == "SELECT bar, baz FROM foo"

    def test_add_condition(self):
        """Test adding WHERE conditions."""
        self.query.add_condition("qux")
        assert str(self.query) == "SELECT FROM foo WHERE qux"

        self.query.add_condition("quux")
        assert str(self.query) == "SELECT FROM foo WHERE qux AND quux"

    def test_add_parameters(self):
        """Test adding parameters."""
        assert_that(self.query.get_parameters()).is_equal_to({})

        self.query.add_parameters(foo="bar", baz="qux")

        assert_that(self.query.get_parameters()).is_equal_to(
            {"foo": "bar", "baz": "qux"}
        )


class TestGridQuery:
    """Test suite for GridQuery class."""

    def test_with_raster(self):
        """Test GridQuery with raster grid."""
        raster = blitzortung.geom.Grid(-10, 20, 15, 35, 1.5, 1)

        query = blitzortung.db.query.GridQuery(raster)
        query.set_table_name("strikes")

        expected_query = (
            "SELECT "
            "TRUNC((ST_X(ST_Transform(geog::geometry, %(srid)s)) - %(xmin)s) "
            "/ %(xdiv)s)::integer AS rx, "
            "TRUNC((ST_Y(ST_Transform(geog::geometry, %(srid)s)) - %(ymin)s) "
            "/ %(ydiv)s)::integer AS ry, "
            "count(*) AS strike_count, "
            'max("timestamp") as "timestamp" '
            "FROM strikes "
            "WHERE ST_GeomFromWKB(%(envelope)s, %(envelope_srid)s) && geog "
            "GROUP BY rx, ry"
        )
        assert_that(str(query)).is_equal_to(expected_query)

        parameters = query.get_parameters()
        assert_that(parameters["xmin"]).is_equal_to(-10)
        assert_that(parameters["xdiv"]).is_equal_to(1.5)
        assert_that(parameters["ymin"]).is_equal_to(15)
        assert_that(parameters["ydiv"]).is_equal_to(1)
        assert_that(parameters["srid"]).is_equal_to(4326)
        assert_that(parameters["envelope_srid"]).is_equal_to(4326)
        wkb = parameters["envelope"].adapted
        envelope = shapely.wkb.loads(wkb)
        assert_that(envelope.bounds).is_equal_to((-10, 15, 20, 35))
