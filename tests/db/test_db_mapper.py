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
from zoneinfo import ZoneInfo

import pytest
from assertpy import assert_that
from mock import Mock, call

import blitzortung.builder
import blitzortung.db.mapper


class TestStrikeMapper:
    """Test suite for Strike mapper class."""

    @pytest.fixture
    def strike(self):
        """Fixture for mock strike object."""
        return Mock(name="strike")

    @pytest.fixture
    def strike_builder(self, strike):
        """Fixture for mock strike builder."""
        mock = Mock(name="strike_builder", spec=blitzortung.builder.Strike)
        mock.build.return_value = strike
        return mock

    @pytest.fixture
    def strike_mapper(self, strike_builder):
        """Fixture for strike mapper instance."""
        return blitzortung.db.mapper.Strike(strike_builder)

    @pytest.fixture
    def timestamp(self):
        """Fixture for current timestamp."""
        return datetime.datetime.now(datetime.UTC).replace(tzinfo=datetime.UTC)

    @pytest.fixture
    def result(self, timestamp):
        """Fixture for database result row."""
        return {
            "id": 12,
            "timestamp": timestamp,
            "nanoseconds": 789,
            "x": 11.0,
            "y": 51.0,
            "altitude": 123,
            "amplitude": 21323,
            "stationcount": 12,
            "error2d": 5000,
        }

    def test_strike_mapper(
        self, strike_mapper, strike_builder, result, strike, timestamp
    ):
        """Test strike mapper converts database result to strike object."""
        create_object = strike_mapper.create_object(result)
        assert_that(create_object).is_equal_to(strike)

        assert_that(strike_builder.method_calls).is_equal_to(
            [
                call.set_id(12),
                call.set_timestamp(timestamp, 789),
                call.set_x(11.0),
                call.set_y(51.0),
                call.set_altitude(123),
                call.set_amplitude(21323),
                call.set_station_count(12),
                call.set_lateral_error(5000),
                call.build(),
            ]
        )

    def test_strike_mapper_with_timezone(
        self, strike_mapper, strike_builder, result, timestamp
    ):
        """Test strike mapper handles timezone conversion."""
        zone = ZoneInfo("CET")

        strike_mapper.create_object(result, timezone=zone)

        result_timetamp = strike_builder.set_timestamp.call_args[0][0]

        assert_that(result_timetamp).is_equal_to(timestamp)
        assert_that(result_timetamp.tzinfo).is_equal_to(zone)

    def test_strike_mapper_without_timestamp(
        self, strike_mapper, strike_builder, result
    ):
        """Test strike mapper handles missing timestamp."""
        result["timestamp"] = None

        strike_mapper.create_object(result)

        assert_that(strike_builder.set_timestamp.call_args[0][0]).is_none()
