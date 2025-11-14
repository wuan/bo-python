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

from assertpy import assert_that  # pylint: disable=import-error
import pytest  # pylint: disable=import-error

import blitzortung.builder.strike
import blitzortung.builder.base
import blitzortung.data


class TestStrikeBuilder:
    """Test suite for Strike builder class."""

    def test_strike_inherits_from_event(self):
        """Test that Strike inherits from Event."""
        assert_that(
            issubclass(
                blitzortung.builder.strike.Strike, blitzortung.builder.base.Event
            )
        ).is_true()

    def test_strike_initialization(self):
        """Test Strike initializes with default values."""
        strike = blitzortung.builder.strike.Strike()
        assert_that(strike.id_value).is_equal_to(-1)
        assert_that(strike.altitude).is_none()
        assert_that(strike.amplitude).is_none()
        assert_that(strike.lateral_error).is_none()
        assert_that(strike.station_count).is_none()
        assert_that(strike.stations).is_equal_to([])

    def test_position_parser_regex(self):
        """Test that position parser regex is defined."""
        parser = blitzortung.builder.strike.Strike.position_parser
        assert_that(parser).is_not_none()
        # Test parsing
        match = parser.findall("pos;-5.2;10.3;200.5")
        assert_that(match).is_length(1)
        assert_that(match[0]).is_equal_to(("-5.2", "10.3", "200.5"))

    def test_amplitude_parser_regex(self):
        """Test that amplitude parser regex is defined."""
        parser = blitzortung.builder.strike.Strike.amplitude_parser
        assert_that(parser).is_not_none()
        match = parser.findall("str;45.3")
        assert_that(match).is_equal_to(["45.3"])

    def test_deviation_parser_regex(self):
        """Test that deviation parser regex is defined."""
        parser = blitzortung.builder.strike.Strike.deviation_parser
        assert_that(parser).is_not_none()
        match = parser.findall("dev;123.45")
        assert_that(match).is_equal_to(["123.45"])

    def test_stations_parser_regex(self):
        """Test that stations parser regex is defined."""
        parser = blitzortung.builder.strike.Strike.stations_parser
        assert_that(parser).is_not_none()
        match = parser.findall("sta;5;10;1,2,3,4,5")
        assert_that(match).is_length(1)
        assert_that(match[0]).is_equal_to(("5", "10", "1,2,3,4,5"))

    def test_set_id(self):
        """Test setting strike id."""
        strike = blitzortung.builder.strike.Strike()
        result = strike.set_id(12345)
        assert_that(result).is_equal_to(strike)  # Returns self
        assert_that(strike.id_value).is_equal_to(12345)

    def test_set_altitude(self):
        """Test setting altitude."""
        strike = blitzortung.builder.strike.Strike()
        result = strike.set_altitude(1500.5)
        assert_that(result).is_equal_to(strike)
        assert_that(strike.altitude).is_equal_to(1500.5)

    def test_set_amplitude(self):
        """Test setting amplitude."""
        strike = blitzortung.builder.strike.Strike()
        result = strike.set_amplitude(42.3)
        assert_that(result).is_equal_to(strike)
        assert_that(strike.amplitude).is_equal_to(42.3)

    def test_set_lateral_error(self):
        """Test setting lateral error."""
        strike = blitzortung.builder.strike.Strike()
        result = strike.set_lateral_error(500.0)
        assert_that(result).is_equal_to(strike)
        # Should be clamped to 0-32767 range
        assert_that(strike.lateral_error).is_greater_than_or_equal_to(0)
        assert_that(strike.lateral_error).is_less_than_or_equal_to(32767)

    def test_set_lateral_error_clamping_lower(self):
        """Test that lateral error is clamped to minimum 0."""
        strike = blitzortung.builder.strike.Strike()
        strike.set_lateral_error(-100.0)
        assert_that(strike.lateral_error).is_equal_to(0)

    def test_set_lateral_error_clamping_upper(self):
        """Test that lateral error is clamped to maximum 32767."""
        strike = blitzortung.builder.strike.Strike()
        strike.set_lateral_error(100000.0)
        assert_that(strike.lateral_error).is_equal_to(32767)

    def test_set_station_count(self):
        """Test setting station count."""
        strike = blitzortung.builder.strike.Strike()
        result = strike.set_station_count(15)
        assert_that(result).is_equal_to(strike)
        assert_that(strike.station_count).is_equal_to(15)

    def test_set_stations(self):
        """Test setting stations list."""
        strike = blitzortung.builder.strike.Strike()
        stations = [1, 2, 3, 4, 5]
        result = strike.set_stations(stations)
        assert_that(result).is_equal_to(strike)
        assert_that(strike.stations).is_equal_to(stations)

    def test_method_chaining(self):
        """Test that methods can be chained."""
        strike = blitzortung.builder.strike.Strike()
        result = (
            strike.set_id(1).set_altitude(1000).set_amplitude(50).set_station_count(10)
        )
        assert_that(result).is_same_as(strike)
        assert_that(strike.id_value).is_equal_to(1)
        assert_that(strike.altitude).is_equal_to(1000)
        assert_that(strike.amplitude).is_equal_to(50)
        assert_that(strike.station_count).is_equal_to(10)

    def test_from_line_with_valid_data(self):
        """Test parsing strike from valid data line."""
        strike = blitzortung.builder.strike.Strike()
        # Example line format with all required fields
        line = "2025-01-15T12:30:45.123456+00:00 pos;48.5;-10.2;500.5 str;45.2 dev;250.0 sta;5;10;1,2,3,4,5"

        result = strike.from_line(line)
        assert_that(result).is_same_as(strike)
        assert_that(strike.x_coord).is_equal_to(-10.2)
        assert_that(strike.y_coord).is_equal_to(48.5)
        assert_that(strike.altitude).is_equal_to(500.5)
        assert_that(strike.amplitude).is_equal_to(45.2)
        assert_that(strike.lateral_error).is_equal_to(250)
        assert_that(strike.station_count).is_equal_to(5)
        assert_that(strike.stations).is_equal_to([1, 2, 3, 4, 5])

    def test_from_line_with_missing_stations(self):
        """Test parsing strike with missing stations in list."""
        strike = blitzortung.builder.strike.Strike()
        line = "2025-01-15T12:30:45.123456+00:00 pos;48.5;-10.2;500.5 str;45.2 dev;250.0 sta;3;10;1,,3"

        result = strike.from_line(line)
        assert_that(result).is_same_as(strike)
        # Empty string should be filtered out
        assert_that(strike.stations).is_equal_to([1, 3])

    def test_from_line_with_no_stations(self):
        """Test parsing strike with empty stations list."""
        strike = blitzortung.builder.strike.Strike()
        line = "2025-01-15T12:30:45.123456+00:00 pos;48.5;-10.2;500.5 str;45.2 dev;250.0 sta;0;10;"

        result = strike.from_line(line)
        assert_that(strike.stations).is_equal_to([])

    def test_from_line_with_invalid_format_raises_error(self):
        """Test that invalid line format raises BuilderError."""
        strike = blitzortung.builder.strike.Strike()
        line = "invalid line format"

        with pytest.raises(blitzortung.builder.base.BuilderError):
            strike.from_line(line)

    def test_from_line_with_missing_position_raises_error(self):
        """Test that missing position raises BuilderError."""
        strike = blitzortung.builder.strike.Strike()
        line = "2025-01-15T12:30:45.123456+00:00 str;45.2 dev;250.0 sta;5;10;1,2,3"

        with pytest.raises(blitzortung.builder.base.BuilderError):
            strike.from_line(line)

    def test_from_line_with_invalid_float_raises_error(self):
        """Test that non-numeric values raise BuilderError."""
        strike = blitzortung.builder.strike.Strike()
        line = "2025-01-15T12:30:45.123456+00:00 pos;invalid;-10.2;500.5 str;45.2 dev;250.0 sta;5;10;1,2,3"

        with pytest.raises(blitzortung.builder.base.BuilderError):
            strike.from_line(line)

    def test_build_returns_strike_object(self):
        """Test that build returns data.Strike object."""
        strike = blitzortung.builder.strike.Strike()
        dt = datetime.datetime.now(datetime.timezone.utc)
        strike.set_timestamp(dt).set_id(100).set_x(5.5).set_y(10.2).set_altitude(1000)
        strike.set_amplitude(50.5).set_lateral_error(250).set_station_count(5)

        result = strike.build()
        assert_that(result).is_instance_of(blitzortung.data.Strike)

    def test_build_with_complete_data(self):
        """Test building strike with all data set."""
        strike = blitzortung.builder.strike.Strike()
        dt = datetime.datetime.now(datetime.timezone.utc)
        strike.set_timestamp(dt)
        strike.set_id(42)
        strike.set_x(8.5)
        strike.set_y(50.2)
        strike.set_altitude(1500.5)
        strike.set_amplitude(45.3)
        strike.set_lateral_error(300)
        strike.set_station_count(12)
        strike.set_stations([1, 2, 3, 4])

        result = strike.build()
        assert_that(result.id).is_equal_to(42)
        assert_that(result.x).is_equal_to(8.5)
        assert_that(result.y).is_equal_to(50.2)
        assert_that(result.altitude).is_equal_to(1500.5)
        assert_that(result.amplitude).is_equal_to(45.3)
        assert_that(result.lateral_error).is_equal_to(300)
        assert_that(result.station_count).is_equal_to(12)
        assert_that(result.stations).is_equal_to([1, 2, 3, 4])

    def test_build_with_default_id(self):
        """Test building strike with default id value."""
        strike = blitzortung.builder.strike.Strike()
        dt = datetime.datetime.now(datetime.timezone.utc)
        strike.set_timestamp(dt).set_x(0).set_y(0)

        result = strike.build()
        assert_that(result.id).is_equal_to(-1)  # Default value

    def test_from_line_parsing_negative_coordinates(self):
        """Test parsing strike with negative coordinates."""
        strike = blitzortung.builder.strike.Strike()
        line = "2025-01-15T12:30:45.123456+00:00 pos;-48.5;-10.2;500.5 str;45.2 dev;250.0 sta;5;10;1,2,3,4,5"

        strike.from_line(line)
        assert_that(strike.x_coord).is_equal_to(-10.2)
        assert_that(strike.y_coord).is_equal_to(-48.5)
