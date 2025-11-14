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

import blitzortung.builder.base
import blitzortung.data


class TestBuilderError:
    """Test suite for BuilderError exception."""

    def test_is_error_subclass(self):
        """Test that BuilderError is a subclass of Error."""
        assert_that(
            issubclass(
                blitzortung.builder.base.BuilderError, blitzortung.builder.base.Error
            )
        ).is_true()

    def test_can_be_raised(self):
        """Test that BuilderError can be raised and caught."""
        exception = blitzortung.builder.base.BuilderError()
        assert_that(exception).is_instance_of(Exception)


class TestBase:
    """Test suite for Base class."""

    def test_base_can_be_instantiated(self):
        """Test that Base class can be instantiated."""
        base = blitzortung.builder.base.Base()
        assert_that(base).is_instance_of(blitzortung.builder.base.Base)

    def test_base_is_empty(self):
        """Test that Base class has no special attributes."""
        base = blitzortung.builder.base.Base()
        assert_that(base.__dict__).is_empty()


class TestTimestamp:
    """Test suite for Timestamp builder class."""

    def test_timestamp_initialization(self):
        """Test that Timestamp initializes with None timestamp."""
        ts = blitzortung.builder.base.Timestamp()
        assert_that(ts.timestamp).is_none()

    def test_timestamp_inherits_from_base(self):
        """Test that Timestamp inherits from Base."""
        assert_that(
            issubclass(
                blitzortung.builder.base.Timestamp, blitzortung.builder.base.Base
            )
        ).is_true()

    def test_set_timestamp_with_datetime(self):
        """Test setting timestamp with datetime object."""
        ts = blitzortung.builder.base.Timestamp()
        dt = datetime.datetime.now(datetime.timezone.utc)
        result = ts.set_timestamp(dt)

        assert_that(result).is_equal_to(ts)  # Returns self for chaining
        assert_that(ts.timestamp).is_instance_of(blitzortung.data.Timestamp)

    def test_set_timestamp_with_timestamp_object(self):
        """Test setting timestamp with Timestamp object."""
        ts = blitzortung.builder.base.Timestamp()
        data_ts = blitzortung.data.Timestamp()
        result = ts.set_timestamp(data_ts)

        assert_that(result).is_equal_to(ts)
        assert_that(ts.timestamp).is_instance_of(blitzortung.data.Timestamp)

    def test_set_timestamp_with_nanosecond_offset(self):
        """Test setting timestamp with nanosecond offset."""
        ts = blitzortung.builder.base.Timestamp()
        dt = datetime.datetime.now(datetime.timezone.utc)
        result = ts.set_timestamp(dt, nanosecond=500)

        assert_that(result).is_equal_to(ts)
        assert_that(ts.timestamp).is_instance_of(blitzortung.data.Timestamp)
        assert_that(ts.timestamp.nanosecond).is_equal_to(500)

    def test_set_timestamp_with_none_clears_timestamp(self):
        """Test that setting timestamp to None clears it."""
        ts = blitzortung.builder.base.Timestamp()
        dt = datetime.datetime.now(datetime.timezone.utc)
        ts.set_timestamp(dt)
        assert_that(ts.timestamp).is_not_none()

        ts.set_timestamp(None)
        assert_that(ts.timestamp).is_none()

    def test_set_timestamp_returns_self_for_chaining(self):
        """Test that set_timestamp returns self for method chaining."""
        ts = blitzortung.builder.base.Timestamp()
        dt = datetime.datetime.now(datetime.timezone.utc)

        result = ts.set_timestamp(dt)
        assert_that(result).is_same_as(ts)

    def test_build_returns_timestamp(self):
        """Test that build returns the timestamp."""
        ts = blitzortung.builder.base.Timestamp()
        dt = datetime.datetime.now(datetime.timezone.utc)
        ts.set_timestamp(dt)

        result = ts.build()
        assert_that(result).is_equal_to(ts.timestamp)

    def test_build_returns_none_when_no_timestamp_set(self):
        """Test that build returns None when no timestamp set."""
        ts = blitzortung.builder.base.Timestamp()
        result = ts.build()
        assert_that(result).is_none()

    def test_timestamp_with_zero_nanoseconds(self):
        """Test setting timestamp with zero nanoseconds."""
        ts = blitzortung.builder.base.Timestamp()
        dt = datetime.datetime.now(datetime.timezone.utc)
        ts.set_timestamp(dt, nanosecond=0)

        assert_that(ts.timestamp.nanosecond).is_equal_to(0)

    def test_timestamp_with_multiple_nanosecond_offsets(self):
        """Test setting timestamp with multiple nanosecond offsets."""
        ts = blitzortung.builder.base.Timestamp()
        data_ts = blitzortung.data.Timestamp()

        # Set initial nanoseconds
        ts.set_timestamp(data_ts, nanosecond=100)
        # Add more nanoseconds
        ts.set_timestamp(data_ts, nanosecond=200)

        # The second call should accumulate
        assert_that(ts.timestamp.nanosecond).is_equal_to(200)


class TestEvent:
    """Test suite for Event builder class."""

    def test_event_initialization(self):
        """Test that Event initializes with zero coordinates."""
        event = blitzortung.builder.base.Event()
        assert_that(event.x_coord).is_equal_to(0)
        assert_that(event.y_coord).is_equal_to(0)
        assert_that(event.timestamp).is_none()

    def test_event_inherits_from_timestamp(self):
        """Test that Event inherits from Timestamp."""
        assert_that(
            issubclass(
                blitzortung.builder.base.Event, blitzortung.builder.base.Timestamp
            )
        ).is_true()

    def test_set_x_coordinate(self):
        """Test setting x coordinate."""
        event = blitzortung.builder.base.Event()
        result = event.set_x(5.5)

        assert_that(result).is_equal_to(event)  # Returns self
        assert_that(event.x_coord).is_equal_to(5.5)

    def test_set_y_coordinate(self):
        """Test setting y coordinate."""
        event = blitzortung.builder.base.Event()
        result = event.set_y(7.2)

        assert_that(result).is_equal_to(event)  # Returns self
        assert_that(event.y_coord).is_equal_to(7.2)

    def test_set_x_returns_self_for_chaining(self):
        """Test that set_x returns self for method chaining."""
        event = blitzortung.builder.base.Event()
        result = event.set_x(10)
        assert_that(result).is_same_as(event)

    def test_set_y_returns_self_for_chaining(self):
        """Test that set_y returns self for method chaining."""
        event = blitzortung.builder.base.Event()
        result = event.set_y(20)
        assert_that(result).is_same_as(event)

    def test_method_chaining(self):
        """Test that methods can be chained together."""
        event = blitzortung.builder.base.Event()
        dt = datetime.datetime.now(datetime.timezone.utc)

        result = event.set_timestamp(dt).set_x(5).set_y(10)

        assert_that(result).is_same_as(event)
        assert_that(event.timestamp).is_not_none()
        assert_that(event.x_coord).is_equal_to(5)
        assert_that(event.y_coord).is_equal_to(10)

    def test_build_returns_event_object(self):
        """Test that build returns a data.Event object."""
        event = blitzortung.builder.base.Event()
        dt = datetime.datetime.now(datetime.timezone.utc)
        event.set_timestamp(dt).set_x(3.5).set_y(4.5)

        result = event.build()
        assert_that(result).is_instance_of(blitzortung.data.Event)

    def test_build_event_with_coordinates(self):
        """Test that built event has correct coordinates."""
        event = blitzortung.builder.base.Event()
        dt = datetime.datetime.now(datetime.timezone.utc)
        event.set_timestamp(dt).set_x(1.5).set_y(2.5)

        result = event.build()
        # Event should have x and y attributes
        assert_that(result.x).is_equal_to(1.5)
        assert_that(result.y).is_equal_to(2.5)

    def test_build_event_with_timestamp(self):
        """Test that built event has correct timestamp."""
        event = blitzortung.builder.base.Event()
        dt = datetime.datetime.now(datetime.timezone.utc)
        event.set_timestamp(dt).set_x(1).set_y(2)

        result = event.build()
        assert_that(result.timestamp).is_not_none()

    def test_build_with_negative_coordinates(self):
        """Test building event with negative coordinates."""
        event = blitzortung.builder.base.Event()
        event.set_x(-10.5).set_y(-20.3)

        result = event.build()
        assert_that(result.x).is_equal_to(-10.5)
        assert_that(result.y).is_equal_to(-20.3)

    def test_build_with_zero_coordinates(self):
        """Test building event with zero coordinates."""
        event = blitzortung.builder.base.Event()
        event.set_x(0).set_y(0)

        result = event.build()
        assert_that(result.x).is_equal_to(0)
        assert_that(result.y).is_equal_to(0)

    def test_build_with_large_coordinates(self):
        """Test building event with large coordinates."""
        event = blitzortung.builder.base.Event()
        event.set_x(999999.999).set_y(888888.888)

        result = event.build()
        assert_that(result.x).is_equal_to(999999.999)
        assert_that(result.y).is_equal_to(888888.888)

    def test_multiple_event_builds(self):
        """Test creating multiple events sequentially."""
        dt = datetime.datetime.now(datetime.timezone.utc)

        # First event
        event1 = blitzortung.builder.base.Event()
        event1.set_timestamp(dt).set_x(1).set_y(2)
        result1 = event1.build()

        # Second event with different coordinates
        event2 = blitzortung.builder.base.Event()
        event2.set_timestamp(dt).set_x(3).set_y(4)
        result2 = event2.build()

        # Results should have different coordinates
        assert_that(result1.x).is_equal_to(1)
        assert_that(result1.y).is_equal_to(2)
        assert_that(result2.x).is_equal_to(3)
        assert_that(result2.y).is_equal_to(4)

    def test_overwriting_coordinates(self):
        """Test that coordinates can be overwritten."""
        event = blitzortung.builder.base.Event()
        event.set_x(5).set_y(10)
        assert_that(event.x_coord).is_equal_to(5)
        assert_that(event.y_coord).is_equal_to(10)

        # Overwrite with new values
        event.set_x(15).set_y(20)
        assert_that(event.x_coord).is_equal_to(15)
        assert_that(event.y_coord).is_equal_to(20)

        result = event.build()
        assert_that(result.x).is_equal_to(15)
        assert_that(result.y).is_equal_to(20)
