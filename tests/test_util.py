# -*- coding: utf8 -*-

"""

   Copyright 2013-2025 Andreas Würl

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
import time

import pytest
from assertpy import assert_that

import blitzortung.util
from blitzortung.data import Timestamp


class TestTimeIntervals:
    def setup_class(self):
        self.duration = datetime.timedelta(minutes=10)

    def initialize_times(self, end_time):
        self.end_time = end_time
        self.start_time = self.end_time - datetime.timedelta(minutes=25)

    def test_time_intervals_generator(self):
        self.initialize_times(datetime.datetime(2013, 8, 20, 12, 9, 0))

        times = [time for time in blitzortung.util.time_intervals(self.start_time, self.duration, self.end_time)]

        assert_that(times).contains(
            datetime.datetime(2013, 8, 20, 11, 40, 0),
            datetime.datetime(2013, 8, 20, 11, 50, 0),
            datetime.datetime(2013, 8, 20, 12, 0, 0),
        )

    def test_time_intervals_generator_at_start_of_interval(self):
        self.initialize_times(datetime.datetime(2013, 8, 20, 12, 5, 0))

        times = [time for time in blitzortung.util.time_intervals(self.start_time, self.duration, self.end_time)]

        assert_that(times).contains(
            datetime.datetime(2013, 8, 20, 11, 40, 0),
            datetime.datetime(2013, 8, 20, 11, 50, 0),
            datetime.datetime(2013, 8, 20, 12, 0, 0),
        )

    def test_time_intervals_generator_before_start_of_interval(self):
        self.initialize_times(datetime.datetime(2013, 8, 20, 12, 4, 59))

        times = [time for time in blitzortung.util.time_intervals(self.start_time, self.duration, self.end_time)]

        assert_that(times).contains(
            datetime.datetime(2013, 8, 20, 11, 30, 0),
            datetime.datetime(2013, 8, 20, 11, 40, 0),
            datetime.datetime(2013, 8, 20, 11, 50, 0),
            datetime.datetime(2013, 8, 20, 12, 0, 0),
        )

    def test_time_intervals_generator_at_current_time(self):
        end_time = datetime.datetime.now(datetime.timezone.utc)
        start_time = end_time - self.duration

        times = [time for time in blitzortung.util.time_intervals(start_time, self.duration)]

        assert_that(times).contains(
            blitzortung.util.round_time(start_time, self.duration),
            blitzortung.util.round_time(end_time, self.duration)
        )


class TestRoundTime:

    def test_round_time_with_datetime(self):
        time_value = datetime.datetime(2013, 8, 20, 12, 3, 23, 123456)

        result = blitzortung.util.round_time(time_value, datetime.timedelta(minutes=2))

        assert_that(result.hour).is_equal_to(12)
        assert_that(result.minute).is_equal_to(2)
        assert_that(result.second).is_equal_to(0)
        assert_that(result.microsecond).is_equal_to(0)

    def test_round_time_with_timestamp(self):
        time_value = datetime.datetime(2013, 8, 20, 12, 3, 23, 123456)

        result = blitzortung.util.round_time(Timestamp(time_value, 789), datetime.timedelta(minutes=2))

        assert_that(result.hour).is_equal_to(12)
        assert_that(result.minute).is_equal_to(2)
        assert_that(result.second).is_equal_to(0)
        assert_that(result.microsecond).is_equal_to(0)
        assert_that(result.nanosecond).is_equal_to(0)

    def test_round_time_does_not_touch_datetime_argument(self):
        time_value = datetime.datetime(2013, 8, 20, 12, 0, 23, 123456)

        blitzortung.util.round_time(time_value, datetime.timedelta(minutes=1))

        assert_that(time_value.second).is_equal_to(23)
        assert_that(time_value.microsecond).is_equal_to(123456)

    def test_round_time_does_not_touch_timestamp_argument(self):
        time_value = Timestamp(datetime.datetime(2013, 8, 20, 12, 0, 23, 123456), 789)

        blitzortung.util.round_time(time_value, datetime.timedelta(minutes=1))

        assert_that(time_value.second).is_equal_to(23)
        assert_that(time_value.microsecond).is_equal_to(123456)
        assert_that(time_value.nanosecond).is_equal_to(789)


class TestLimit:

    def test_limit_value_in_rage(self):
        assert blitzortung.util.force_range(10,15,20) == 15

    def test_limit_value_below_rage(self):
        assert blitzortung.util.force_range(10,9,20) == 10

    def test_limit_value_above_rage(self):
        assert blitzortung.util.force_range(10,21,20) == 20


class TestTimer:

    def test_initialization(self):
        timer = blitzortung.util.Timer()
        assert_that(timer.start_time).is_not_none()
        assert_that(timer.lap_time).is_equal_to(timer.start_time)

    def test_read(self):
        timer = blitzortung.util.Timer()
        time.sleep(0.01)
        assert_that(timer.read()).is_between(0.01, 1.0)

    def test_lap(self):
        timer = blitzortung.util.Timer()
        time.sleep(0.01)
        lap1 = timer.lap()
        time.sleep(0.01)
        lap2 = timer.lap()

        assert_that(lap1).is_between(0.01, 1.0)
        assert_that(lap2).is_between(0.01, 1.0)


class TestTotalSeconds:

    def test_with_datetime(self):
        result = blitzortung.util.total_seconds(datetime.datetime(2013, 8, 20, 12, 30, 45))
        assert_that(result).is_equal_to(12 * 3600 + 30 * 60 + 45)

    def test_with_timestamp(self):
        result = blitzortung.util.total_seconds(Timestamp(datetime.datetime(2013, 8, 20, 5, 15, 30), 123))
        assert_that(result).is_equal_to(5 * 3600 + 15 * 60 + 30)

    def test_with_timedelta(self):
        result = blitzortung.util.total_seconds(datetime.timedelta(hours=2, minutes=30, seconds=15))
        assert_that(result).is_equal_to(2 * 3600 + 30 * 60 + 15)

    def test_with_timedelta_and_days(self):
        result = blitzortung.util.total_seconds(datetime.timedelta(days=2, hours=3, minutes=15, seconds=30))
        assert_that(result).is_equal_to(2 * 24 * 3600 + 3 * 3600 + 15 * 60 + 30)

    def test_invalid_type_raises_error(self):
        with pytest.raises(ValueError, match="unhandled type"):
            blitzortung.util.total_seconds("invalid")


class TestTimeConstraint:

    @pytest.fixture
    def constraint(self):
        """Fixture providing a TimeConstraint with default=60, max=1440."""
        return blitzortung.util.TimeConstraint(60, 1440)

    def test_initialization(self, constraint):
        assert_that(constraint.default_minute_length).is_equal_to(60)
        assert_that(constraint.max_minute_length).is_equal_to(1440)

    def test_enforce_with_valid_values(self, constraint):
        minute_length, minute_offset = constraint.enforce(120, -60)

        assert_that(minute_length).is_equal_to(120)
        assert_that(minute_offset).is_equal_to(-60)

    def test_enforce_with_zero_minute_length_uses_default(self, constraint):
        minute_length, minute_offset = constraint.enforce(0, 0)

        assert_that(minute_length).is_equal_to(60)
        assert_that(minute_offset).is_equal_to(0)

    def test_enforce_clamps_minute_length_to_max(self, constraint):
        minute_length, minute_offset = constraint.enforce(2000, 0)

        assert_that(minute_length).is_equal_to(1440)
        assert_that(minute_offset).is_equal_to(0)

    def test_enforce_clamps_negative_minute_length_to_zero_then_defaults(self, constraint):
        minute_length, minute_offset = constraint.enforce(-100, 0)

        # Negative minute_length gets clamped, then defaults
        assert_that(minute_length).is_equal_to(60)
        assert_that(minute_offset).is_equal_to(0)

    def test_enforce_clamps_minute_offset_to_zero_max(self, constraint):
        minute_length, minute_offset = constraint.enforce(60, 100)

        assert_that(minute_length).is_equal_to(60)
        assert_that(minute_offset).is_equal_to(0)

    def test_enforce_clamps_minute_offset_to_valid_negative_range(self, constraint):
        minute_length, minute_offset = constraint.enforce(60, -2000)

        assert_that(minute_length).is_equal_to(60)
        # Minimum offset is -max_minute_length + minute_length = -1440 + 60 = -1380
        assert_that(minute_offset).is_equal_to(-1380)

    def test_enforce_with_small_minute_length_allows_small_offset_range(self, constraint):
        minute_length, minute_offset = constraint.enforce(10, -20)

        assert_that(minute_length).is_equal_to(10)
        # Minimum offset is -1440 + 10 = -1430
        # -20 is within range, so it should remain -20
        assert_that(minute_offset).is_equal_to(-20)

    def test_enforce_boundary_minute_length_equals_max(self, constraint):
        minute_length, minute_offset = constraint.enforce(1440, -100)

        assert_that(minute_length).is_equal_to(1440)
        # With minute_length = 1440 (max), minimum offset is -1440 + 1440 = 0
        # So -100 gets clamped to 0
        assert_that(minute_offset).is_equal_to(0)

    def test_enforce_boundary_minute_offset_at_zero(self, constraint):
        minute_length, minute_offset = constraint.enforce(60, 0)

        assert_that(minute_length).is_equal_to(60)
        assert_that(minute_offset).is_equal_to(0)

    def test_enforce_boundary_minute_offset_at_minimum(self, constraint):
        minute_length, minute_offset = constraint.enforce(60, -1380)

        assert_that(minute_length).is_equal_to(60)
        assert_that(minute_offset).is_equal_to(-1380)

    def test_enforce_with_maximum_minute_length_allows_zero_offset_only(self, constraint):
        minute_length, minute_offset = constraint.enforce(1440, -10)

        assert_that(minute_length).is_equal_to(1440)
        # With minute_length = 1440, minimum offset is -1440 + 1440 = 0
        assert_that(minute_offset).is_equal_to(0)
