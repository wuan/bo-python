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
    """Test Timer class for time measurements."""

    def test_timer_initialization(self):
        """Test that Timer initializes with start and lap times."""
        timer = blitzortung.util.Timer()

        assert_that(timer.start_time).is_not_none()
        assert_that(timer.lap_time).is_equal_to(timer.start_time)

    def test_timer_read(self):
        """Test reading elapsed time from timer."""
        import time
        timer = blitzortung.util.Timer()
        time.sleep(0.01)

        elapsed = timer.read()

        assert_that(elapsed).is_greater_than_or_equal_to(0.01)
        assert_that(elapsed).is_less_than(1.0)

    def test_timer_lap(self):
        """Test lap functionality returns lap time and resets."""
        import time
        timer = blitzortung.util.Timer()
        time.sleep(0.01)

        lap1 = timer.lap()
        assert_that(lap1).is_greater_than_or_equal_to(0.01)
        assert_that(lap1).is_less_than(1.0)

        time.sleep(0.01)
        lap2 = timer.lap()
        assert_that(lap2).is_greater_than_or_equal_to(0.01)
        assert_that(lap2).is_less_than(1.0)


class TestTotalSeconds:
    """Test total_seconds function with different time types."""

    def test_total_seconds_with_datetime(self):
        """Test total_seconds with datetime object."""
        time_value = datetime.datetime(2013, 8, 20, 12, 30, 45)

        result = blitzortung.util.total_seconds(time_value)

        expected = 12 * 3600 + 30 * 60 + 45
        assert_that(result).is_equal_to(expected)

    def test_total_seconds_with_timestamp(self):
        """Test total_seconds with Timestamp object."""
        time_value = Timestamp(datetime.datetime(2013, 8, 20, 5, 15, 30), 123)

        result = blitzortung.util.total_seconds(time_value)

        expected = 5 * 3600 + 15 * 60 + 30
        assert_that(result).is_equal_to(expected)

    def test_total_seconds_with_timedelta(self):
        """Test total_seconds with timedelta object."""
        time_value = datetime.timedelta(hours=2, minutes=30, seconds=15)

        result = blitzortung.util.total_seconds(time_value)

        expected = 2 * 3600 + 30 * 60 + 15
        assert_that(result).is_equal_to(expected)

    def test_total_seconds_with_timedelta_including_days(self):
        """Test total_seconds with timedelta including days."""
        time_value = datetime.timedelta(days=2, hours=3, minutes=15, seconds=30)

        result = blitzortung.util.total_seconds(time_value)

        expected = 2 * 24 * 3600 + 3 * 3600 + 15 * 60 + 30
        assert_that(result).is_equal_to(expected)

    def test_total_seconds_with_invalid_type(self):
        """Test total_seconds raises ValueError with invalid type."""
        import pytest

        with pytest.raises(ValueError, match="unhandled type"):
            blitzortung.util.total_seconds("invalid")
