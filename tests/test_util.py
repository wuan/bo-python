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
