# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import unittest
import datetime

from hamcrest import assert_that, contains
import pytz

import blitzortung
import blitzortung.util


class TimeIntervalsTest(unittest.TestCase):
    def setUp(self):
        self.duration = datetime.timedelta(minutes=10)

    def initialize_times(self, end_time):
        self.end_time = end_time
        self.start_time = self.end_time - datetime.timedelta(minutes=25)

    def test_time_intervals_generator(self):
        self.initialize_times(datetime.datetime(2013, 8, 20, 12, 9, 0))

        times = [time for time in blitzortung.util.time_intervals(self.start_time, self.duration, self.end_time)]

        assert_that(times, contains(
            datetime.datetime(2013, 8, 20, 11, 40, 0),
            datetime.datetime(2013, 8, 20, 11, 50, 0),
            datetime.datetime(2013, 8, 20, 12, 0, 0),
        ))

    def test_time_intervals_generator_at_start_of_interval(self):
        self.initialize_times(datetime.datetime(2013, 8, 20, 12, 5, 0))

        times = [time for time in blitzortung.util.time_intervals(self.start_time, self.duration, self.end_time)]

        assert_that(times, contains(
            datetime.datetime(2013, 8, 20, 11, 40, 0),
            datetime.datetime(2013, 8, 20, 11, 50, 0),
            datetime.datetime(2013, 8, 20, 12, 0, 0),
        ))

    def test_time_intervals_generator_before_start_of_interval(self):
        self.initialize_times(datetime.datetime(2013, 8, 20, 12, 4, 59))

        times = [time for time in blitzortung.util.time_intervals(self.start_time, self.duration, self.end_time)]

        assert_that(times, contains(
            datetime.datetime(2013, 8, 20, 11, 30, 0),
            datetime.datetime(2013, 8, 20, 11, 40, 0),
            datetime.datetime(2013, 8, 20, 11, 50, 0),
            datetime.datetime(2013, 8, 20, 12, 0, 0),
        ))

    def test_time_intervals_generator_at_current_time(self):
        end_time = datetime.datetime.utcnow()
        end_time = end_time.replace(tzinfo=pytz.UTC)
        start_time = end_time - self.duration

        times = [time for time in blitzortung.util.time_intervals(start_time, self.duration)]

        assert_that(times, contains(
            blitzortung.util.round_time(start_time, self.duration),
            blitzortung.util.round_time(end_time, self.duration)
        ))
