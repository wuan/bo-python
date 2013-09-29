import unittest
import math
import datetime
from hamcrest import assert_that, contains
import pytz

import blitzortung


class TimeIntervalsTest(unittest.TestCase):
    def setUp(self):
        self.duration = datetime.timedelta(minutes=10)

    def initialize_times(self, end_time):
        self.end_time = end_time
        end_time = end_time.replace(tzinfo=pytz.UTC)
        self.start_time = end_time - datetime.timedelta(minutes=25)

    def test_time_intervals_generator(self):
        self.initialize_times(datetime.datetime(2013, 8, 20, 12, 9, 0))

        times = [time for time in blitzortung.util.time_intervals(self.start_time, self.duration, self.end_time)]

        assert_that(times, contains(
           datetime.datetime(2013, 8, 20, 11, 40, 0),
           datetime.datetime(2013, 8, 20, 11, 50, 0),
           datetime.datetime(2013, 8, 20, 12,  0, 0),
        ))

    def test_time_intervals_generator_at_start_of_interval(self):
        self.initialize_times(datetime.datetime(2013, 8, 20, 12, 5, 0))

        times = [time for time in blitzortung.util.time_intervals(self.start_time, self.duration, self.end_time)]

        assert_that(times, contains(
            datetime.datetime(2013, 8, 20, 11, 40, 0),
            datetime.datetime(2013, 8, 20, 11, 50, 0),
            datetime.datetime(2013, 8, 20, 12,  0, 0),
            ))

    def test_time_intervals_generator_before_start_of_interval(self):
        self.initialize_times(datetime.datetime(2013, 8, 20, 12, 4, 59))

        times = [time for time in blitzortung.util.time_intervals(self.start_time, self.duration, self.end_time)]

        assert_that(times, contains(
            datetime.datetime(2013, 8, 20, 11, 30, 0),
            datetime.datetime(2013, 8, 20, 11, 40, 0),
            datetime.datetime(2013, 8, 20, 11, 50, 0),
            datetime.datetime(2013, 8, 20, 12,  0, 0),
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
