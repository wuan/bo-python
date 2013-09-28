import unittest
import datetime
from hamcrest import assert_that, is_, equal_to
import numpy as np
import pandas as pd
import pytz

import blitzortung


class TestTimeRange(unittest.TestCase):
    def setUp(self):
        self.end_time = datetime.datetime(2012, 3, 2, 11, 20, 24)
        self.interval = datetime.timedelta(hours=1, minutes=30, seconds=10)
        self.microsecond_delta = datetime.timedelta(microseconds=1)

        self.time_range = blitzortung.data.TimeRange(self.end_time, self.interval)

    def test_get_start_and_end_time(self):
        assert_that(self.time_range.get_start_time(), is_(equal_to(self.end_time - self.interval)))
        assert_that(self.time_range.get_end_time(), is_(equal_to(self.end_time)))
        assert_that(self.time_range.get_end_minute(), is_(equal_to(self.end_time - datetime.timedelta(minutes=1))))

    def test_contains(self):
        start_time = self.time_range.get_start_time()
        end_time = self.time_range.get_end_time()

        assert_that(self.time_range.contains(start_time))
        assert_that(not self.time_range.contains(end_time))
        assert_that(not self.time_range.contains(start_time - self.microsecond_delta))
        assert_that(self.time_range.contains(end_time - self.microsecond_delta))

    def test_string_representation(self):
        assert_that(str(self.time_range), is_(equal_to("['2012-03-02 09:50:14':'2012-03-02 11:20:24']")))


class TestEvent(unittest.TestCase):
    def test_time_difference(self):
        now = pd.Timestamp(datetime.datetime.utcnow(), tz=pytz.UTC)
        later = pd.Timestamp(np.datetime64(now.value + 100, 'ns'), tz=pytz.UTC)

        event1 = blitzortung.data.Event(now, 11, 49)
        event2 = blitzortung.data.Event(later, 11, 49)

        self.assertEqual(datetime.timedelta(), event1.difference_to(event2))
        self.assertEqual(datetime.timedelta(), event2.difference_to(event1))
        self.assertEqual(100, event1.ns_difference_to(event2))
        self.assertEqual(-100, event2.ns_difference_to(event1))

        even_later = pd.Timestamp(np.datetime64(now.value + 20150, 'ns'), tz=pytz.UTC)
        event3 = blitzortung.data.Event(even_later, 11, 49)
        self.assertEqual(datetime.timedelta(days=-1, seconds=86399, microseconds=999980), event1.difference_to(event3))
        self.assertEqual(datetime.timedelta(microseconds=20), event3.difference_to(event1))
        self.assertEqual(20150, event1.ns_difference_to(event3))
        self.assertEqual(-20150, event3.ns_difference_to(event1))

        much_later = pd.Timestamp(np.datetime64(now.value + 3000000200, 'ns'), tz=pytz.UTC)
        event4 = blitzortung.data.Event(much_later, 11, 49)
        self.assertEqual(datetime.timedelta(days=-1, seconds=86397, microseconds=0), event1.difference_to(event4))
        self.assertEqual(datetime.timedelta(seconds=3), event4.difference_to(event1))
        self.assertEqual(3000000200, event1.ns_difference_to(event4))
        self.assertEqual(-3000000200, event4.ns_difference_to(event1))


class TestStroke(unittest.TestCase):

    def setUp(self):
        self.timestamp = pd.Timestamp(pd.Timestamp('2013-09-28 23:23:38.123456').value + 789)
        self.stroke = blitzortung.data.Stroke(123, self.timestamp, 11.2, 49.3, 2500, 10.5, 5400, 11, [1, 5, 7, 15])

    def test_get_id(self):
        assert_that(self.stroke.get_id(), is_(equal_to(123)))

    def test_get_timestamp(self):
        assert_that(self.stroke.get_timestamp(), is_(equal_to(self.timestamp)))

    def test_get_location(self):
        location = self.stroke.get_location()
        assert_that(location.x_coord, is_(equal_to(11.2)))
        assert_that(location.y_coord, is_(equal_to(49.3)))

    def test_get_altitude(self):
        assert_that(self.stroke.get_altitude(), is_(equal_to(2500)))

    def test_get_amplitude(self):
        assert_that(self.stroke.get_amplitude(), is_(equal_to(10.5)))

    def test_get_lateral_error(self):
        assert_that(self.stroke.get_lateral_error(), is_(equal_to(5400)))

    def test_get_station_count(self):
        assert_that(self.stroke.get_station_count(), is_(equal_to(11)))

    def test_get_default_stations(self):
        self.stroke = blitzortung.data.Stroke(123, self.timestamp, 11.2, 49.3, 2500, 10.5, 5400, 11)
        assert_that(self.stroke.get_stations(), is_(equal_to([])))

    def test_with_stations(self):
        assert_that(self.stroke.get_stations(), is_(equal_to([1, 5, 7, 15])))

    def test_has_participant(self):
        assert_that(self.stroke.has_participant(5))

    def test_string_represenation(self):
        assert_that(str(self.stroke), is_(equal_to("2013-09-28 23:23:38.123456789 11.2000 49.3000 2500 10.5 5400 11")))
