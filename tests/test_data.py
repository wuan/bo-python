import unittest
import datetime
from hamcrest import assert_that, is_, equal_to
import numpy as np
import pandas as pd
import pytz

import blitzortung
import blitzortung.data


class EventBaseTest(unittest.TestCase):
    def setUp(self):
        self.not_a_time = pd.NaT
        self.now_time = pd.Timestamp(datetime.datetime.utcnow(), tz=pytz.UTC)
        self.later_time = pd.Timestamp(np.datetime64(self.now_time.value + 100, 'ns'), tz=pytz.UTC)


class TestEvent(EventBaseTest):
    def test_create_and_get_values(self):
        event = blitzortung.data.Event(self.now_time, 11, 49)
        assert_that(event.get_timestamp(), is_(self.now_time))
        assert_that(event.get_x(), is_(11))
        assert_that(event.get_y(), is_(49))

    def test_time_comparison(self):
        event1 = blitzortung.data.Event(self.now_time, 11, 49)
        event2 = blitzortung.data.Event(self.later_time, 11, 49)

        self.assertTrue(event2 > event1)
        self.assertTrue(event2 >= event1)
        self.assertTrue(event2 >= event2)
        self.assertFalse(event1 > event2)
        self.assertFalse(event1 >= event2)
        self.assertFalse(event1 >= event2)

        self.assertTrue(event1 < event2)
        self.assertTrue(event1 <= event2)
        self.assertTrue(event1 <= event1)
        self.assertFalse(event2 < event1)
        self.assertFalse(event2 <= event1)
        self.assertFalse(event2 <= event1)

    def test_time_difference(self):
        event1 = blitzortung.data.Event(self.now_time, 11, 49)
        event2 = blitzortung.data.Event(self.later_time, 11, 49)

        self.assertEqual(datetime.timedelta(), event1.difference_to(event2))
        self.assertEqual(datetime.timedelta(), event2.difference_to(event1))
        self.assertEqual(100, event1.ns_difference_to(event2))
        self.assertEqual(-100, event2.ns_difference_to(event1))

        even_later = pd.Timestamp(np.datetime64(self.now_time.value + 20150, 'ns'), tz=pytz.UTC)
        event3 = blitzortung.data.Event(even_later, 11, 49)
        self.assertEqual(datetime.timedelta(days=-1, seconds=86399, microseconds=999980), event1.difference_to(event3))
        self.assertEqual(datetime.timedelta(microseconds=20), event3.difference_to(event1))
        self.assertEqual(20150, event1.ns_difference_to(event3))
        self.assertEqual(-20150, event3.ns_difference_to(event1))

        much_later = pd.Timestamp(np.datetime64(self.now_time.value + 3000000200, 'ns'), tz=pytz.UTC)
        event4 = blitzortung.data.Event(much_later, 11, 49)
        self.assertEqual(datetime.timedelta(days=-1, seconds=86397, microseconds=0), event1.difference_to(event4))
        self.assertEqual(datetime.timedelta(seconds=3), event4.difference_to(event1))
        self.assertEqual(3000000200, event1.ns_difference_to(event4))
        self.assertEqual(-3000000200, event4.ns_difference_to(event1))

    def test_is_valid(self):
        event = blitzortung.data.Event(self.now_time, 1.0, 2.0)
        self.assertTrue(event.is_valid())

        event = blitzortung.data.Event(self.now_time, 0.0, 1.0)
        self.assertTrue(event.is_valid())

        event = blitzortung.data.Event(self.now_time, 1.0, 0.0)
        self.assertTrue(event.is_valid())

    def test_is_valid_returning_false_if_event_is_not_valid(self):
        event = blitzortung.data.Event(self.now_time, 0.0, 0.0)
        self.assertFalse(event.is_valid())

        event = blitzortung.data.Event(self.not_a_time, 1.0, 2.0)
        self.assertFalse(event.is_valid())

        event = blitzortung.data.Event(self.not_a_time, 0.0, 0.0)
        self.assertFalse(event.is_valid())


class TestStrike(unittest.TestCase):
    def setUp(self):
        self.timestamp = pd.Timestamp(pd.Timestamp('2013-09-28 23:23:38.123456').value + 789)
        self.strike = blitzortung.data.Strike(123, self.timestamp, 11.2, 49.3, 2500, 10.5, 5400, 11, [1, 5, 7, 15])

    def test_get_id(self):
        assert_that(self.strike.get_id(), is_(equal_to(123)))

    def test_get_timestamp(self):
        assert_that(self.strike.get_timestamp(), is_(equal_to(self.timestamp)))

    def test_get_location(self):
        location = self.strike.get_location()
        assert_that(location.x_coord, is_(equal_to(11.2)))
        assert_that(location.y_coord, is_(equal_to(49.3)))

    def test_get_altitude(self):
        assert_that(self.strike.get_altitude(), is_(equal_to(2500)))

    def test_get_amplitude(self):
        assert_that(self.strike.get_amplitude(), is_(equal_to(10.5)))

    def test_get_lateral_error(self):
        assert_that(self.strike.get_lateral_error(), is_(equal_to(5400)))

    def test_get_station_count(self):
        assert_that(self.strike.get_station_count(), is_(equal_to(11)))

    def test_get_default_stations(self):
        self.strike = blitzortung.data.Strike(123, self.timestamp, 11.2, 49.3, 2500, 10.5, 5400, 11)
        assert_that(self.strike.get_stations(), is_(equal_to([])))

    def test_with_stations(self):
        assert_that(self.strike.get_stations(), is_(equal_to([1, 5, 7, 15])))

    def test_has_participant(self):
        assert_that(self.strike.has_participant(5))

    def test_string_represenation(self):
        assert_that(str(self.strike), is_(equal_to("2013-09-28 23:23:38.123456789 11.2000 49.3000 2500 10.5 5400 11")))


class TestStation(unittest.TestCase):
    def setUp(self):
        self.timestamp = pd.Timestamp(pd.Timestamp('2013-09-28 23:23:38').value)
        self.station = blitzortung.data.Station(123, 45, '<name>', '<country>', 11.2, 49.4, self.timestamp,
                                                '<status>', '<board>')

    def test_str(self):
        assert_that(str(self.station),
                    is_("123/ 45 '<name>' '<country>' 2013-09-28 23:23:38.000000000 11.2000 49.4000"))

    def test_get_number(self):
        assert_that(self.station.get_number(), is_(123))

    def test_get_user(self):
        assert_that(self.station.get_user(), is_(45))

    def test_get_name(self):
        assert_that(self.station.get_name(), is_('<name>'))

    def test_get_country(self):
        assert_that(self.station.get_country(), is_('<country>'))

    def test_get_status(self):
        assert_that(self.station.get_country(), is_('<country>'))
