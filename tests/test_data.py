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

import blitzortung
import blitzortung.data
from blitzortung.data import Timestamp, Timedelta, NaT


class TestTimestamp(object):
    def test_value(self):
        now = Timestamp()
        later = now + 1234567

        assert_that(later.value - now.value).is_equal_to(1234567)

    def test_difference(self):
        now = Timestamp()
        later = now + 1234567

        assert_that(later - now).is_equal_to(Timedelta(nanodelta=1234567))

    def test_from_nanoseconds(self):
        timestamp = Timestamp(1540935833552753700)

        print(timestamp)

        assert_that(timestamp.datetime).is_equal_to(
            datetime.datetime(2018, 10, 30, 21, 43, 53, 552753, datetime.timezone.utc))
        assert_that(timestamp.nanosecond).is_equal_to(700)


class TestTimedelta(object):
    def test_normalizing(self):
        assert_that(Timedelta(nanodelta=1500)).is_equal_to(
            Timedelta(datetime.timedelta(microseconds=1), 500))
        assert_that(Timedelta(nanodelta=500)).is_equal_to(
            Timedelta(nanodelta=500))
        assert_that(Timedelta(nanodelta=-500)).is_equal_to(
            Timedelta(datetime.timedelta(microseconds=-1), nanodelta=500))
        assert_that(Timedelta(nanodelta=-1500)).is_equal_to(
            Timedelta(datetime.timedelta(microseconds=-2), 500))


class EventBaseTest(object):
    def setup_method(self):
        self.not_a_time = NaT
        self.now_time = Timestamp()
        self.later_time = self.now_time + 100

    def assertTrue(self, value):
        assert_that(value).is_true()

    def assertFalse(self, value):
        assert_that(value).is_false()

    def assertEqual(self, value, other):
        assert_that(other).is_equal_to(value)


class TestEvent(EventBaseTest):
    def test_create_and_get_values(self):
        event = blitzortung.data.Event(self.now_time, 11, 49)
        assert_that(event.timestamp).is_equal_to(self.now_time)
        assert_that(event.x).is_equal_to(11)
        assert_that(event.y).is_equal_to(49)

    def test_time_comparison(self):
        event1 = blitzortung.data.Event(self.now_time, 11, 49)
        event2 = blitzortung.data.Event(self.later_time, 11, 49)

        self.assertTrue(event2 > event1)
        self.assertTrue(event2 >= event1)
        self.assertFalse(event2 > event2)
        self.assertFalse(event1 > event2)
        self.assertFalse(event1 >= event2)
        self.assertFalse(event1 >= event2)

        self.assertTrue(event1 < event2)
        self.assertTrue(event1 <= event2)
        self.assertFalse(event1 < event1)
        self.assertFalse(event2 < event1)
        self.assertFalse(event2 <= event1)
        self.assertFalse(event2 <= event1)

    def test_time_difference(self):
        event1 = blitzortung.data.Event(self.now_time, 11, 49)
        event2 = blitzortung.data.Event(self.later_time, 11, 49)

        self.assertEqual(Timedelta(nanodelta=100), event1.difference_to(event2))
        self.assertEqual(Timedelta(nanodelta=-100), event2.difference_to(event1))
        self.assertEqual(100, event1.ns_difference_to(event2))
        self.assertEqual(-100, event2.ns_difference_to(event1))

        even_later = self.now_time + 20150
        event3 = blitzortung.data.Event(even_later, 11, 49)
        self.assertEqual(Timedelta(nanodelta=20150), event1.difference_to(event3))
        self.assertEqual(Timedelta(nanodelta=-20150), event3.difference_to(event1))
        self.assertEqual(20150, event1.ns_difference_to(event3))
        self.assertEqual(-20150, event3.ns_difference_to(event1))

        much_later = self.now_time + 3000000200
        event4 = blitzortung.data.Event(much_later, 11, 49)
        self.assertEqual(Timedelta(nanodelta=3000000200), event1.difference_to(event4))
        self.assertEqual(Timedelta(nanodelta=-3000000200), event4.difference_to(event1))
        self.assertEqual(3000000200, event1.ns_difference_to(event4))
        self.assertEqual(-3000000200, event4.ns_difference_to(event1))

    def test_is_valid(self):
        event = blitzortung.data.Event(self.now_time, 1.0, 2.0)
        self.assertTrue(event.is_valid)

        event = blitzortung.data.Event(self.now_time, 0.0, 1.0)
        self.assertTrue(event.is_valid)

        event = blitzortung.data.Event(self.now_time, 1.0, 0.0)
        self.assertTrue(event.is_valid)

    def test_is_valid_returning_false_if_event_is_not_valid(self):
        event = blitzortung.data.Event(self.now_time, 0.0, 0.0)
        self.assertFalse(event.is_valid)

        event = blitzortung.data.Event(self.not_a_time, 1.0, 2.0)
        self.assertFalse(event.is_valid)

        event = blitzortung.data.Event(self.not_a_time, 0.0, 0.0)
        self.assertFalse(event.is_valid)


class TestStrike(object):
    def setup_method(self):
        self.timestamp = Timestamp('2013-09-28 23:23:38.123456', 789)
        self.strike = blitzortung.data.Strike(123, self.timestamp, 11.2, 49.3, 2500, 10.5, 5400, 11, [1, 5, 7, 15])

    def test_get_id(self):
        assert_that(self.strike.id).is_equal_to(123)

    def test_get_timestamp(self):
        assert_that(self.strike.timestamp).is_equal_to(self.timestamp)

    def test_get_location(self):
        assert_that(self.strike.x).is_equal_to(11.2)
        assert_that(self.strike.y).is_equal_to(49.3)

    def test_get_altitude(self):
        assert_that(self.strike.altitude).is_equal_to(2500)

    def test_get_amplitude(self):
        assert_that(self.strike.amplitude).is_equal_to(10.5)

    def test_get_lateral_error(self):
        assert_that(self.strike.lateral_error).is_equal_to(5400)

    def test_get_station_count(self):
        assert_that(self.strike.station_count).is_equal_to(11)

    def test_get_default_stations(self):
        self.strike = blitzortung.data.Strike(123, self.timestamp, 11.2, 49.3, 2500, 10.5, 5400, 11)
        assert_that(self.strike.stations).is_equal_to([])

    def test_with_stations(self):
        assert_that(self.strike.stations).is_equal_to([1, 5, 7, 15])

    def test_has_participant(self):
        assert_that(self.strike.has_participant(5))

    def test_string_represenation(self):
        assert_that(str(self.strike)).is_equal_to("2013-09-28 23:23:38.123456789 11.2000 49.3000 2500 10.5 5400 11")


class TestStation(object):
    def setup_method(self):
        self.timestamp = Timestamp('2013-09-28 23:23:38')
        self.station = blitzortung.data.Station(123, 45, '<name>', '<country>', 11.2, 49.4, self.timestamp,
                                                '<status>', '<board>')

    def test_online_str(self):
        self.station = blitzortung.data.Station(123, 45, '<name>', '<country>', 11.2, 49.4, None, '<status>', '<board>')
        assert_that(str(self.station)).is_equal_to(
            "*123/ 45 '<name>' '<country>' (11.2000, 49.4000)")

    def test_offline_str(self):
        assert_that(str(self.station)).is_equal_to(
            "-123/ 45 '<name>' '<country>' (11.2000, 49.4000) offline since 2013-09-28 23:23 UTC")

    def test_get_number(self):
        assert_that(self.station.number).is_equal_to(123)

    def test_get_user(self):
        assert_that(self.station.user).is_equal_to(45)

    def test_get_name(self):
        assert_that(self.station.name).is_equal_to('<name>')

    def test_get_country(self):
        assert_that(self.station.country).is_equal_to('<country>')

    def test_get_status(self):
        assert_that(self.station.status).is_equal_to('<status>')


class TestGridData(object):
    def setup_method(self):
        self.reference_time = datetime.datetime.utcnow()
        self.grid = blitzortung.geom.Grid(-5, 4, -3, 2, 0.5, 1.25)
        self.grid_data = blitzortung.data.GridData(self.grid)

    def test_get_grid(self):
        assert_that(self.grid_data.grid).is_equal_to(self.grid)

    def test_empty_raster(self):
        for x_index in range(0, self.grid.x_bin_count):
            for y_index in range(0, self.grid.y_bin_count):
                assert_that(self.grid_data.get(x_index, y_index)).is_none()

    def test_empty_raster_to_arcgrid(self):
        assert_that(self.grid_data.to_arcgrid()).is_equal_to("""NCOLS 18
NROWS 4
XLLCORNER -5.0000
YLLCORNER -3.0000
CELLSIZE 0.5000
NODATA_VALUE 0
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0""")

    def test_empty_raster_to_map(self):
        assert_that(self.grid_data.to_map()).is_equal_to("""--------------------
|                  |
|                  |
|                  |
|                  |
--------------------
total count: 0, max per area: 0""")

    def test_empty_raster_to_reduced_array(self):
        assert_that(self.grid_data.to_reduced_array(self.reference_time)).is_equal_to(())

    def add_raster_data(self):
        self.grid_data.set(0, 0, blitzortung.geom.GridElement(5, self.reference_time - datetime.timedelta(minutes=2)))
        self.grid_data.set(1, 1, blitzortung.geom.GridElement(10, self.reference_time - datetime.timedelta(seconds=10)))
        self.grid_data.set(4, 2, blitzortung.geom.GridElement(20, self.reference_time - datetime.timedelta(hours=1)))

    def test_raster_to_arcgrid(self):
        self.add_raster_data()
        assert_that(self.grid_data.to_arcgrid()).is_equal_to("""NCOLS 18
NROWS 4
XLLCORNER -5.0000
YLLCORNER -3.0000
CELLSIZE 0.5000
NODATA_VALUE 0
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 20 0 0 0 0 0 0 0 0 0 0 0 0 0
0 10 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
5 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0""")

    def test_raster_to_map(self):
        self.add_raster_data()
        assert_that(self.grid_data.to_map()).is_equal_to("""--------------------
|                  |
|    8             |
| o                |
|-                 |
--------------------
total count: 35, max per area: 20""")

    def test_raster_to_reduced_array(self):
        self.add_raster_data()
        assert_that(self.grid_data.to_reduced_array(self.reference_time)).is_equal_to(
            ((4, 1, 20, -3600), (1, 2, 10, -10), (0, 3, 5, -120))
        )

    def test_raster_set_outside_valid_index_value_does_not_throw_exception(self):
        self.grid_data.set(1000, 0, blitzortung.geom.GridElement(20, self.reference_time - datetime.timedelta(hours=1)))
        assert_that(self.grid_data.to_reduced_array(self.reference_time)).is_equal_to(())
