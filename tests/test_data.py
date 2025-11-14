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

import pytest
from assertpy import assert_that
from mock import Mock

import blitzortung
import blitzortung.data
from blitzortung.data import Timestamp, Timedelta, NaT


class TestTimestamp:
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

    def test_comparison_with_datetime(self):
        ts = Timestamp(datetime.datetime(2020, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc))
        dt_before = datetime.datetime(2019, 12, 31, 12, 0, 0, tzinfo=datetime.timezone.utc)
        dt_after = datetime.datetime(2020, 1, 2, 12, 0, 0, tzinfo=datetime.timezone.utc)

        # Test __lt__ with datetime
        assert_that(ts < dt_after).is_true()
        assert_that(ts < dt_before).is_false()

        # Test __le__ with datetime
        assert_that(ts <= dt_after).is_true()
        assert_that(ts <= dt_before).is_false()

        # Test __gt__ with datetime
        assert_that(ts > dt_before).is_true()
        assert_that(ts > dt_after).is_false()

        # Test __ge__ with datetime
        assert_that(ts >= dt_before).is_true()
        assert_that(ts >= dt_after).is_false()

    def test_addition_operations(self):
        ts = Timestamp(datetime.datetime(2020, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc))

        # Test adding Timedelta
        td = Timedelta(datetime.timedelta(hours=1), 500)
        result = ts + td
        assert_that(result.datetime).is_equal_to(datetime.datetime(2020, 1, 1, 13, 0, 0, tzinfo=datetime.timezone.utc))
        assert_that(result.nanosecond).is_equal_to(500)

        # Test adding datetime.timedelta
        delta = datetime.timedelta(hours=2)
        result = ts + delta
        assert_that(result.datetime).is_equal_to(datetime.datetime(2020, 1, 1, 14, 0, 0, tzinfo=datetime.timezone.utc))

        # Test adding int (nanoseconds)
        result = ts + 500
        assert_that(result.nanosecond).is_equal_to(500)

    def test_subtraction_operations(self):
        ts = Timestamp(datetime.datetime(2020, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc))

        # Test subtracting datetime.timedelta
        delta = datetime.timedelta(hours=2)
        result = ts - delta
        assert_that(result.datetime).is_equal_to(datetime.datetime(2020, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc))

        # Test subtracting int (nanoseconds)
        ts_with_ns = Timestamp(datetime.datetime(2020, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc), 500)
        result = ts_with_ns - 200
        assert_that(result.nanosecond).is_equal_to(300)

    def test_replace_with_nanosecond(self):
        ts = Timestamp(datetime.datetime(2020, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc), 100)
        result = ts.replace(nanosecond=500)
        assert_that(result.nanosecond).is_equal_to(500)
        assert_that(result.datetime).is_equal_to(ts.datetime)


class TestTimedelta:
    def test_normalizing(self):
        assert_that(Timedelta(nanodelta=1500)).is_equal_to(
            Timedelta(datetime.timedelta(microseconds=1), 500))
        assert_that(Timedelta(nanodelta=500)).is_equal_to(
            Timedelta(nanodelta=500))
        assert_that(Timedelta(nanodelta=-500)).is_equal_to(
            Timedelta(datetime.timedelta(microseconds=-1), nanodelta=500))
        assert_that(Timedelta(nanodelta=-1500)).is_equal_to(
            Timedelta(datetime.timedelta(microseconds=-2), 500))

    def test_properties(self):
        td = Timedelta(datetime.timedelta(days=5, seconds=3661), 500)
        assert_that(td.days).is_equal_to(5)
        assert_that(td.seconds).is_equal_to(3661)

    def test_repr(self):
        td = Timedelta(datetime.timedelta(hours=2), 500)
        result = repr(td)
        assert_that(result).contains("Timedelta")
        assert_that(result).contains("500")


class EventBaseTest:
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

    def test_has_same_location(self):
        event1 = blitzortung.data.Event(self.now_time, 11.0, 49.0)
        event2 = blitzortung.data.Event(self.later_time, 11.0, 49.0)
        event3 = blitzortung.data.Event(self.now_time, 12.0, 49.0)

        self.assertTrue(event1.has_same_location(event2))
        self.assertFalse(event1.has_same_location(event3))

    def test_string_with_nat(self):
        event = blitzortung.data.Event(self.not_a_time, 11.0, 49.0)
        result = str(event)
        assert_that(result).contains("NaT")


class TestStrike:
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


class TestGridData(object):
    def setup_method(self):
        self.reference_time = datetime.datetime.now(datetime.timezone.utc)
        self.grid = blitzortung.geom.Grid(-5, 4, -3, 2, 0.5, 1.25, 5000)
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

    @pytest.mark.parametrize(["count", "divider", "expected"], [
        (5, 5, 1),
        (6, 5, 2),
        (10, 5, 2),
        (11, 5, 3),
    ])
    def test_cell_index(self, count, divider, expected):
        cell = Mock()
        cell.count = count
        result = blitzortung.data.GridData.cell_index(cell, divider)

        assert_that(result).is_equal_to(expected)

    def test_empty_cell_index(self):
        result = blitzortung.data.GridData.cell_index(None, 5)

        assert_that(result).is_equal_to(0)

    def test_max_and_total(self):
        cell1 = Mock()
        cell1.count = 4
        cell2 = Mock()
        cell2.count = 8
        cell4 = Mock()
        cell4.count = 6

        maximum, total = blitzortung.data.GridData.max_and_total_entries([[cell1, cell2],[None, cell4]])

        assert_that(maximum).is_equal_to(8)
        assert_that(total).is_equal_to(18)

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


class TestChannelWaveform:
    def test_init(self):
        waveform_data = [1, 2, 3, 4, 5]
        waveform = blitzortung.data.ChannelWaveform(
            channel_number=1,
            amplifier_version=2,
            antenna=3,
            gain=4.5,
            values=100,
            start=0,
            bits=12,
            shift=0,
            conversion_gap=10,
            conversion_time=5,
            waveform=waveform_data
        )

        assert_that(waveform.channel_number).is_equal_to(1)
        assert_that(waveform.amplifier_version).is_equal_to(2)
        assert_that(waveform.antenna).is_equal_to(3)
        assert_that(waveform.gain).is_equal_to(4.5)
        assert_that(waveform.values).is_equal_to(100)
        assert_that(waveform.start).is_equal_to(0)
        assert_that(waveform.bits).is_equal_to(12)
        assert_that(waveform.shift).is_equal_to(0)
        assert_that(waveform.conversion_gap).is_equal_to(10)
        assert_that(waveform.conversion_time).is_equal_to(5)
        assert_that(waveform.waveform).is_equal_to(waveform_data)
