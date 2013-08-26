# -*- coding: utf8 -*-

import unittest
import datetime
from hamcrest import assert_that, is_, equal_to, none
import nose
import pytz
import numpy as np
import pandas as pd

import blitzortung


class TestBase(unittest.TestCase):
    def get_timestamp(self, time_string):
        return pd.Timestamp(np.datetime64(time_string), tz=pytz.UTC)


class TimestampTest(unittest.TestCase):
    def setUp(self):
        self.builder = blitzortung.builder.Timestamp()

    def test_initial_value(self):
        assert_that(self.builder.build(), is_(none()))

    def test_set_timestamp_from_none_value(self):
        self.builder.set_timestamp(None)
        assert_that(self.builder.build(), is_(none()))

    def test_set_timestamp_from_datetime(self):
        timestamp = self.builder.set_timestamp(datetime.datetime(2012, 2, 10, 12, 56, 18, 96651)).build()

        self.assert_timestamp(timestamp)

    def test_set_timestamp_from_pandas_timestamp(self):
        timestamp = pd.Timestamp(datetime.datetime(2012, 2, 10, 12, 56, 18, 96651))
        timestamp = pd.Timestamp(timestamp.value + 423)

        self.builder.set_timestamp(timestamp)

        timestamp = self.builder.build()
        self.assert_timestamp(timestamp)
        assert_that(timestamp.nanosecond, is_(equal_to(423)))

    def test_set_timestamp_from_pandas_timestamp_with_ns_offset(self):
        timestamp = pd.Timestamp(datetime.datetime(2012, 2, 10, 12, 56, 18, 96651), tz='CET')

        self.builder.set_timestamp(timestamp, 423)

        timestamp = self.builder.build()
        self.assert_timestamp(timestamp)

        assert_that(timestamp.tzinfo, is_(equal_to(pytz.timezone('CET'))))
        assert_that(timestamp.nanosecond, is_(equal_to(423)))

    def test_set_timestamp_from_bad_string(self):
        raise nose.SkipTest("fix pandas timestamp behavior first")
        timestamp = self.builder.set_timestamp('0000-00-00').build()
        assert_that(timestamp, is_(pd.Timestamp.NaT))

    def test_set_timestamp_from_string(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.096651423").build()

        self.assert_timestamp(timestamp)
        assert_that(timestamp.nanosecond, is_(equal_to(423)))
        assert_that(timestamp.tzinfo, is_(equal_to(pytz.UTC)))

    def test_set_timestamp_from_string(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.096651423")

        timestamp = self.builder.build()
        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(96651)))
        assert_that(timestamp.nanosecond, is_(equal_to(423)))

    def test_set_timestamp_from_millisecond_string(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.096").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(96000)))
        assert_that(timestamp.nanosecond, is_(equal_to(0)))

    def test_create_from_string_wihtout_fractional_seconds(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(0)))
        assert_that(timestamp.nanosecond, is_(equal_to(0)))

    def test_create_from_nanosecond_string(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.123456789").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(123456)))
        assert_that(timestamp.nanosecond, is_(equal_to(789)))

        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.12345678").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(123456)))
        assert_that(timestamp.nanosecond, is_(equal_to(780)))

        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.1234567").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(123456)))
        assert_that(timestamp.nanosecond, is_(equal_to(700)))

    def assert_timestamp_base(self, timestamp):
        assert_that(timestamp.day, is_(equal_to(10)))
        assert_that(timestamp.month, is_(equal_to(2)))
        assert_that(timestamp.year, is_(equal_to(2012)))
        assert_that(timestamp.hour, is_(equal_to(12)))
        assert_that(timestamp.minute, is_(equal_to(56)))
        assert_that(timestamp.second, is_(equal_to(18)))

    def assert_timestamp(self, timestamp):
        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(96651)))


class StrokeTest(TestBase):
    def setUp(self):
        self.builder = blitzortung.builder.Stroke()

    def test_default_values(self):
        assert_that(self.builder.id_value, is_(equal_to(-1)))
        assert_that(self.builder.altitude, is_(none()))
        assert_that(self.builder.stations, is_(equal_to([])))

    def test_set_id(self):
        self.builder.set_id(1234)
        assert_that(self.builder.id_value, 1234)

        self.builder.set_x(0.0)
        self.builder.set_y(0.0)
        self.builder.set_timestamp(datetime.datetime.utcnow())
        self.builder.set_amplitude(1.0)
        self.builder.set_lateral_error(5.0)
        self.builder.set_type(-1)
        self.builder.set_station_count(10)

        assert_that(self.builder.build().get_id(), is_(equal_to(1234)))

    def test_set_timestamp(self):
        timestamp = datetime.datetime.utcnow()
        self.builder.set_timestamp(timestamp)
        assert_that(self.builder.timestamp, is_(equal_to(timestamp)))

        self.builder.set_x(0.0)
        self.builder.set_y(0.0)
        self.builder.set_amplitude(1.0)
        self.builder.set_lateral_error(5.0)
        self.builder.set_type(-1)
        self.builder.set_station_count(10)

        assert_that(self.builder.build().get_timestamp(), is_(equal_to(pd.Timestamp(timestamp))))

    def test_build_stroke_from_data(self):
        stroke_data = {u'sta': [u'10', u'24',
                                u'226,529,391,233,145,398,425,533,701,336,336,515,434,392,439,283,674,573,559,364,111,43,582,594'],
                       u'pos': [u'44.162701', u'8.931001', u'0'], u'dev': u'20146', u'str': u'4.75',
                       'time': u'10:30:03.644038642', 'date': u'2013-08-08', u'typ': u'0'}

        stroke = self.builder.from_data(stroke_data).build()

        assert_that(stroke.get_timestamp(), is_(equal_to(self.get_timestamp("2013-08-08 10:30:03.644038642Z"))))
        assert_that(stroke.get_x(), is_(equal_to(8.931001)))
        assert_that(stroke.get_y(), is_(equal_to(44.162701)))
        assert_that(stroke.get_amplitude(), is_(equal_to(4.75)))
        assert_that(stroke.get_type(), is_(equal_to(0)))
        assert_that(stroke.get_lateral_error(), is_(equal_to(20146)))
        assert_that(stroke.get_station_count(), is_(equal_to(10)))
        assert_that(stroke.get_stations(), is_(equal_to(
                         [226, 529, 391, 233, 145, 398, 425, 533, 701, 336, 336, 515, 434, 392, 439, 283, 674, 573, 559,
                          364, 111, 43, 582, 594])))


class StationTest(TestBase):
    def setUp(self):
        self.builder = blitzortung.builder.Station()

    def test_default_values(self):
        assert_that(self.builder.number, is_(equal_to(-1)))
        assert_that(self.builder.user, is_(equal_to(-1)))
        assert_that(self.builder.name, is_(none()))
        assert_that(self.builder.status, is_(none()))
        assert_that(self.builder.board, is_(none()))

    def test_build_station_from_data(self):
        data = {u'status': u'0', u'city': u'Musterdörfl', u'last_signal': u'2012-02-10 14:39:47.410492569',
                u'last_stroke': u'0000-00-00',
                u'input_firmware': [u'1.9_/_May_13_201', u'1.9_/_May_13_201', u'', u'', u'', u''],
                u'distance': u'71.474273030335',
                u'strokes': [u'0', u'0', u'0', u'0', u'0', u'0', u'0', u'5879', u'0'], u'country': u'Germany',
                u'firmware': u'STM32F4', u'myblitz': u'N', u'pos': [u'49.5435', u'9.7314', u'130'],
                u'input_board': [u'5.7', u'5.7', u'', u'', u'', u''], u'signals': u'100', u'station': u'364',
                u'input_gain': [u'7.7', u'7.7', u'7.7', u'7.7', u'7.7', u'7.7'], u'board': u'6.8',
                u'input_antenna': [u'', u'', u'', u'', u'', u''], u'user': u'1'}

        station = self.builder.from_data(data).build()

        assert_that(station.get_number(), is_(equal_to(364)))
        assert_that(station.get_user(), is_(equal_to(1)))
        assert_that(station.get_name(), is_(equal_to(u'Musterdörfl')))
        assert_that(station.get_country(), is_(equal_to('Germany')))
        assert_that(station.get_x(), is_(equal_to(9.7314)))
        assert_that(station.get_y(), is_(equal_to(49.5435)))
        assert_that(station.get_timestamp(), is_(equal_to(self.get_timestamp("2012-02-10T14:39:47.410492569Z"))))
        assert_that(station.get_board(), is_(equal_to(u'6.8')))


    def test_build_station_offline(self):
        self.builder.set_number(364)
        self.builder.set_user(10)
        self.builder.set_name(u'Musterdörfl')
        self.builder.set_country(u'Germany')
        self.builder.set_x(9.7314)
        self.builder.set_y(49.5435)
        self.builder.set_timestamp("2012-02-10 14:39:47.410492123")
        self.builder.set_status('A')
        self.builder.set_board('0815')

        station = self.builder.build()

        assert_that(station.get_number(), is_(equal_to(364)))
        assert_that(station.get_name(), is_(equal_to(u'Musterdörfl')))
        assert_that(station.get_country(), is_(equal_to(u'Germany')))
        assert_that(station.get_x(), is_(equal_to(9.7314)))
        assert_that(station.get_y(), is_(equal_to(49.5435)))
        assert_that(station.get_timestamp(), is_(equal_to(self.get_timestamp("2012-02-10T14:39:47.410492123Z"))))
        assert_that(station.get_status(), is_(equal_to('A')))
        assert_that(station.get_board(), is_(equal_to('0815')))


class StationOffline(unittest.TestCase):
    def setUp(self):
        self.builder = blitzortung.builder.StationOffline()

    def test_default_values(self):
        assert_that(self.builder.id_value, is_(equal_to(-1)))
        assert_that(self.builder.number, is_(equal_to(-1)))
        assert_that(self.builder.begin, is_(none()))
        assert_that(self.builder.end, is_(none()))

    def test_build_station_offline(self):
        self.builder.set_id(364)
        self.builder.set_number(123)

        end = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
        begin = end - datetime.timedelta(hours=1)
        self.builder.set_begin(begin)
        self.builder.set_end(end)

        station_offline = self.builder.build()

        assert_that(station_offline.get_id(), is_(equal_to(364)))
        assert_that(station_offline.get_number(), is_(equal_to(123)))
        assert_that(station_offline.get_begin(), is_(equal_to(begin)))
        assert_that(station_offline.get_end(), is_(equal_to(end)))


class RawEvent(unittest.TestCase):
    def setUp(self):
        self.builder = blitzortung.builder.RawEvent()

    def test_default_values(self):
        assert_that(self.builder.x_coord, is_(equal_to(0)))
        assert_that(self.builder.y_coord, is_(equal_to(0)))
        assert_that(self.builder.timestamp, is_(none()))
        assert_that(self.builder.altitude, is_(equal_to(0)))
        assert_that(self.builder.amplitude, is_(equal_to(0.0)))
        assert_that(self.builder.angle, is_(equal_to(0.0)))


    def test_build_raw_event(self):
        self.builder.set_x(11.0)
        self.builder.set_y(49.0)
        self.builder.set_altitude(530)

        raw_event = self.builder.build()

        assert_that(raw_event.get_x(), is_(equal_to(11.0)))
        assert_that(raw_event.get_y(), is_(equal_to(49.0)))
        assert_that(raw_event.get_altitude(), is_(equal_to(530)))


class ExtEvent(unittest.TestCase):
    def setUp(self):
        self.builder = blitzortung.builder.ExtEvent()

    def test_default_values(self):
        assert_that(self.builder.x_coord, is_(equal_to(0)))
        assert_that(self.builder.y_coord, is_(equal_to(0)))
        assert_that(self.builder.timestamp, is_(none()))
        assert_that(self.builder.altitude, is_(equal_to(0)))
        assert_that(self.builder.amplitude, is_(equal_to(0.0)))
        assert_that(self.builder.angle, is_(equal_to(0.0)))
        assert_that(self.builder.station_number, is_(equal_to(0)))

    def test_build_ext_event(self):
        self.builder.set_x(11.0)
        self.builder.set_y(49.0)
        self.builder.set_altitude(530)
        self.builder.set_station_number(39)

        ext_event = self.builder.build()

        assert_that(ext_event.get_x(), is_(equal_to(11.0)))
        assert_that(ext_event.get_y(), is_(equal_to(49.0)))
        assert_that(ext_event.get_altitude(), is_(equal_to(530)))
        assert_that(ext_event.get_station_number(), is_(equal_to(39)))

