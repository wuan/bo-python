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

import blitzortung.builder
from blitzortung.data import Timestamp


class TestBase:
    @staticmethod
    def get_timestamp(timestamp_string):
        return datetime.datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S.%f').replace(
            tzinfo=datetime.timezone.utc)


class TestTimestamp:
    def setup_class(self):
        self.builder = blitzortung.builder.Timestamp()

    def test_initial_value(self):
        assert_that(self.builder.build()).is_none()

    def test_set_timestamp_from_none_value(self):
        self.builder.set_timestamp(None)
        assert_that(self.builder.build()).is_none()

    def test_set_timestamp_from_datetime(self):
        timestamp = self.builder.set_timestamp(datetime.datetime(2012, 2, 10, 12, 56, 18, 96651)).build()

        self.assert_timestamp(timestamp)

    def test_value_property(self):
        timestamp = self.builder.set_timestamp(datetime.datetime(2012, 2, 10, 12, 56, 18, 96651)).build()

        value = timestamp.value
        assert_that(value).is_equal_to(1328874978096651000)

    def test_value_property_with_nanoseconds(self):
        timestamp = self.builder.set_timestamp(datetime.datetime(2012, 2, 10, 12, 56, 18, 96651), 456).build()

        value = timestamp.value
        assert_that(value).is_equal_to(1328874978096651456)

    def test_value_property(self):
        timestamp = self.builder.set_timestamp(
            datetime.datetime(2012, 2, 10, 12, 56, 18, 96651, tzinfo=datetime.timezone.utc)).build()

        value = timestamp.value
        assert_that(value).is_equal_to(1328878578096651000)

    def test_value_property_with_nanoseconds(self):
        timestamp = self.builder.set_timestamp(
            datetime.datetime(2012, 2, 10, 12, 56, 18, 96651, tzinfo=datetime.timezone.utc),
            456).build()

        value = timestamp.value
        assert_that(value).is_equal_to(1328878578096651456)

    def test_set_timestamp_from_bad_string(self):
        timestamp = self.builder.set_timestamp('0000-00-00').build()
        assert_that(timestamp.datetime).is_none()

    def test_set_timestamp_from_string(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.096651423").build()

        self.assert_timestamp(timestamp)
        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond).is_equal_to(96651)
        assert_that(timestamp.nanosecond).is_equal_to(423)
        assert_that(timestamp.tzinfo).is_equal_to(datetime.timezone.utc)

    def test_set_timestamp_from_millisecond_string(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.096").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond).is_equal_to(96000)
        assert_that(timestamp.nanosecond).is_equal_to(0)

    def test_create_from_string_wihtout_fractional_seconds(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.datetime.microsecond).is_equal_to(0)
        assert_that(timestamp.nanosecond).is_equal_to(0)

    def test_create_from_nanosecond_string(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.123456789").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.datetime.microsecond).is_equal_to(123456)
        assert_that(timestamp.nanosecond).is_equal_to(789)

        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.12345678").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond).is_equal_to(123456)
        assert_that(timestamp.nanosecond).is_equal_to(780)

        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.1234567").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond).is_equal_to(123456)
        assert_that(timestamp.nanosecond).is_equal_to(700)

    def assert_timestamp_base(self, timestamp):
        assert_that(timestamp.day).is_equal_to(10)
        assert_that(timestamp.month).is_equal_to(2)
        assert_that(timestamp.year).is_equal_to(2012)
        assert_that(timestamp.hour).is_equal_to(12)
        assert_that(timestamp.minute).is_equal_to(56)
        assert_that(timestamp.second).is_equal_to(18)

    def assert_timestamp(self, timestamp):
        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.datetime.microsecond).is_equal_to(96651)


class TestStrike(TestBase):
    def setup_class(self):
        self.builder = blitzortung.builder.Strike()

    def test_default_values(self):
        assert_that(self.builder.id_value).is_equal_to(-1)
        assert_that(self.builder.altitude).is_none()
        assert_that(self.builder.stations).is_empty()

    def test_set_id(self):
        self.builder.set_id(1234)
        assert_that(self.builder.id_value).is_equal_to(1234)

        self.builder.set_x(0.0)
        self.builder.set_y(0.0)
        self.builder.set_timestamp(datetime.datetime.now(datetime.timezone.utc))
        self.builder.set_amplitude(1.0)
        self.builder.set_lateral_error(5.0)
        self.builder.set_station_count(10)

        assert_that(self.builder.build().id).is_equal_to(1234)

    def test_set_timestamp(self):
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        self.builder.set_timestamp(timestamp)

        self.builder.set_x(0.0)
        self.builder.set_y(0.0)
        self.builder.set_amplitude(1.0)
        self.builder.set_altitude(0)
        self.builder.set_lateral_error(5.0)
        self.builder.set_station_count(10)

        strike = self.builder.build()
        assert_that(strike.timestamp.datetime).is_equal_to(timestamp)
        assert_that(strike.timestamp.nanosecond).is_equal_to(0)

    def test_set_lateral_error_lower_limit(self):
        self.builder.set_lateral_error(-1)
        assert_that(self.builder.build().lateral_error).is_equal_to(0)
        self.builder.set_lateral_error(0)
        assert_that(self.builder.build().lateral_error).is_equal_to(0)

    def test_set_lateral_error_upper_limit(self):
        self.builder.set_lateral_error(32767)
        assert_that(self.builder.build().lateral_error).is_equal_to(32767)
        self.builder.set_lateral_error(32768)
        assert_that(self.builder.build().lateral_error).is_equal_to(32767)

    def test_build_strike_from_line(self):
        strike_line = u"2013-08-08 10:30:03.644038642 pos;44.162701;8.931001;0 str;4.75 typ;0 dev;20146 sta;10;24;226,529,391,233,145,398,425,533,701,336,336,515,434,392,439,283,674,573,559,364,111,43,582,594"
        strike = self.builder.from_line(strike_line).build()

        assert_that(strike.timestamp).is_equal_to(Timestamp("2013-08-08 10:30:03.644038642"))
        assert_that(strike.timestamp.nanosecond).is_equal_to(642)
        assert_that(strike.x).is_equal_to(8.931001)
        assert_that(strike.y).is_equal_to(44.162701)
        assert_that(strike.altitude).is_equal_to(0)
        assert_that(strike.amplitude).is_equal_to(4.75)
        assert_that(strike.lateral_error).is_equal_to(20146)
        assert_that(strike.station_count).is_equal_to(10)
        assert_that(strike.stations).is_equal_to(
            [226, 529, 391, 233, 145, 398, 425, 533, 701, 336, 336, 515, 434, 392, 439, 283, 674, 573, 559,
             364, 111, 43, 582, 594])

    def test_build_strike_from_bad_line(self):
        strike_line = u"2013-08-08 10:30:03.644038642"
        with pytest.raises(blitzortung.builder.BuilderError):
            self.builder.from_line(strike_line)
