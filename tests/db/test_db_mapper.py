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
from unittest import TestCase
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import shapely.geometry
import shapely.wkb
from assertpy import assert_that
from mock import Mock, call

import blitzortung.builder
import blitzortung.db.mapper


class TestStrikeMapper(TestCase):
    def setUp(self):
        self.strike_builder = Mock(name="strike_builder", spec=blitzortung.builder.Strike)
        self.strike_mapper = blitzortung.db.mapper.Strike(self.strike_builder)

        self.timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        self.result = {
            'id': 12,
            'timestamp': self.timestamp,
            'nanoseconds': 789,
            'x': 11.0,
            'y': 51.0,
            'altitude': 123,
            'amplitude': 21323,
            'stationcount': 12,
            'error2d': 5000
        }
        self.strike = Mock(name="strike")
        self.strike_builder.build.return_value = self.strike

    def test_strike_mapper(self):
        assert_that(self.strike_mapper.create_object(self.result)).is_equal_to(self.strike)

        assert_that(self.strike_builder.method_calls).is_equal_to([
            call.set_id(12),
            call.set_timestamp(self.timestamp, 789),
            call.set_x(11.0),
            call.set_y(51.0),
            call.set_altitude(123),
            call.set_amplitude(21323),
            call.set_station_count(12),
            call.set_lateral_error(5000),
            call.build()
        ])

    def test_strike_mapper_with_timezone(self):
        zone = ZoneInfo('CET')

        self.strike_mapper.create_object(self.result, timezone=zone)

        timestamp = self.strike_builder.set_timestamp.call_args[0][0]

        assert_that(timestamp).is_equal_to(self.timestamp)
        assert_that(timestamp.tzinfo).is_equal_to(zone)

    def test_strike_mapper_without_timestamp(self):
        self.result['timestamp'] = None

        self.strike_mapper.create_object(self.result)

        assert_that(self.strike_builder.set_timestamp.call_args[0][0]).is_none()


class TestStationMapper(TestCase):
    def setUp(self):
        self.station_builder = Mock(name="station_builder", spec=blitzortung.builder.Station)
        self.strike_mapper = blitzortung.db.mapper.Station(self.station_builder)

        self.timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        self.result = {
            'number': 31,
            'user': '<user>',
            'name': '<name>',
            'country': '<country>',
            'geog': shapely.wkb.dumps(shapely.geometry.Point(11, 49), hex=True),
            'begin': self.timestamp
        }

        self.station = Mock(name="station")
        self.station_builder.build.return_value = self.station

    def test_station_mapper(self):
        assert_that(self.strike_mapper.create_object(self.result)).is_equal_to(self.station)

        assert_that(self.station_builder.method_calls).is_equal_to([
            call.set_number(31),
            call.set_user('<user>'),
            call.set_name('<name>'),
            call.set_country('<country>'),
            call.set_x(11.0),
            call.set_y(49.0),
            call.set_timestamp(self.timestamp),
            call.build()
        ])

    def test_strike_mapper_with_timezone(self):
        zone_info = ZoneInfo('CET')

        self.strike_mapper.create_object(self.result, timezone=zone_info)

        timestamp = self.station_builder.set_timestamp.call_args[0][0]

        assert_that(timestamp).is_equal_to(self.timestamp)
        assert_that(timestamp.tzinfo).is_equal_to(zone_info)

    def test_strike_mapper_without_timestamp(self):
        self.result['begin'] = None

        self.strike_mapper.create_object(self.result)

        assert_that(self.station_builder.set_timestamp.call_args[0][0]).is_none()
