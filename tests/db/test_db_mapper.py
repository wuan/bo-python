# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

from unittest import TestCase
import datetime

import pytz
from mock import Mock, call
from hamcrest import assert_that, is_, equal_to, none

import blitzortung.builder
import blitzortung.db.mapper


class TestStrikeMapper(TestCase):
    def setUp(self):
        self.strike_builder = Mock(name="strike_builder", spec=blitzortung.builder.Strike)
        self.strike_mapper = blitzortung.db.mapper.Strike(self.strike_builder)

        self.timestamp = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
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
        assert_that(self.strike_mapper.create_object(self.result), is_(self.strike))

        assert_that(self.strike_builder.method_calls, is_([
            call.set_id(12),
            call.set_timestamp(self.timestamp, 789),
            call.set_x(11.0),
            call.set_y(51.0),
            call.set_altitude(123),
            call.set_amplitude(21323),
            call.set_station_count(12),
            call.set_lateral_error(5000),
            call.build()
        ]))

    def test_strike_mapper_with_timezone(self):
        zone = pytz.timezone('CET')

        self.strike_mapper.create_object(self.result, timezone=zone)

        timestamp = self.strike_builder.set_timestamp.call_args[0][0]

        assert_that(timestamp, is_(self.timestamp))
        assert_that(timestamp.tzinfo.zone, is_(zone.zone))

    def test_strike_mapper_without_timestamp(self):
        self.result['timestamp'] = None

        self.strike_mapper.create_object(self.result)

        assert_that(self.strike_builder.set_timestamp.call_args[0][0], is_(none()))
