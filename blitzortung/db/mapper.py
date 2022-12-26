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
from abc import abstractmethod

import shapely.wkb
from injector import inject

from .. import builder


class ObjectMapper:
    @abstractmethod
    def create_object(self, result, **kwargs):
        pass

    @staticmethod
    def convert_to_timezone(timestamp, target_timezone=None):
        if timestamp is not None:
            target_timezone = target_timezone if target_timezone is not None else datetime.timezone.utc
            timestamp = timestamp.astimezone(target_timezone)
        #            if target_timezone != datetime.timezone.utc:
        #                timestamp = target_timezone.enfold(timestamp)
        return timestamp


class Strike(ObjectMapper):
    @inject
    def __init__(self, strike_builder: builder.Strike):
        self.strike_builder = strike_builder

    def create_object(self, result, **kwargs):
        timezone = kwargs['timezone'] if 'timezone' in kwargs else datetime.timezone.utc

        self.strike_builder.set_id(result['id'])
        self.strike_builder.set_timestamp(
            self.convert_to_timezone(result['timestamp'], timezone),
            result['nanoseconds'])
        self.strike_builder.set_x(result['x'])
        self.strike_builder.set_y(result['y'])
        self.strike_builder.set_altitude(result['altitude'])
        self.strike_builder.set_amplitude(result['amplitude'])
        self.strike_builder.set_station_count(result['stationcount'])
        self.strike_builder.set_lateral_error(result['error2d'])

        return self.strike_builder.build()


class Station(ObjectMapper):
    @inject
    def __init__(self, station_builder: builder.Station):
        self.station_builder = station_builder

    def create_object(self, result, **kwargs):
        timezone = kwargs['timezone'] if 'timezone' in kwargs else datetime.timezone.utc

        self.station_builder.set_number(result['number'])
        self.station_builder.set_user(result['user'])
        self.station_builder.set_name(result['name'])
        self.station_builder.set_country(result['country'])
        location = shapely.wkb.loads(result['geog'], hex=True)
        self.station_builder.set_x(location.x)
        self.station_builder.set_y(location.y)
        self.station_builder.set_timestamp(
            self.convert_to_timezone(result['begin'], timezone))

        return self.station_builder.build()


class StationOffline(ObjectMapper):
    @inject
    def __init__(self, station_offline_builder: builder.StationOffline):
        self.station_offline_builder = station_offline_builder

    def create_object(self, result, **kwargs):
        self.station_offline_builder.set_id(result['id'])
        self.station_offline_builder.set_number(result['number'])
        self.station_offline_builder.set_begin(result['begin'])
        self.station_offline_builder.set_end(result['end'])

        return self.station_offline_builder.build()
