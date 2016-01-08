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

from abc import abstractmethod
from injector import inject
import pytz
import shapely.wkb

import blitzortung.builder


class ObjectMapper(object):
    @abstractmethod
    def create_object(self, result, **kwargs):
        pass

    @staticmethod
    def convert_to_timezone(timestamp, target_timezone=None):
        if timestamp is not None:
            target_timezone = target_timezone if target_timezone is not None else pytz.UTC
            timestamp = timestamp.astimezone(target_timezone)
            if target_timezone != pytz.UTC:
                timestamp = target_timezone.normalize(timestamp)
        return timestamp


class Strike(ObjectMapper):
    @inject(strike_builder=blitzortung.builder.Strike)
    def __init__(self, strike_builder):
        self.strike_builder = strike_builder

    def create_object(self, result, **kwargs):
        timezone = kwargs['timezone'] if 'timezone' in kwargs else pytz.UTC

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


class StrikeCluster(ObjectMapper):
    @inject(strike_cluster_builder=blitzortung.builder.StrikeCluster)
    def __init__(self, strike_cluster_builder):
        self.strike_cluster_builder = strike_cluster_builder

    def create_object(self, result, **kwargs):
        timezone = kwargs['timezone'] if 'timezone' in kwargs else pytz.UTC

        self.strike_cluster_builder\
            .with_id(result['id'])\
            .with_timestamp(self.convert_to_timezone(result['timestamp'], timezone))\
            .with_interval_seconds(kwargs['interval_seconds'])\
            .with_shape(shapely.wkb.loads(result['geom'], hex=True))\
            .with_strike_count(result['strike_count'])

        return self.strike_cluster_builder.build()


class Station(ObjectMapper):
    @inject(station_builder=blitzortung.builder.Station)
    def __init__(self, station_builder):
        self.station_builder = station_builder

    def create_object(self, result, **kwargs):
        timezone = kwargs['timezone'] if 'timezone' in kwargs else pytz.UTC

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
    @inject(station_offline_builder=blitzortung.builder.StationOffline)
    def __init__(self, station_offline_builder):
        self.station_offline_builder = station_offline_builder

    def create_object(self, result, **kwargs):
        self.station_offline_builder.set_id(result['id'])
        self.station_offline_builder.set_number(result['number'])
        self.station_offline_builder.set_begin(result['begin'])
        self.station_offline_builder.set_end(result['end'])

        return self.station_offline_builder.build()
