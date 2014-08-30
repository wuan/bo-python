# -*- coding: utf8 -*-

"""
Copyright (C) 2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

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


class Strike(ObjectMapper):
    @inject(strike_builder=blitzortung.builder.Strike)
    def __init__(self, strike_builder):
        self.strike_builder = strike_builder

    def create_object(self, result, **kwargs):
        timezone = kwargs['timezone'] if 'timezone' in kwargs else pytz.UTC

        self.strike_builder.set_id(result['id'])
        timestamp_value = result['timestamp']
        self.strike_builder.set_timestamp(
            timestamp_value.astimezone(timezone) if timestamp_value else None, result['nanoseconds'])
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

        self.strike_cluster_builder.set_id(result['id'])
        start_time = result['start_time']
        self.strike_cluster_builder.set_start_time(start_time.astimezone(timezone) if start_time else None)
        end_time = result['end_time']
        self.strike_cluster_builder.set_end_time(end_time.astimezone(timezone) if end_time else None)
        self.strike_cluster_builder.set_shape(shapely.wkb.loads(result['geog'], hex=True))
        self.strike_cluster_builder.set_strike_count(result['strike_count'])

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
        timestamp_value = result['begin']
        self.station_builder.set_timestamp(timestamp_value.astimezone(timezone) if timestamp_value else None)

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
