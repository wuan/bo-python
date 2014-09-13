# -*- coding: utf8 -*-

"""
Copyright (C) 2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import datetime
import itertools
import re
from injector import inject

import pytz
import numpy as np
import pandas as pd
from geographiclib.polygonarea import PolygonArea
from geographiclib.geodesic import Geodesic

from . import Error
from . import data
from .util import next_element, force_range


class BuilderError(Error):
    pass


class Base(object):
    pass


class Timestamp(Base):
    timestamp_string_minimal_fractional_seconds_length = 20
    timestamp_string_microseconds_length = 26

    def __init__(self):
        super(Timestamp, self).__init__()
        self.timestamp = None

    def set_timestamp(self, timestamp, nanoseconds=0):
        if not timestamp:
            self.timestamp = None
        elif type(timestamp) == pd.Timestamp:
            if nanoseconds:
                self.timestamp = pd.Timestamp(timestamp.value + nanoseconds, tz=timestamp.tzinfo)
            else:
                self.timestamp = timestamp
        elif type(timestamp) == datetime.datetime:
            total_nanoseconds = pd.Timestamp(timestamp).value + nanoseconds
            self.timestamp = pd.Timestamp(total_nanoseconds, tz=timestamp.tzinfo)
        else:
            self.timestamp = self.__parse_timestamp(timestamp)
        return self

    @staticmethod
    def __parse_timestamp(timestamp_string):
        try:
            timestamp = np.datetime64(timestamp_string + 'Z', 'ns')
            return pd.Timestamp(timestamp, tz=pytz.UTC)
        except ValueError:
            return pd.NaT

    def build(self):
        return self.timestamp


class Event(Timestamp):
    def __init__(self):
        super(Event, self).__init__()
        self.x_coord = 0
        self.y_coord = 0

    def set_x(self, x_coord):
        self.x_coord = x_coord
        return self

    def set_y(self, y_coord):
        self.y_coord = y_coord
        return self

    def build(self):
        return data.Event(self.timestamp, self.x_coord, self.y_coord)


class Strike(Event):
    position_parser = re.compile(r'pos;([-0-9\.]+);([-0-9\.]+);([-0-9\.]+)')
    amplitude_parser = re.compile(r'str;([0-9\.]+)')
    deviation_parser = re.compile(r'dev;([0-9\.]+)')
    stations_parser = re.compile(r'sta;([0-9]+);([0-9]+);([^ ]+)')

    def __init__(self):
        super(Strike, self).__init__()
        self.id_value = -1
        self.altitude = None
        self.amplitude = None
        self.lateral_error = None
        self.station_count = None
        self.stations = []

    def set_id(self, id_value):
        self.id_value = id_value
        return self

    def set_altitude(self, altitude):
        self.altitude = altitude
        return self

    def set_amplitude(self, amplitude):
        self.amplitude = amplitude
        return self

    def set_lateral_error(self, lateral_error):
        self.lateral_error = force_range(0, lateral_error, 32767)
        return self

    def set_station_count(self, station_count):
        self.station_count = station_count
        return self

    def set_stations(self, stations):
        self.stations = stations
        return self

    def from_line(self, line):
        """ Construct strike from new blitzortung text format data line """
        try:
            self.set_timestamp(line[0:29])

            position = self.position_parser.findall(line)[0]
            self.set_x(float(position[1]))
            self.set_y(float(position[0]))
            self.set_altitude(float(position[2]))

            self.set_amplitude(float(self.amplitude_parser.findall(line)[0]))

            self.set_lateral_error(float(self.deviation_parser.findall(line)[0]))
            stations = self.stations_parser.findall(line)[0]
            self.set_station_count(int(stations[0]))
            self.set_stations([int(station) for station in stations[2].split(',') if station])
        except (KeyError, ValueError, IndexError) as e:
            raise BuilderError(e)

        return self

    def build(self):
        return data.Strike(self.id_value, self.timestamp, self.x_coord, self.y_coord, self.altitude,
                           self.amplitude, self.lateral_error, self.station_count, self.stations)


class StrikeCluster(object):
    """
    class for strike cluster objects
    """

    def __init__(self):
        self.cluster_id = -1
        self.timestamp = None
        self.interval_seconds = 0
        self.shape = None
        self.strike_count = 0

    def with_id(self, cluster_id):
        self.cluster_id = cluster_id
        return self

    def with_timestamp(self, timestamp):
        self.timestamp = timestamp
        return self

    def with_interval_seconds(self, interval_seconds):
        self.interval_seconds = interval_seconds
        return self

    def with_shape(self, shape):
        self.shape = shape

        return self

    def with_strike_count(self, strike_count):
        self.strike_count = strike_count
        return self

    def build(self):
        if self.shape is not None:
            poly_area = PolygonArea(Geodesic.WGS84)
            print(self.shape.coords)
            if self.shape.coords:
                for (x, y) in zip(self.shape.coords.xy[0], self.shape.coords.xy[1]):
                    poly_area.AddPoint(x, y)
                area = poly_area.Compute(False, True)[2] / 1e6
            else:
                area = 0.0;
        else:
            area = None

        return data.StrikeCluster(self.cluster_id, self.timestamp, self.interval_seconds, self.shape, self.strike_count,
                                  area)


class Station(Event):
    station_parser = re.compile(r'station;([0-9]+)')
    user_parser = re.compile(r'user;([0-9]+)')
    city_parser = re.compile(r'city;"([^"]+)"')
    country_parser = re.compile(r'country;"([^"]+)"')
    position_parser = re.compile(r'pos;([-0-9\.]+);([-0-9\.]+);([-0-9\.]+)')
    status_parser = re.compile(r'status;"?([^ ]+)"?')
    board_parser = re.compile(r'board;"?([^ ]+)"?')
    last_signal_parser = re.compile(r'last_signal;"([-: 0-9]+)" ?')

    def __init__(self):
        super(Station, self).__init__()
        self.number = -1
        self.user = -1
        self.name = None
        self.country = None
        self.status = None
        self.board = None

    def set_number(self, number):
        self.number = number
        return self

    def set_user(self, user):
        self.user = user
        return self

    def set_name(self, name):
        self.name = name
        return self

    def set_country(self, country):
        self.country = country
        return self

    def set_board(self, board):
        self.board = board

    def set_status(self, status):
        self.status = status

    def from_line(self, line):
        try:
            self.set_number(int(self.station_parser.findall(line)[0]))
            self.set_user(int(self.user_parser.findall(line)[0]))
            self.set_name(self.city_parser.findall(line)[0])
            self.set_country(self.country_parser.findall(line)[0])
            pos = self.position_parser.findall(line)[0]
            self.set_x(float(pos[1]))
            self.set_y(float(pos[0]))
            self.set_board(self.board_parser.findall(line)[0])
            # self.set_status(self.status_parser.findall(line)[0])
            self.set_timestamp(self.last_signal_parser.findall(line)[0])
        except (KeyError, ValueError, IndexError) as e:
            raise BuilderError(e)
        return self

    def build(self):
        return data.Station(self.number, self.user, self.name, self.country,
                            self.x_coord, self.y_coord, self.timestamp, self.status,
                            self.board)


class StationOffline(Base):
    def __init__(self):
        super(StationOffline, self).__init__()
        self.id_value = -1
        self.number = -1
        self.begin = None
        self.end = None

    def set_id(self, id_value):
        self.id_value = id_value
        return self

    def set_number(self, number):
        self.number = number
        return self

    def set_begin(self, begin):
        self.begin = begin
        return self

    def set_end(self, end):
        self.end = end
        return self

    def build(self):
        return data.StationOffline(self.id_value, self.number, self.begin, self.end)


class ChannelWaveform(object):
    fields_per_channel = 11

    def __init__(self):
        self.channel_number = None
        self.amplifier_version = None
        self.antenna = None
        self.gain = None
        self.values = None
        self.start = None
        self.bits = 0
        self.shift = None
        self.conversion_gap = None
        self.conversion_time = None
        self.waveform = None

    def from_field_iterator(self, field):
        self.channel_number = int(next_element(field))
        self.amplifier_version = next_element(field)
        self.antenna = int(next_element(field))
        self.gain = next_element(field)
        self.values = int(next_element(field))
        self.start = int(next_element(field))
        self.bits = int(next_element(field))
        self.shift = int(next_element(field))
        self.conversion_gap = int(next_element(field))
        self.conversion_time = int(next_element(field))
        self.__extract_waveform_from_hex_string(next_element(field))
        return self

    def __extract_waveform_from_hex_string(self, waveform_hex_string):
        hex_character = iter(waveform_hex_string)
        self.waveform = np.zeros(self.values)
        bits_per_char = 4
        chars_per_sample = self.bits // bits_per_char
        value_offset = -(1 << (chars_per_sample * 4 - 1))

        for index in range(0, self.values):
            value_text = "".join(itertools.islice(hex_character, chars_per_sample))
            value = int(value_text, 16)
            self.waveform[index] = value + value_offset

    def build(self):
        return data.ChannelWaveform(
            self.channel_number,
            self.amplifier_version,
            self.antenna,
            self.gain,
            self.values,
            self.start,
            self.bits,
            self.shift,
            self.conversion_gap,
            self.conversion_time,
            self.waveform)


class RawWaveformEvent(Event):
    @inject(channel_builder=ChannelWaveform)
    def __init__(self, channel_builder):
        super(RawWaveformEvent, self).__init__()
        self.altitude = 0
        self.channels = []

        self.channel_builder = channel_builder

    def build(self):
        return data.RawWaveformEvent(
            self.timestamp,
            self.x_coord,
            self.y_coord,
            self.altitude,
            self.channels
        )

    def set_altitude(self, altitude):
        self.altitude = altitude
        return self

    def from_json(self, json_object):
        self.set_timestamp(json_object[0])
        self.set_x(json_object[1])
        self.set_y(json_object[2])
        self.set_altitude(json_object[3])
        if len(json_object[9]) > 1:
            self.set_y(json_object[9][1])

        return self

    def from_string(self, string):
        """ Construct strike from blitzortung text format data line """
        if string:
            try:
                field = iter(string.split(' '))
                self.set_timestamp(next_element(field) + ' ' + next_element(field))
                self.timestamp += datetime.timedelta(seconds=1)
                self.set_y(float(next_element(field)))
                self.set_x(float(next_element(field)))
                self.set_altitude(int(next_element(field)))

                self.channels = []
                while True:
                    try:
                        self.channel_builder.from_field_iterator(field)
                    except StopIteration:
                        break
                    self.channels.append(self.channel_builder.build())
            except ValueError as e:
                raise BuilderError(e)

        return self


