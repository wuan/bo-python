# -*- coding: utf8 -*-

"""

@author Andreas Würl

"""

import datetime, pytz
import math
import HTMLParser
from injector import Module, provides
import numpy as np
import pandas as pd

import blitzortung


class BuildError(blitzortung.Error):
    pass


class Base(object):
    pass


class Timestamp(Base):
    time_format = '%Y-%m-%d %H:%M:%S'
    time_format_fractional_seconds = time_format + '.%f'
    timestamp_string_minimal_fractional_seconds_length = 20
    timestamp_string_microseconds_length = 26

    def __init__(self):
        super(Timestamp, self).__init__()
        self.timestamp = None

    def set_timestamp(self, timestamp, nanoseconds=0):
        if not timestamp:
            self.timestamp = None
        elif isinstance(timestamp, pd.Timestamp):
            if nanoseconds:
                self.timestamp = pd.Timestamp(timestamp.value + nanoseconds, tz=timestamp.tzinfo)
            else:
                self.timestamp = timestamp
        elif isinstance(timestamp, datetime.datetime):
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
            return None

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
        return blitzortung.data.Event(self.timestamp, self.x_coord, self.y_coord)


class Stroke(Event):
    def __init__(self):
        super(Stroke, self).__init__()
        self.id_value = -1
        self.altitude = None
        self.amplitude = None
        self.type_val = None
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

    def set_type(self, type_val):
        self.type_val = type_val
        return self

    def set_lateral_error(self, lateral_error):
        self.lateral_error = lateral_error if lateral_error > 0 else 0
        return self

    def set_station_count(self, station_count):
        self.station_count = station_count
        return self

    def set_stations(self, stations):
        self.stations = stations
        return self

    def build(self):
        return blitzortung.data.Stroke(self.id_value, self.timestamp, self.x_coord, self.y_coord, self.amplitude,
                                       self.altitude, self.lateral_error, self.type_val, self.station_count,
                                       self.stations)

    def from_data(self, data):
        """ Construct stroke from new blitzortung text format data line """
        self.set_timestamp(data['date'] + ' ' + data['time'])
        position = data['pos']
        self.set_x(float(position[1]))
        self.set_y(float(position[0]))
        self.set_altitude(float(position[2]))
        self.set_amplitude(float(data['str']))
        self.set_lateral_error(float(data['dev']))
        if 'typ' in data:
            self.set_type(int(data['typ']))
        stations = data['sta']
        self.set_station_count(int(stations[0]))
        self.set_stations([int(station) for station in stations[2].split(',')])
        return self


class Station(Event):
    html_parser = HTMLParser.HTMLParser()

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

    def from_data(self, data):
        try:
            self.set_number(int(data['station']))
            self.set_user(int(data['user']))
            self.set_name(data['city'])
            self.set_country(data['country'])
            pos = data['pos']
            self.set_x(float(pos[1]))
            self.set_y(float(pos[0]))
            self.set_board(data['board'])
            self.set_status(data['status'])
            self.set_timestamp(data['last_signal'])
        except KeyError:
            raise BuildError()
        return self

    def build(self):
        return blitzortung.data.Station(self.number, self.user, self.name, self.country,
                                        self.x_coord, self.y_coord, self.timestamp, self.status,
                                        self.board)

    @staticmethod
    def _unquote(html_coded_string):
        return Station.html_parser.unescape(html_coded_string.replace('&nbsp;', ' '))


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
        return blitzortung.data.StationOffline(self.id_value, self.number, self.begin, self.end)


class RawEvent(Event):
    def __init__(self):
        super(RawEvent, self).__init__()
        self.altitude = 0
        self.amplitude = 0
        self.x_amplitude = 0
        self.y_amplitude = 0
        self.angle = 0

    def build(self):
        return blitzortung.data.RawEvent(self.timestamp, self.x_coord, self.y_coord, self.altitude, self.amplitude,
                                         self.angle)

    def set_altitude(self, altitude):
        self.altitude = altitude
        return self

    def set_amplitude(self, amplitude):
        self.amplitude = amplitude
        return self

    def set_angle(self, angle):
        self.angle = angle
        return self

    def from_json(self, json_object):
        self.set_timestamp(json_object[0])
        self.set_x(json_object[1])
        self.set_y(json_object[2])
        self.set_altitude(json_object[3])
        self.set_amplitude(json_object[6])
        self.set_angle(json_object[7])
        return self

    def from_string(self, string):
        """ Construct stroke from blitzortung text format data line """
        if string:
            fields = string.split(' ')
            if len(fields) >= 8:
                self.set_x(float(fields[2]))
                self.set_y(float(fields[3]))
                self.set_timestamp(' '.join(fields[0:2]))
                self.set_timestamp(self.timestamp + datetime.timedelta(seconds=1))
                self.set_altitude(int(fields[4]))
                self.x_amplitude = float(fields[7])
                self.y_amplitude = float(fields[8])
            else:
                raise RuntimeError("not enough data fields for raw event data '%s'" % string)
        return self


class RawWaveformEvent(Event):
    def __init__(self):
        super(RawWaveformEvent, self).__init__()
        self.altitude = 0
        self.sample_period = 0
        self.x_values = None
        self.y_values = None
        self.angle_offset = 0

    def build(self):
        return blitzortung.data.RawWaveformEvent(self.timestamp, self.x_coord, self.y_coord, self.altitude,
                                                 self.sample_period, self.x_values, self.y_values,
                                                 self.angle_offset)

    def set_altitude(self, altitude):
        self.altitude = altitude
        return self

    def set_sample_period(self, sample_period):
        self.sample_period = sample_period
        return self

    def set_x_values(self, x_values):
        self.x_values = x_values
        return self

    def set_y_values(self, y_values):
        self.y_values = y_values
        return self

    def set_angle_offset(self, angle_offset):
        self.angle_offset = angle_offset
        return self

    def from_json(self, json_object):
        self.set_timestamp(json_object[0])
        self.set_x(json_object[1])
        self.set_y(json_object[2])
        self.set_altitude(json_object[3])
        self.set_sample_period(json_object[5])
        self.set_angle_offset(json_object[7])
        self.set_x_values(json_object[9][0])
        if len(json_object[9]) > 1:
            self.set_y_values(json_object[9][1])

        return self

    def from_string(self, string):
        """ Construct stroke from blitzortung text format data line """
        if string:
            fields = string.split(' ')
            self.set_y(float(fields[2]))
            self.set_x(float(fields[3]))
            self.set_timestamp(' '.join(fields[0:2]))
            self.timestamp += datetime.timedelta(seconds=1)
            if len(fields) >= 8:
                self.number_of_satellites = int(fields[4])
                self.sample_period = int(fields[8])

                number_of_channels = int(fields[5])
                number_of_samples = int(fields[6])
                chars_per_sample = int(fields[7]) / 4
                data = fields[9]

                maximum = 0.0
                maximum_index = 0
                maximum_values = [0] * number_of_channels
                current_values = [0] * number_of_channels

                value_offset = -(1 << (chars_per_sample * 4 - 1))
                for sample in range(0, number_of_samples):
                    current_sum = 0.0
                    for channel in range(0, number_of_channels):
                        index = chars_per_sample * (number_of_channels * sample + channel)
                        value_string = data[index: index + chars_per_sample]
                        value = int(value_string, 16) + value_offset

                        current_values[channel] = value
                        current_sum += value * value

                    if math.sqrt(current_sum) > maximum:
                        maximum = math.sqrt(current_sum)
                        maximum_index = sample
                        maximum_values = list(current_values)

                self.x_amplitude = maximum_values[0]
                if number_of_channels > 1:
                    self.y_amplitude = maximum_values[1]

                self.timestamp += np.timedelta64(maximum_index * self.sample_period, 'ns')

            else:
                raise RuntimeError("not enough data fields for raw event data '%s'" % string)
        return self


class ExtEvent(RawEvent):
    def __init__(self):
        super(ExtEvent, self).__init__()
        self.station_number = 0

    def set_station_number(self, station_number):
        self.station_number = station_number
        return self

    def build(self):
        return blitzortung.data.ExtEvent(self.timestamp, self.x_coord, self.y_coord, self.altitude, self.amplitude,
                                         self.angle, self.station_number)


