# -*- coding: utf8 -*-

"""

@author Andreas Würl

"""

import datetime, pytz
import math
import HTMLParser
from injector import Module, provides
import itertools
import numpy as np
import pandas as pd

import blitzortung


class BuilderError(blitzortung.Error):
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
        return blitzortung.data.Stroke(self.id_value, self.timestamp, self.x_coord, self.y_coord, self.altitude,
                                       self.amplitude, self.lateral_error, self.station_count, self.stations)

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
            raise BuilderError()
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


class RawWaveformEvent(Event):
    def __init__(self, channel_builder):
        super(RawWaveformEvent, self).__init__()
        self.altitude = 0
        self.channels = []

        self.channel_builder = channel_builder

    def build(self):
        return blitzortung.data.RawWaveformEvent(
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
        self.set_sample_period(json_object[5])
        self.set_angle_offset(json_object[7])
        self.set_x_values(json_object[9][0])
        if len(json_object[9]) > 1:
            self.set_y_values(json_object[9][1])

        return self

    def from_string(self, string):
        """ Construct stroke from blitzortung text format data line """
        if string:
            field = iter(string.split(' '))
            self.set_timestamp(field.next() + ' ' + field.next())
            self.timestamp += datetime.timedelta(seconds=1)
            self.set_y(float(field.next()))
            self.set_x(float(field.next()))
            self.set_altitude(int(field.next()))

            self.channels = []
            while True:
                try:
                    self.channel_builder.from_field_iterator(field)
                except StopIteration:
                    break
                self.channels.append(self.channel_builder.build())

        return self


class ChannelWaveform(object):
    fields_per_channel = 11

    def __init__(self):
        self.channel_number = None
        self.amplifier_version = None
        self.antenna = None
        self.gain = None
        self.values = None
        self.start = None
        self.bits = None
        self.shift = None
        self.conversion_gap = None
        self.conversion_time = None
        self.waveform = None

    def from_field_iterator(self, field):
        self.channel_number = int(field.next())
        self.amplifier_version = field.next()
        self.antenna = int(field.next())
        self.gain = field.next()
        self.values = int(field.next())
        self.start = int(field.next())
        self.bits = int(field.next())
        self.shift = int(field.next())
        self.conversion_gap = int(field.next())
        self.conversion_time = int(field.next())
        self.__extract_waveform_from_hex_string(field.next())
        return self

    def __extract_waveform_from_hex_string(self, waveform_hex_string):
        hex_character = iter(waveform_hex_string)
        self.waveform = []
        bits_per_char = 4
        chars_per_sample = self.bits / bits_per_char
        value_offset = -(1 << (chars_per_sample * 4 - 1))

        for _ in range(0, self.values):
            value_text = "".join(itertools.islice(hex_character, chars_per_sample))
            value = int(value_text, 16)
            self.waveform.append(value + value_offset)

    def build(self):
        return blitzortung.data.ChannelWaveform(
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


