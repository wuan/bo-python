# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

import datetime, pytz
import math
import HTMLParser

import data

class Base(object):

    timeformat = '%Y-%m-%d %H:%M:%S'
    timeformat_fractional_seconds = timeformat + '.%f'
    timestamp_string_minimal_fractional_seconds_length = 20
    timestamp_string_microseconds_length = 26

    def parse_timestamp(self, timestamp_string):
        if len(timestamp_string) > Base.timestamp_string_minimal_fractional_seconds_length:
            timestamp_format = Base.timeformat_fractional_seconds
            if len(timestamp_string) > Base.timestamp_string_microseconds_length:
                timestamp_string = timestamp_string[:self.timestamp_string_microseconds_length]
        else:
            timestamp_format = Base.timeformat

        timestamp = datetime.datetime.strptime(timestamp_string, timestamp_format)
        return timestamp.replace(tzinfo=pytz.UTC)

    def parse_timestamp_with_nanoseconds(self, timestamp_string):
        timestamp = self.parse_timestamp(timestamp_string)
        if len(timestamp_string) > Base.timestamp_string_microseconds_length:
            nanoseconds_string = timestamp_string[self.timestamp_string_microseconds_length:self.timestamp_string_microseconds_length + 3]
            nanoseconds = int(nanoseconds_string.ljust(3).replace(' ', '0'))
        else:
            nanoseconds = 0
        return (timestamp, nanoseconds)

class Stroke(Base):

    def __init__(self):
        self.id_value = -1
        self.altitude = None
        self.participants = []

    def set_id(self, id_value):
        self.id_value = id_value

    def set_timestamp(self, timestamp):
        self.timestamp = timestamp

    def set_timestamp_nanoseconds(self, timestamp_nanoseconds):
        self.timestamp_nanoseconds = timestamp_nanoseconds

    def set_x(self, x):
        self.x = x

    def set_y(self, y):
        self.y = y

    def set_altitude(self, altitude):
        self.altitude = altitude

    def set_amplitude(self, amplitude):
        self.amplitude = amplitude

    def set_type(self, type_val):
        self.type_val = type_val

    def set_lateral_error(self, lateral_error):
        self.lateral_error = lateral_error if lateral_error > 0 else 0

    def set_station_count(self, station_count):
        self.station_count = station_count

    def set_participants(self, participants):
        self.participants = participants

    def build(self):
        return data.Stroke(self.id_value, self.x, self.y, self.timestamp, self.timestamp_nanoseconds, self.amplitude, self.altitude, self.lateral_error, self.type_val, self.station_count, self.participants)

    def from_string(self, string):
        if string != None:
            ' Construct stroke from blitzortung text format data line '
            fields = string.split(' ')
            self.set_x(float(fields[3]))
            self.set_y(float(fields[2]))
            (self.timestamp, self.timestamp_nanoseconds) = self.parse_timestamp_with_nanoseconds(' '.join(fields[0:2]))

            if len(fields) >= 5:
                self.set_amplitude(float(fields[4][:-2]) * 1e3)
                self.set_type(int(fields[5]))
                self.set_lateral_error(int(fields[6][:-1]))
                self.set_station_count(int(fields[7]))
                participants = []
                if (len(fields) >=9):
                    for index in range(8,len(fields)):
                        participants.append(fields[index])
                self.set_participants(participants)
            else:
                raise RuntimeError("not enough data fields from stroke data line '%s'" %(string))
        self.set_altitude(0.0)


class Station(Base):

    html_parser = HTMLParser.HTMLParser()

    def __init__(self):
        self.number = -1
        self.location_name = None
        self.last_data = None
        self.gps_status = None
        self.samples_per_hour = -1
        self.tracker_version = None
        self.offline_since = None

    def set_number(self, number):
        self.number = number

    def set_short_name(self, short_name):
        self.short_name = short_name

    def set_name(self, name):
        self.name = name

    def set_location_name(self, location_name):
        self.location_name = location_name

    def set_country(self, country):
        self.country = country

    def set_offline_since(self, offline_since):
        self.offline_since = offline_since

    def set_x(self, x):
        self.x = x

    def set_y(self, y):
        self.y = y

    def set_last_data(self, last_data):
        if isinstance(last_data, str):
            (self.last_data, _) = self.parse_timestamp_with_nanoseconds(last_data)
        else:
            self.last_data = last_data

    def set_gps_status(self, gps_status):
        self.gps_status = gps_status

    def set_tracker_version(self, tracker_version):
        self.tracker_version = tracker_version

    def set_samples_per_hour(self, samples_per_hour):
        self.samples_per_hour = samples_per_hour

    def from_string(self, data):
        fields = data.split(' ')
        self.set_number(int(fields[0]))
        self.set_short_name(fields[1])
        self.set_name(self._unquote(fields[2]))
        self.set_location_name(self._unquote(fields[3]))
        self.set_country(self._unquote(fields[4]))
        self.set_x(float(fields[6]))
        self.set_y(float(fields[5]))
        self.set_last_data(self._unquote(fields[7]).encode('ascii'))
        self.set_gps_status(fields[8])
        self.set_tracker_version(self._unquote(fields[9]))
        self.set_samples_per_hour(int(fields[10]))

    def build(self):
        return data.Station(self.number, self.short_name, self.name, self.location_name, self.country, self.x, self.y, self.last_data, self.offline_since, self.gps_status, self.tracker_version, self.samples_per_hour)

    def _unquote(self, html_coded_string):
        return Station.html_parser.unescape(html_coded_string.replace('&nbsp;', ' '))

class StationOffline(Base):

    def __init__(self):
        self.id_number = -1
        self.number = -1
        self.begin = None
        self.end = None

    def set_id(self, id_number):
        self.id_number = id_number

    def set_number(self, number):
        self.number = number

    def set_begin(self, begin):
        self.begin = begin

    def set_end(self, end):
        self.end = end

    def build(self):
        return data.StationOffline(self.id_number, self.number, self.begin, self.end)

class RawEvent(Base):

    def __init__(self):
        self.x = 0
        self.y = 0
        self.timestamp = None
        self.timestamp_nanoseconds = 0
        self.height = 0
        self.numberOfSatellites = 0
        self.samplePeriod = 0
        self.amplitude_x = 0
        self.amplitude_y = 0

    def build(self):
        return data.RawEvent(self.x, self.y, self.timestamp, self.timestamp_nanoseconds, self.height, self.numberOfSatellites, self.samplePeriod, self.amplitude_x, self.amplitude_y)

    def from_string(self, string):
        if string != None:
            ' Construct stroke from blitzortung text format data line '
            fields = string.split(' ')
            self.x = float(fields[2])
            self.y = float(fields[3])
            (self.timestamp, self.timestamp_nanoseconds) = self.parse_timestamp_with_nanoseconds(' '.join(fields[0:2]))
            self.timestamp = self.timestamp + datetime.timedelta(seconds=1)
            if len(fields) >= 8:
                self.height = int(fields[4])
                self.numberOfSatellites = int(fields[5])
                self.samplePeriod = int(fields[6])
                self.amplitude_x = float(fields[7])
                self.amplitude_y = float(fields[8])
            else:
                raise RuntimeError("not enough data fields for raw event data '%s'" %(data))

    def from_archive_string(self, string):
        if string != None:
            ' Construct stroke from blitzortung text format data line '
            fields = string.split(' ')
            self.y = float(fields[2])
            self.x = float(fields[3])
            (self.timestamp, self.timestamp_nanoseconds) = self.parse_timestamp_with_nanoseconds(' '.join(fields[0:2]))
            self.timestamp = self.timestamp + datetime.timedelta(seconds=1)
            if len(fields) >= 8:
                self.numberOfSatellites = int(fields[4])
                self.samplePeriod = int(fields[8])

                number_of_channels = int(fields[5])
                number_of_samples = int(fields[6])
                chars_per_sample = int(fields[7])/4
                data = fields[9]

                maximum = 0.0
                maximum_index = 0
                maximum_values = [0] * number_of_channels;
                current_values = [0] * number_of_channels;

                value_offset = -(1 << (chars_per_sample * 4 - 1))
                for sample in range(0, number_of_samples):
                    current_sum = 0.0
                    for channel in range(0, number_of_channels):
                        index = chars_per_sample * (number_of_channels * sample + channel)
                        value_string = data[index : index+ chars_per_sample]
                        value = int(value_string, 16) + value_offset

                        current_values[channel] = value
                        current_sum += value * value

                    if math.sqrt(current_sum) > maximum:
                        maximum = math.sqrt(current_sum)
                        maximum_index = sample
                        maximum_values = list(current_values)

                self.amplitude_x = maximum_values[0]
                if number_of_channels > 1:
                    self.amplitude_y = maximum_values[1]

                self.timestamp_nanoseconds += maximum_index * self.samplePeriod # add maximum offset to time
                self.timestamp += datetime.timedelta(microseconds=self.timestamp_nanoseconds / 1000) # fix nanoseconds overflow
                self.timestamp_nanoseconds %= 1000;


            else:
                raise RuntimeError("not enough data fields for raw event data '%s'" %(data))

class ExtEvent(RawEvent):

    def __init__(self):
        super(ExtEvent, self).__init__()
        self.station_number = 0

    def set_station_number(self, station_number):
        self.station_number = station_number

    def build(self):
        return data.ExtEvent(self.x, self.y, self.timestamp, self.timestamp_nanoseconds, self.height, self.numberOfSatellites, self.samplePeriod, self.amplitude_x, self.amplitude_y, self.station_number)


