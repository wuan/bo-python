# -*- coding: utf8 -*-

"""

   Copyright 2014-2022 Andreas Würl

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
import math

from . import base
from .geom import GridElement


class Timestamp(base.EqualityAndHash):
    timestamp_string_minimal_fractional_seconds_length = 20
    timestamp_string_microseconds_length = 26

    __slots__ = ['datetime', 'nanosecond']

    def __init__(self, date_time=datetime.datetime.now(datetime.timezone.utc), nanosecond=0):
        if isinstance(date_time, str):
            date_time, date_time_nanosecond = Timestamp.from_timestamp(date_time)
            nanosecond += date_time_nanosecond
        elif isinstance(date_time, int):
            date_time, date_time_nanosecond = Timestamp.from_nanoseconds(date_time)
            nanosecond += date_time_nanosecond

        if nanosecond < 0 or nanosecond > 999:
            microdelta = nanosecond // 1000
            date_time += datetime.timedelta(microseconds=microdelta)
            nanosecond -= microdelta * 1000

        self.datetime = date_time
        self.nanosecond = nanosecond

    @staticmethod
    def from_timestamp(timestamp_string):
        try:
            if len(timestamp_string) > Timestamp.timestamp_string_minimal_fractional_seconds_length:
                divider_index = Timestamp.timestamp_string_microseconds_length
                date_time = datetime.datetime.strptime(timestamp_string[:divider_index], '%Y-%m-%d %H:%M:%S.%f')
                nanosecond_string = timestamp_string[divider_index:]
                nanosecond = int(
                    float(nanosecond_string) * math.pow(10, 3 - len(nanosecond_string))) if nanosecond_string else 0
            else:
                date_time = datetime.datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S')
                nanosecond = 0

            return date_time.replace(tzinfo=datetime.timezone.utc), nanosecond
        except ValueError:
            return None, 0

    @staticmethod
    def from_nanoseconds(total_nanoseconds):
        total_microseconds = total_nanoseconds // 1000
        residual_nanoseconds = total_nanoseconds % 1000
        total_seconds = total_microseconds // 1000000
        residual_microseconds = total_microseconds % 1000000
        return datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + \
               datetime.timedelta(seconds=total_seconds,
                                  microseconds=residual_microseconds), \
            residual_nanoseconds

    @property
    def year(self):
        return self.datetime.year

    @property
    def month(self):
        return self.datetime.month

    @property
    def day(self):
        return self.datetime.day

    @property
    def hour(self):
        return self.datetime.hour

    @property
    def minute(self):
        return self.datetime.minute

    @property
    def second(self):
        return self.datetime.second

    @property
    def microsecond(self):
        return self.datetime.microsecond

    @property
    def tzinfo(self):
        return self.datetime.tzinfo

    epoch = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)

    @property
    def value(self):
        total_microseconds = int((self.datetime - self.epoch).total_seconds() * 1000000)
        return (total_microseconds * 1000 + self.nanosecond) if self.datetime is not None else -1

    @property
    def is_valid(self):
        return self.datetime is not None and self.datetime.year > 1900

    def __ne__(self, other):
        return self.datetime != other.datetime or self.nanosecond != other.nanosecond

    def __lt__(self, other):
        if isinstance(other, datetime.datetime):
            return self.datetime < other
        else:
            return self.datetime < other.datetime or (
                    self.datetime == other.datetime and self.nanosecond < other.nanosecond)

    def __le__(self, other):
        if isinstance(other, datetime.datetime):
            return self.datetime <= other
        else:
            return self.datetime < other.datetime or (
                    self.datetime == other.datetime and self.nanosecond <= other.nanosecond)

    def __gt__(self, other):
        if isinstance(other, datetime.datetime):
            return self.datetime > other
        else:
            return self.datetime > other.datetime or (
                    self.datetime == other.datetime and self.nanosecond > other.nanosecond)

    def __ge__(self, other):
        if isinstance(other, datetime.datetime):
            return self.datetime >= other
        else:
            return self.datetime > other.datetime or (
                    self.datetime == other.datetime and self.nanosecond >= other.nanosecond)

    def __add__(self, other):
        if isinstance(other, Timedelta):
            return Timestamp(self.datetime + other.timedelta, self.nanosecond + other.nanodelta)
        elif isinstance(other, datetime.timedelta):
            return Timestamp(self.datetime + other, self.nanosecond)
        elif isinstance(other, int):
            return Timestamp(self.datetime, self.nanosecond + other)
        return NotImplemented

    def __sub__(self, other):
        if isinstance(other, Timestamp):
            return Timedelta(self.datetime - other.datetime, self.nanosecond - other.nanosecond)
        elif isinstance(other, datetime.timedelta):
            return Timestamp(self.datetime - other, self.nanosecond)
        elif isinstance(other, int):
            return Timestamp(self.datetime, self.nanosecond - other)
        return NotImplemented

    def strftime(self, datetime_format):
        return self.datetime.strftime(datetime_format)

    def replace(self, **kwargs):
        if 'nanosecond' in kwargs:
            nanosecond = kwargs['nanosecond']
            del kwargs['nanosecond']
        else:
            nanosecond = self.nanosecond
        return Timestamp(self.datetime.replace(**kwargs), nanosecond)

    def __repr__(self):
        return "Timestamp({}{:03d})".format(self.datetime.strftime("%Y-%m-%d %H:%M:%S.%f"), self.nanosecond)


NaT = Timestamp(None)


class Timedelta(base.EqualityAndHash):
    def __init__(self, timedelta=datetime.timedelta(), nanodelta=0):
        if nanodelta < 0 or nanodelta > 999:
            microdelta = nanodelta // 1000
            timedelta += datetime.timedelta(microseconds=microdelta)
            nanodelta -= microdelta * 1000
        self.timedelta = timedelta
        self.nanodelta = nanodelta

    @property
    def days(self):
        return self.timedelta.days

    @property
    def seconds(self):
        return self.timedelta.seconds

    def __repr__(self):
        return "Timedelta({}, {})".format(self.timedelta, self.nanodelta)


class Event(base.Point):
    time_format = '%Y-%m-%d %H:%M:%S'
    time_format_fractional_seconds = time_format + '.%f'

    __slots__ = ['__timestamp']

    def __init__(self, timestamp, x_coord_or_point, y_coord=None):
        super().__init__(x_coord_or_point, y_coord)
        self.__timestamp = timestamp

    @property
    def timestamp(self):
        return self.__timestamp

    def difference_to(self, other):
        return other.timestamp - self.timestamp

    def ns_difference_to(self, other):
        value1 = other.timestamp.value
        value2 = self.timestamp.value
        difference = value1 - value2
        return difference

    def has_same_location(self, other):
        return super().__eq__(other)

    @property
    def is_valid(self):
        return (not math.isclose(self.x, 0.0, rel_tol=1e-09, abs_tol=1e-09) or not math.isclose(self.y, 0.0, rel_tol=1e-09)) \
            and -180 <= self.x <= 180 \
            and -90 < self.y < 90 \
            and self.has_valid_timestamp

    @property
    def has_valid_timestamp(self):
        return self.timestamp is not None and self.timestamp.is_valid

    def __lt__(self, other):
        return self.timestamp.value < other.timestamp.value

    def __le__(self, other):
        return self.timestamp.value <= other.timestamp.value

    def __str__(self):
        if self.has_valid_timestamp:
            timestamp_string = self.timestamp.strftime(self.time_format_fractional_seconds + 'XXX')
            timestamp_string = timestamp_string.replace('XXX', "%03d" % self.timestamp.nanosecond)
        else:
            timestamp_string = "NaT"

        return "%s %.4f %.4f" \
            % (timestamp_string, self.x, self.y)

    @property
    def uuid(self):
        return "%s-%05.0f-%05.0f" \
            % (str(self.timestamp().value), self.x * 100, self.y * 100)


class Strike(Event):
    """
    class for strike objects
    """

    __slots__ = ['id', 'altitude', 'amplitude', 'lateral_error', 'station_count', 'stations']

    def __init__(self, strike_id, timestamp, x_coord, y_coord, altitude, amplitude, lateral_error, station_count,
                 stations=None):
        super().__init__(timestamp, x_coord, y_coord)
        self.id = strike_id
        self.altitude = altitude
        self.amplitude = amplitude
        self.lateral_error = lateral_error
        self.station_count = station_count
        self.stations = [] if stations is None else stations

    def has_participant(self, participant):
        """
        returns true if the given participant is contained in the stations list
        """
        return participant in self.stations

    def __str__(self):
        return super().__str__() + " %s %.1f %d %d" % (
            str(self.altitude) if self.altitude is not None else '-',
            self.amplitude if self.amplitude else 0.0,
            self.lateral_error,
            self.station_count if self.station_count else 0
        )


class ChannelWaveform:
    """
    class for raw data waveform channels
    """

    def __init__(self, channel_number, amplifier_version, antenna, gain, values, start, bits, shift, conversion_gap,
                 conversion_time, waveform):
        self.channel_number = channel_number
        self.amplifier_version = amplifier_version
        self.antenna = antenna
        self.gain = gain
        self.values = values
        self.start = start
        self.bits = bits
        self.shift = shift
        self.conversion_gap = conversion_gap
        self.conversion_time = conversion_time
        self.waveform = waveform


class GridData:
    """ class for grid characteristics"""

    def __init__(self, grid, no_data=None):
        self.grid = grid
        self.no_data = no_data if no_data else GridElement(0, None)
        self.data = [[None for _ in range(grid.x_bin_count)] for _ in range(grid.y_bin_count)]

    def set(self, x_index, y_index, value):
        try:
            self.data[y_index][x_index] = value
        except IndexError:
            pass

    def get(self, x_index, y_index):
        return self.data[y_index][x_index]

    def to_arcgrid(self):
        result = 'NCOLS %d\n' % self.grid.x_bin_count
        result += 'NROWS %d\n' % self.grid.y_bin_count
        result += 'XLLCORNER %.4f\n' % self.grid.x_min
        result += 'YLLCORNER %.4f\n' % self.grid.y_min
        result += 'CELLSIZE %.4f\n' % self.grid.x_div
        result += 'NODATA_VALUE %s\n' % str(self.no_data.count)

        result += '\n'.join([' '.join([self.cell_to_multiplicity(cell) for cell in row]) for row in self.data[::-1]])

        return result

    @staticmethod
    def cell_to_multiplicity(current_cell):
        return str(current_cell.count) if current_cell else '0'

    def to_map(self):
        chars = " .-o*O8"

        matrix = self.data[::-1]
        maximum, total = self.max_and_total_entries(matrix)

        if maximum > len(chars):
            divider = float(maximum) / (len(chars) - 1)
        else:
            divider = 1

        result = (self.grid.x_bin_count + 2) * '-' + '\n'
        for row in self.data[::-1]:
            result += "|"
            for cell in row:
                index = self.cell_index(cell, divider)
                result += chars[index]
            result += "|\n"

        result += (self.grid.x_bin_count + 2) * '-' + '\n'
        result += 'total count: %d, max per area: %d' % (total, maximum)
        return result

    @staticmethod
    def cell_index(cell, divider):
        return int(math.floor((cell.count - 1) / divider + 1)) if cell else 0

    @staticmethod
    def max_and_total_entries(matrix):
        maximum = 0
        total = 0
        for row in matrix:
            for cell in row:
                if cell:
                    total += cell.count
                    if maximum < cell.count:
                        maximum = cell.count
        return maximum, total

    def to_reduced_array(self, reference_time):

        reduced_array = []

        for row_index, row in enumerate(self.data[::-1]):
            reduced_array += tuple((column_index, row_index,
                                    int(cell.count),
                                    -(reference_time - cell.timestamp).seconds) for column_index, cell in
                                   enumerate(row) if cell)

        return tuple(reduced_array)
