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

from __future__ import unicode_literals
from . import types
import math
import numpy
from shapely.geometry import mapping
from blitzortung.geom import GridElement


class Event(types.Point):
    time_format = '%Y-%m-%d %H:%M:%S'
    time_format_fractional_seconds = time_format + '.%f'

    __slots__ = ['__timestamp']

    def __init__(self, timestamp, x_coord_or_point, y_coord=None):
        super(Event, self).__init__(x_coord_or_point, y_coord)
        self.__timestamp = timestamp

    @property
    def timestamp(self):
        return self.__timestamp

    def difference_to(self, other):
        return other.timestamp - self.timestamp

    def ns_difference_to(self, other):
        return other.timestamp.value - self.timestamp.value

    def has_same_location(self, other):
        return super(Event, self).__eq__(other)

    @property
    def is_valid(self):
        return (self.x != 0.0 or self.y != 0.0) \
               and -180 <= self.x <= 180 \
               and -90 < self.y < 90 \
               and self.has_valid_timestamp

    @property
    def has_valid_timestamp(self):
        return self.timestamp is not None and self.timestamp.year > 1900

    def __lt__(self, other):
        return self.timestamp.value < other.timestamp.value

    def __le__(self, other):
        return self.timestamp.value <= other.timestamp.value

    def __str__(self):
        if self.has_valid_timestamp:
            timestamp_string = self.timestamp.strftime(self.time_format_fractional_seconds + 'XXX%z')
            timestamp_string = timestamp_string.replace('XXX', "%03d" % self.timestamp.nanosecond)
        else:
            timestamp_string = "NaT"

        return "%s %.4f %.4f" \
               % (timestamp_string, self.x, self.y)

    def __hash__(self):
        return super(Event, self).__hash__() ^ hash(self.timestamp)

    @property
    def uuid(self):
        return "%s-%05.0f-%05.0f" \
               % (str(self.timestamp().value), self.x * 100, self.y * 100)


class RawWaveformEvent(Event):
    def __init__(self, timestamp, x_coord, y_coord, altitude, channels):
        super(RawWaveformEvent, self).__init__(timestamp, x_coord, y_coord)
        self.altitude = altitude
        self.channels = channels

    def __str__(self):
        return super(RawWaveformEvent, self).__str__() + "%d %d chs" % (self.altitude, len(self.channels))

    def __repr__(self):
        return self.__str__()


class Station(Event):
    """
    class for station objects
    """

    __slots__ = ['number', 'user', 'name', 'country', 'status', 'board']

    def __init__(self, number, user, name, country, x_coord, y_coord, last_data, status, board):
        super(Station, self).__init__(last_data, x_coord, y_coord)
        self.number = number
        self.user = user
        self.name = name
        self.country = country
        self.status = status
        self.board = board

    def __str__(self):
        offline_since = self.timestamp
        status_char = "*" if offline_since is None else "-"
        status_text = "" if offline_since is None else " offline since " + offline_since.strftime("%Y-%m-%d %H:%M %Z")
        return u"%s%3d/%3d '%s' '%s' (%.4f, %.4f)%s" % (
            status_char, self.number, self.user, self.name, self.country, self.x, self.y, status_text)

    def __eq__(self, other):
        return self.number == other.number and self.name == other.name and self.country == other.country

    def __ne__(self, other):
        return not self == other

    @property
    def is_valid(self):
        return super(Station, self).is_valid \
               and self.number > 0

    @property
    def is_offline(self):
        return self.timestamp is not None


class StationOffline(object):
    """
    class for station offline information objects
    """

    def __init__(self, id_number, number, begin, end=None):
        self.id = id_number
        self.number = number
        self.begin = begin
        self.__end = end

    @property
    def end(self):
        """
        return end of offline time
        """
        return self.__end

    @end.setter
    def end(self, end):
        """
        set end of offline time
        """
        if not self.__end:
            self.__end = end
        else:
            raise ValueError('cannot overwrite end of StationOffline when already set')


class Strike(Event):
    """
    class for strike objects
    """

    __slots__ = ['id', 'altitude', 'amplitude', 'lateral_error', 'station_count', 'stations']

    def __init__(self, strike_id, timestamp, x_coord, y_coord, altitude, amplitude, lateral_error, station_count,
                 stations=None):
        super(Strike, self).__init__(timestamp, x_coord, y_coord)
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
        return super(Strike, self).__str__() + " %s %.1f %d %d" % (
            str(self.altitude) if self.altitude is not None else '-',
            self.amplitude if self.amplitude else 0.0,
            self.lateral_error,
            self.station_count if self.station_count else 0
        )


class StrikeCluster(object):
    """
    class for strike cluster objects
    """

    def __init__(self, cluster_id, timestamp, interval_seconds, shape, strike_count, area):
        self.id = cluster_id
        self.timestamp = timestamp
        self.interval_seconds = interval_seconds
        self.shape = shape
        self.strike_count = strike_count
        self.area = area

    def __str__(self):
        return "StrikeCluster({}, {}, {}, {}, {}, {:.1f})".format(self.id, self.timestamp,
                                                                  self.interval_seconds, mapping(self.shape),
                                                                  self.strike_count, self.area)


class ChannelWaveform(object):
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


class GridData(object):
    """ class for grid characteristics"""

    def __init__(self, grid, no_data=None):
        self.grid = grid
        self.no_data = no_data if no_data else GridElement(0, None)
        self.data = []
        self.clear()

    def clear(self):
        self.data = numpy.empty((self.grid.y_bin_count, self.grid.x_bin_count), dtype=type(self.no_data))

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
        result += 'NODATA_VALUE %s\n' % str(self.no_data)

        result += '\n'.join([' '.join([self.cell_to_multiplicity(cell) for cell in row]) for row in self.data[::-1]])

        return result

    @staticmethod
    def cell_to_multiplicity(current_cell):
        return str(current_cell.get_count()) if current_cell else '0'

    def to_map(self):
        chars = " .-o*O8"
        maximum = 0
        total = 0

        for row in self.data[::-1]:
            for cell in row:
                if cell:
                    total += cell.get_count()
                    if maximum < cell.get_count():
                        maximum = cell.get_count()

        if maximum > len(chars):
            divider = float(maximum) / (len(chars) - 1)
        else:
            divider = 1

        result = (self.grid.x_bin_count + 2) * '-' + '\n'
        for row in self.data[::-1]:
            result += "|"
            for cell in row:
                if cell:
                    index = int(math.floor((cell.count - 1) / divider + 1))
                else:
                    index = 0
                result += chars[index]
            result += "|\n"

        result += (self.grid.x_bin_count + 2) * '-' + '\n'
        result += 'total count: %d, max per area: %d' % (total, maximum)
        return result

    def to_reduced_array(self, reference_time):

        reduced_array = []

        for row_index, row in enumerate(self.data[::-1]):
            reduced_array += tuple((column_index, row_index,
                                    int(cell.count),
                                    -(reference_time - cell.timestamp).seconds) for column_index, cell in
                                   enumerate(row) if cell)

        return tuple(reduced_array)
