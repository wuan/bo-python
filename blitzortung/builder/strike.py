# -*- coding: utf8 -*-

"""
Copyright (C) 2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import re
from .base import Event, BuilderError
from ..util import force_range
from .. import data


class Strike(Event):
    """
    class for building strike objects
    """
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