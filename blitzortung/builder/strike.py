# -*- coding: utf8 -*-

"""

   Copyright 2014-2025 Andreas Würl

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

import re

from .base import Event, BuilderError
from .. import data
from ..data import Timestamp
from ..util import force_range


class Strike(Event):
    """
    class for building strike objects
    """
    position_parser = re.compile(r'pos;([-0-9.]+);([-0-9.]+);([-0-9.]+)')
    amplitude_parser = re.compile(r'str;([0-9.]+)')
    deviation_parser = re.compile(r'dev;([0-9.]+)')
    stations_parser = re.compile(r'sta;(\d+);(\d+);([^ ]*)')

    def __init__(self):
        super().__init__()
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
        self.lateral_error = force_range(0, lateral_error, 32767) if lateral_error is not None else None
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

    def from_json(self, json_data: dict):
        """ Construct strike from json data """
        try:
            self.set_altitude(json_data['alt'])
            self.set_x(round(json_data['lon'], 4))
            self.set_y(round(json_data['lat'], 4))
            self.set_timestamp(Timestamp(json_data['time']))
            self.set_lateral_error(json_data['mds'])

            self.set_altitude(0)
            self.set_amplitude(0)
            self.set_station_count(0)
        except (KeyError, ValueError, IndexError) as e:
            raise BuilderError(e)

        return self

    def build(self):
        return data.Strike(self.id_value, self.timestamp, self.x_coord, self.y_coord, self.altitude,
                           self.amplitude, self.lateral_error, self.station_count, self.stations)
