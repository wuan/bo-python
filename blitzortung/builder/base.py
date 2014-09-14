# -*- coding: utf8 -*-

"""
Copyright (C) 2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import datetime

import pytz
import numpy as np
import pandas as pd

from .. import data, Error


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



