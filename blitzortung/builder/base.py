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



