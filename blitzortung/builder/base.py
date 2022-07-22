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

from .. import data, Error


class BuilderError(Error):
    pass


class Base:
    pass


class Timestamp(Base):
    def __init__(self):
        super().__init__()
        self.timestamp = None

    def set_timestamp(self, timestamp, nanosecond=0):
        if not timestamp:
            self.timestamp = None
        elif isinstance(timestamp, data.Timestamp):
            self.timestamp = timestamp + nanosecond
        else:
            self.timestamp = data.Timestamp(timestamp, nanosecond=nanosecond)
        return self

    def build(self):
        return self.timestamp


class Event(Timestamp):
    def __init__(self):
        super().__init__()
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
