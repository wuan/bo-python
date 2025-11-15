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
import time

from .data import Timestamp, Timedelta


class Timer():
    """
    simple timer for repeated time measurements
    """

    def __init__(self):
        self.start_time = time.time()
        self.lap_time = self.start_time

    def read(self):
        """
        read time duration of current lap
        """
        return time.time() - self.start_time

    def lap(self):
        """
        start a new lap returning the total time of the currently active lap
        """
        now = time.time()
        lap = now - self.lap_time
        self.lap_time = now
        return lap


def total_seconds(time_value):
    """
    return the total seconds of the given time or datetime (relative to midnight)
    """

    if isinstance(time_value, datetime.datetime) or isinstance(time_value, Timestamp):
        return time_value.hour * 3600 + time_value.minute * 60 + time_value.second
    elif isinstance(time_value, datetime.timedelta) or isinstance(time_value, Timedelta):
        return time_value.seconds + time_value.days * 24 * 3600
    else:
        raise ValueError("unhandled type " + str(type(time_value)))


def round_time(time_value, duration):
    """
    round time to a given timedelta duration
    """
    duration_seconds = total_seconds(duration)
    seconds = (total_seconds(time_value) // duration_seconds) * duration_seconds

    updated_values = {
        'hour': seconds // 3600,
        'minute': seconds // 60 % 60,
        'second': seconds % 60,
        'microsecond': 0
    }

    if isinstance(time_value, Timestamp):
        updated_values['nanosecond'] = 0

    return time_value.replace(**updated_values)


def time_intervals(start_time, duration, end_time=None):
    """
    generator for time interval start times for a specified duration and end time

    if end time is not specified the current time is assumed
    """
    current_time = round_time(start_time, duration)
    if not end_time:
        end_time = datetime.datetime.now(datetime.timezone.utc)
    end_time = round_time(end_time, duration)

    while current_time <= end_time:
        yield current_time
        current_time += duration


def force_range(lower_limit, value, upper_limit):
    if value < lower_limit:
        return lower_limit
    elif value > upper_limit:
        return upper_limit
    else:
        return value

class TimeConstraint:

    def __init__(self, default_minute_length: int, max_minute_length: int):
        self.default_minute_length = default_minute_length
        self.max_minute_length = max_minute_length

    def enforce(self, minute_length, minute_offset):
        minute_length = force_range(0, minute_length, self.max_minute_length)
        minute_length = self.default_minute_length if minute_length == 0 else minute_length
        minute_offset = force_range(-self.max_minute_length + minute_length, minute_offset, 0)
        return minute_length, minute_offset
