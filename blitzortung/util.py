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

from __future__ import annotations

import datetime
import time
from typing import Generator, TypeVar, Union

from .data import Timestamp, Timedelta

T = TypeVar('T', int, float)


class Timer:
    """
    simple timer for repeated time measurements
    """

    start_time: float
    lap_time: float

    def __init__(self) -> None:
        self.start_time = time.time()
        self.lap_time = self.start_time

    def read(self) -> float:
        """
        read time duration of current lap
        """
        return time.time() - self.start_time

    def lap(self) -> float:
        """
        start a new lap returning the total time of the currently active lap
        """
        now = time.time()
        lap = now - self.lap_time
        self.lap_time = now
        return lap


def total_seconds(time_value: datetime.datetime | Timestamp | datetime.timedelta | Timedelta) -> int:
    """
    return the total seconds of the given time or datetime (relative to midnight)
    """

    if isinstance(time_value, datetime.datetime) or isinstance(time_value, Timestamp):
        return time_value.hour * 3600 + time_value.minute * 60 + time_value.second
    elif isinstance(time_value, datetime.timedelta) or isinstance(time_value, Timedelta):
        return time_value.seconds + time_value.days * 24 * 3600
    else:
        raise ValueError("unhandled type " + str(type(time_value)))


def round_time(time_value: datetime.datetime | Timestamp, duration: datetime.timedelta | Timedelta) -> datetime.datetime | Timestamp:
    """
    round time to a given timedelta duration
    """
    duration_seconds = total_seconds(duration)
    seconds = (total_seconds(time_value) // duration_seconds) * duration_seconds

    updated_values: dict[str, int] = {
        'hour': int(seconds // 3600),
        'minute': int(seconds // 60 % 60),
        'second': int(seconds % 60),
        'microsecond': 0
    }

    if isinstance(time_value, Timestamp):
        updated_values['nanosecond'] = 0

    return time_value.replace(**updated_values)  # type: ignore[return-value,arg-type]


def time_intervals(
    start_time: datetime.datetime | Timestamp,
    duration: datetime.timedelta | Timedelta,
    end_time: datetime.datetime | Timestamp | None = None,
) -> Generator[datetime.datetime | Timestamp, None, None]:
    """
    generator for time interval start times for a specified duration and end time

    if end time is not specified the current time is assumed
    """
    current_time: datetime.datetime | Timestamp = round_time(start_time, duration)
    if not end_time:
        end_time = datetime.datetime.now(datetime.timezone.utc)
    end_time = round_time(end_time, duration)

    while current_time <= end_time:
        yield current_time
        current_time = current_time + duration  # type: ignore[assignment,operator]


def force_range(lower_limit: T, value: T, upper_limit: T) -> T:
    if value < lower_limit:
        return lower_limit
    elif value > upper_limit:
        return upper_limit
    else:
        return value

class TimeConstraint:

    default_minute_length: int
    max_minute_length: int

    def __init__(self, default_minute_length: int, max_minute_length: int) -> None:
        self.default_minute_length = default_minute_length
        self.max_minute_length = max_minute_length

    def enforce(self, minute_length: int, minute_offset: int) -> tuple[int, int]:
        minute_length = force_range(0, minute_length, self.max_minute_length)
        minute_length = self.default_minute_length if minute_length == 0 else minute_length
        minute_offset = force_range(-self.max_minute_length + minute_length, minute_offset, 0)
        return minute_length, minute_offset
