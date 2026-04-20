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

from __future__ import annotations

import datetime as dt_module
import math
from typing import Any

from . import base
from .geom import Grid, GridElement


class Timestamp(base.EqualityAndHash):
    timestamp_string_minimal_fractional_seconds_length = 20
    timestamp_string_microseconds_length = 26

    __slots__ = ['_datetime', 'nanosecond']

    _datetime: dt_module.datetime
    nanosecond: int

    def __init__(
        self,
        date_time: dt_module.datetime | str | int | None = ...,  # type: ignore[assignment]
        nanosecond: int = 0,
    ) -> None:
        # Use a sentinel to detect if no argument was provided
        if date_time is ...:
            dt: dt_module.datetime = dt_module.datetime.now(dt_module.timezone.utc)
        elif date_time is None:
            dt = None  # type: ignore[assignment]
        elif isinstance(date_time, str):
            parsed_dt, date_time_nanosecond = Timestamp.from_timestamp(date_time)
            if parsed_dt is not None:
                dt = parsed_dt
            else:
                dt = None  # type: ignore[assignment]
            nanosecond += date_time_nanosecond
        elif isinstance(date_time, int):
            dt, date_time_nanosecond = Timestamp.from_nanoseconds(date_time)
            nanosecond += date_time_nanosecond
        else:
            dt = date_time

        if dt is not None and (nanosecond < 0 or nanosecond > 999):
            microdelta = nanosecond // 1000
            dt += dt_module.timedelta(microseconds=microdelta)
            nanosecond -= microdelta * 1000

        self._datetime = dt
        self.nanosecond = nanosecond

    @staticmethod
    def from_timestamp(timestamp_string: str) -> tuple[dt_module.datetime | None, int]:
        try:
            if len(timestamp_string) > Timestamp.timestamp_string_minimal_fractional_seconds_length:
                divider_index = Timestamp.timestamp_string_microseconds_length
                date_time = dt_module.datetime.strptime(timestamp_string[:divider_index], '%Y-%m-%d %H:%M:%S.%f')
                nanosecond_string = timestamp_string[divider_index:]
                nanosecond = int(
                    float(nanosecond_string) * math.pow(10, 3 - len(nanosecond_string))) if nanosecond_string else 0
            else:
                date_time = dt_module.datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S')
                nanosecond = 0

            return date_time.replace(tzinfo=dt_module.timezone.utc), nanosecond
        except ValueError:
            return None, 0

    @staticmethod
    def from_nanoseconds(total_nanoseconds: int) -> tuple[dt_module.datetime, int]:
        total_microseconds = total_nanoseconds // 1000
        residual_nanoseconds = total_nanoseconds % 1000
        total_seconds = total_microseconds // 1000000
        residual_microseconds = total_microseconds % 1000000
        return dt_module.datetime(1970, 1, 1, tzinfo=dt_module.timezone.utc) + \
            dt_module.timedelta(seconds=total_seconds,
                               microseconds=residual_microseconds), \
            residual_nanoseconds

    @property
    def year(self) -> int:
        return self._datetime.year

    @property
    def month(self) -> int:
        return self._datetime.month

    @property
    def day(self) -> int:
        return self._datetime.day

    @property
    def hour(self) -> int:
        return self._datetime.hour

    @property
    def minute(self) -> int:
        return self._datetime.minute

    @property
    def second(self) -> int:
        return self._datetime.second

    @property
    def microsecond(self) -> int:
        return self._datetime.microsecond

    @property
    def tzinfo(self) -> dt_module.tzinfo | None:
        return self._datetime.tzinfo

    @property
    def datetime(self) -> dt_module.datetime:
        return self._datetime

    epoch: dt_module.datetime = dt_module.datetime.fromtimestamp(0, dt_module.timezone.utc)

    @property
    def value(self) -> int:
        total_microseconds = int((self.datetime - self.epoch).total_seconds() * 1000000)
        return (total_microseconds * 1000 + self.nanosecond) if self.datetime is not None else -1

    @property
    def is_valid(self) -> bool:
        return self.datetime is not None and self.datetime.year > 1900

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, Timestamp):
            return True
        return self.datetime != other.datetime or self.nanosecond != other.nanosecond

    def __lt__(self, other: object) -> bool:
        if isinstance(other, dt_module.datetime):
            return self.datetime < other
        elif isinstance(other, Timestamp):
            return self.datetime < other.datetime or (
                    self.datetime == other.datetime and self.nanosecond < other.nanosecond)
        return False

    def __le__(self, other: object) -> bool:
        if isinstance(other, dt_module.datetime):
            return self.datetime <= other
        elif isinstance(other, Timestamp):
            return self.datetime < other.datetime or (
                    self.datetime == other.datetime and self.nanosecond <= other.nanosecond)
        return False

    def __gt__(self, other: object) -> bool:
        if isinstance(other, dt_module.datetime):
            return self.datetime > other
        elif isinstance(other, Timestamp):
            return self.datetime > other.datetime or (
                    self.datetime == other.datetime and self.nanosecond > other.nanosecond)
        return False

    def __ge__(self, other: object) -> bool:
        if isinstance(other, dt_module.datetime):
            return self.datetime >= other
        elif isinstance(other, Timestamp):
            return self.datetime > other.datetime or (
                    self.datetime == other.datetime and self.nanosecond >= other.nanosecond)
        return False

    def __add__(self, other: object) -> Timestamp:
        if isinstance(other, Timedelta):
            return Timestamp(self.datetime + other.timedelta, self.nanosecond + other.nanodelta)
        elif isinstance(other, dt_module.timedelta):
            return Timestamp(self.datetime + other, self.nanosecond)
        elif isinstance(other, int):
            return Timestamp(self.datetime, self.nanosecond + other)
        raise TypeError(f"unsupported operand type(s) for +: 'Timestamp' and '{type(other).__name__}'")

    def __sub__(self, other: object) -> Timedelta | Timestamp:
        if isinstance(other, Timestamp):
            return Timedelta(self.datetime - other.datetime, self.nanosecond - other.nanosecond)
        elif isinstance(other, dt_module.timedelta):
            return Timestamp(self.datetime - other, self.nanosecond)
        elif isinstance(other, int):
            return Timestamp(self.datetime, self.nanosecond - other)
        raise TypeError(f"unsupported operand type(s) for -: 'Timestamp' and '{type(other).__name__}'")

    def strftime(self, datetime_format: str) -> str:
        return self.datetime.strftime(datetime_format)

    def replace(self, **kwargs: Any) -> Timestamp:
        if 'nanosecond' in kwargs:
            nanosecond = kwargs['nanosecond']
            del kwargs['nanosecond']
        else:
            nanosecond = self.nanosecond
        return Timestamp(self._datetime.replace(**kwargs), nanosecond)

    def __repr__(self) -> str:
        return "Timestamp({}{:03d})".format(self._datetime.strftime("%Y-%m-%d %H:%M:%S.%f"), self.nanosecond)


NaT = Timestamp(None)


class Timedelta(base.EqualityAndHash):
    def __init__(self, timedelta: dt_module.timedelta = dt_module.timedelta(), nanodelta: int = 0) -> None:
        if nanodelta < 0 or nanodelta > 999:
            microdelta = nanodelta // 1000
            timedelta += dt_module.timedelta(microseconds=microdelta)
            nanodelta -= microdelta * 1000
        self.timedelta = timedelta
        self.nanodelta = nanodelta

    @property
    def days(self) -> int:
        return self.timedelta.days

    @property
    def seconds(self) -> int:
        return self.timedelta.seconds

    def __repr__(self) -> str:
        return "Timedelta({}, {})".format(self.timedelta, self.nanodelta)


class Event(base.Point):
    time_format = '%Y-%m-%d %H:%M:%S'
    time_format_fractional_seconds = time_format + '.%f'

    __slots__ = ['_timestamp']

    _timestamp: Timestamp

    def __init__(self, timestamp: Timestamp, x_coord_or_point: float | base.Point, y_coord: float | None = None) -> None:
        super().__init__(x_coord_or_point, y_coord)
        self._timestamp = timestamp

    @property
    def timestamp(self) -> Timestamp:
        return self._timestamp

    def difference_to(self, other: Event) -> Timedelta | Timestamp:
        result = other.timestamp - self.timestamp
        if isinstance(result, (Timedelta, Timestamp)):
            return result
        raise TypeError("Unexpected result type from timestamp subtraction")

    def ns_difference_to(self, other: Event) -> int:
        value1 = other.timestamp.value
        value2 = self.timestamp.value
        difference = value1 - value2
        return difference

    def has_same_location(self, other: object) -> bool:
        return super().__eq__(other)

    @property
    def is_valid(self) -> bool:
        return (not math.isclose(self.x, 0.0, rel_tol=1e-09, abs_tol=1e-09) or not math.isclose(self.y, 0.0, rel_tol=1e-09)) \
            and -180 <= self.x <= 180 \
            and -90 < self.y < 90 \
            and self.has_valid_timestamp

    @property
    def has_valid_timestamp(self) -> bool:
        return self.timestamp is not None and self.timestamp.is_valid

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Event):
            return False
        return self.timestamp.value < other.timestamp.value

    def __le__(self, other: object) -> bool:
        if not isinstance(other, Event):
            return False
        return self.timestamp.value <= other.timestamp.value

    def __str__(self) -> str:
        if self.has_valid_timestamp:
            timestamp_string = self.timestamp.strftime(self.time_format_fractional_seconds + 'XXX')
            timestamp_string = timestamp_string.replace('XXX', "%03d" % self.timestamp.nanosecond)
        else:
            timestamp_string = "NaT"

        return "%s %.4f %.4f" \
            % (timestamp_string, self.x, self.y)

    @property
    def uuid(self) -> str:
        return "%s-%05.0f-%05.0f" \
            % (str(self.timestamp.value), self.x * 100, self.y * 100)


class Strike(Event):
    """
    class for strike objects
    """

    __slots__ = ['id', 'altitude', 'amplitude', 'lateral_error', 'station_count', 'stations']

    id: int | None
    altitude: float | None
    amplitude: float | None
    lateral_error: int | None
    station_count: int | None
    stations: list[int]

    def __init__(
        self,
        strike_id: int | None,
        timestamp: Timestamp,
        x_coord: float,
        y_coord: float,
        altitude: float | None,
        amplitude: float | None,
        lateral_error: int | None,
        station_count: int | None,
        stations: list[int] | None = None,
    ) -> None:
        super().__init__(timestamp, x_coord, y_coord)
        self.id = strike_id
        self.altitude = altitude
        self.amplitude = amplitude
        self.lateral_error = lateral_error
        self.station_count = station_count
        self.stations = [] if stations is None else stations

    def has_participant(self, participant: int) -> bool:
        """
        returns true if the given participant is contained in the stations list
        """
        return participant in self.stations

    def __str__(self) -> str:
        return super().__str__() + " %s %.1f %d %d" % (
            str(self.altitude) if self.altitude is not None else '-',
            self.amplitude if self.amplitude else 0.0,
            self.lateral_error if self.lateral_error else 0,
            self.station_count if self.station_count else 0
        )


class ChannelWaveform:
    """
    class for raw data waveform channels
    """

    channel_number: int
    amplifier_version: str
    antenna: int
    gain: float
    values: list[float]
    start: float
    bits: int
    shift: int
    conversion_gap: float
    conversion_time: float
    waveform: list[float]

    def __init__(
        self,
        channel_number: int,
        amplifier_version: str,
        antenna: int,
        gain: float,
        values: list[float],
        start: float,
        bits: int,
        shift: int,
        conversion_gap: float,
        conversion_time: float,
        waveform: list[float],
    ) -> None:
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

    grid: Grid
    no_data: GridElement
    data: list[list[GridElement | None]]

    def __init__(self, grid: Grid, no_data: GridElement | None = None) -> None:
        self.grid = grid
        self.no_data = no_data if no_data else GridElement(0, None)
        self.data = [[None for _ in range(grid.x_bin_count)] for _ in range(grid.y_bin_count)]

    def set(self, x_index: int, y_index: int, value: GridElement | None) -> None:
        try:
            self.data[y_index][x_index] = value
        except IndexError:
            pass

    def get(self, x_index: int, y_index: int) -> GridElement | None:
        return self.data[y_index][x_index]

    def to_arcgrid(self) -> str:
        result = 'NCOLS %d\n' % self.grid.x_bin_count  # type: ignore[attr-defined]
        result += 'NROWS %d\n' % self.grid.y_bin_count  # type: ignore[attr-defined]
        result += 'XLLCORNER %.4f\n' % self.grid.x_min  # type: ignore[attr-defined]
        result += 'YLLCORNER %.4f\n' % self.grid.y_min  # type: ignore[attr-defined]
        result += 'CELLSIZE %.4f\n' % self.grid.x_div  # type: ignore[attr-defined]
        result += 'NODATA_VALUE %s\n' % str(self.no_data.count)

        result += '\n'.join([' '.join([self.cell_to_multiplicity(cell) for cell in row]) for row in self.data[::-1]])

        return result

    @staticmethod
    def cell_to_multiplicity(current_cell: GridElement | None) -> str:
        return str(current_cell.count) if current_cell else '0'

    def to_map(self) -> str:
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
    def cell_index(cell: GridElement | None, divider: float) -> int:
        return int(math.floor((cell.count - 1) / divider + 1)) if cell else 0

    @staticmethod
    def max_and_total_entries(matrix: list[list[GridElement | None]]) -> tuple[int, int]:
        maximum = 0
        total = 0
        for row in matrix:
            for cell in row:
                if cell:
                    total += cell.count
                    if maximum < cell.count:
                        maximum = cell.count
        return maximum, total

    def to_reduced_array(self, reference_time: Timestamp) -> tuple[tuple[int, int, int, int], ...]:

        reduced_array: list[tuple[int, int, int, int]] = []

        ref_ts = reference_time if isinstance(reference_time, Timestamp) else Timestamp(reference_time)

        for row_index, row in enumerate(self.data[::-1]):
            for column_index, cell in enumerate(row):
                if cell and cell.timestamp is not None:
                    # check if cell.timestamp is a Timestamp object or a datetime.datetime object
                    # in some tests, it might be a datetime.datetime object
                    ts = cell.timestamp if isinstance(cell.timestamp, Timestamp) else Timestamp(cell.timestamp)

                    if ts.is_valid:
                        time_diff = ref_ts - ts
                        if isinstance(time_diff, Timedelta):
                            reduced_array.append((
                                column_index,
                                row_index,
                                int(cell.count),
                                -time_diff.seconds,
                            ))

        return tuple(reduced_array)  # type: ignore[return-value]
