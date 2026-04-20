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

import math
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any

import pyproj
import shapely.geometry

if TYPE_CHECKING:
    from blitzortung.data import Timestamp


class Geometry:
    """
    abstract base class for geometries
    """

    __metaclass__ = ABCMeta

    __slots__ = ('srid',)

    default_srid = 4326

    srid: int

    def __init__(self, srid: int | None = None) -> None:
        self.srid = srid if srid is not None else Geometry.default_srid

    def get_srid(self) -> int:
        return self.srid

    def set_srid(self, srid: int) -> None:
        self.srid = srid

    @property
    @abstractmethod
    def env(self) -> shapely.geometry.Polygon:
        pass


class Envelope(Geometry):
    """
    definition of a coordinate envelope
    """

    __slots__ = ('x_min', 'x_max', 'y_min', 'y_max')

    x_min: float
    x_max: float
    y_min: float
    y_max: float

    def __init__(self, x_min: float, x_max: float, y_min: float, y_max: float, srid: int = Geometry.default_srid) -> None:
        super().__init__(srid)
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max

    @property
    def y_delta(self) -> float:
        return abs(self.y_max - self.y_min)

    @property
    def x_delta(self) -> float:
        return abs(self.x_max - self.x_min)

    def contains(self, point: object) -> bool:
        if not hasattr(point, 'x') or not hasattr(point, 'y'):
            return False
        return (self.x_min <= point.x <= self.x_max) and \
               (self.y_min <= point.y <= self.y_max)  # type: ignore[return-value]

    @property
    def env(self) -> shapely.geometry.Polygon:
        return shapely.geometry.Polygon(
            [(self.x_min, self.y_min), (self.x_min, self.y_max), (self.x_max, self.y_max),
             (self.x_max, self.y_min), (self.x_min, self.y_min)])

    def __repr__(self) -> str:
        return 'Envelope(x: %.4f..%.4f, y: %.4f..%.4f)' % (
            self.x_min, self.x_max, self.y_min, self.y_max)


class Grid(Envelope):
    """ grid characteristics"""

    __slots__ = ['x_div', 'y_div', '_Grid__x_bin_count', '_Grid__y_bin_count']

    x_div: float
    y_div: float
    _Grid__x_bin_count: int | None
    _Grid__y_bin_count: int | None

    def __init__(
        self,
        x_min: float,
        x_max: float,
        y_min: float,
        y_max: float,
        x_div: float,
        y_div: float,
        srid: int = Geometry.default_srid,
    ) -> None:
        super().__init__(x_min, x_max, y_min, y_max, srid)
        self.x_div = x_div
        self.y_div = y_div
        self._Grid__x_bin_count = None
        self._Grid__y_bin_count = None

    def get_x_bin(self, x_pos: float) -> int:
        return int(math.ceil(float(x_pos - self.x_min) / self.x_div)) - 1

    def get_y_bin(self, y_pos: float) -> int:
        return int(math.ceil(float(y_pos - self.y_min) / self.y_div)) - 1

    @property
    def x_bin_count(self) -> int:
        if not self._Grid__x_bin_count:
            self._Grid__x_bin_count = self.get_x_bin(self.x_max) + 1
        return self._Grid__x_bin_count

    @property
    def y_bin_count(self) -> int:
        if not self._Grid__y_bin_count:
            self._Grid__y_bin_count = self.get_y_bin(self.y_max) + 1
        return self._Grid__y_bin_count

    def get_x_center(self, cell_index: int) -> float:
        return self.x_min + (cell_index + 0.5) * self.x_div

    def get_y_center(self, row_index: int) -> float:
        return self.y_min + (row_index + 0.5) * self.y_div

    def __repr__(self) -> str:
        return 'Grid(x: %.4f..%.4f (%.4f, #%d), y: %.4f..%.4f (%.4f, #%d))' % (
            self.x_min, self.x_max, self.x_div, self.x_bin_count,
            self.y_min, self.y_max, self.y_div, self.y_bin_count)


class GridFactory:
    WGS84 = pyproj.CRS(f"epsg:{Geometry.default_srid}")

    __slots__ = ['min_lon', 'max_lon', 'max_lat', 'min_lat', 'coord_sys', 'ref_lon', 'ref_lat', 'grid_data']

    min_lon: float
    max_lon: float
    min_lat: float
    max_lat: float
    coord_sys: pyproj.CRS
    ref_lon: float | None
    ref_lat: float | None
    grid_data: dict[float, Grid]

    def __init__(
        self,
        min_lon: float,
        max_lon: float,
        min_lat: float,
        max_lat: float,
        coord_sys: pyproj.CRS,
        ref_lon: float | None = None,
        ref_lat: float | None = None,
    ) -> None:
        self.min_lon = max(-180.0, min_lon)
        self.max_lon = min(180.0, max_lon)
        self.min_lat = max(-90.0, min_lat)
        self.max_lat = min(90.0, max_lat)
        self.coord_sys = coord_sys
        self.ref_lon = ref_lon
        self.ref_lat = ref_lat

        self.grid_data = {}

    @staticmethod
    def fix_max(minimum: float, maximum: float, delta: float) -> float:
        return minimum + math.floor((maximum - minimum) / delta) * delta

    def get_for(self, base_length: float) -> Grid:
        if base_length not in self.grid_data:
            ref_lon = self.ref_lon if self.ref_lon else (self.min_lon + self.max_lon) / 2.0
            ref_lat = self.ref_lat if self.ref_lat else (self.min_lat + self.max_lat) / 2.0

            utm_x, utm_y = pyproj.Transformer.from_crs(self.WGS84, self.coord_sys) \
                .transform(ref_lat, ref_lon)
            lat_d, lon_d = pyproj.Transformer.from_crs(self.coord_sys, self.WGS84) \
                .transform(utm_x + base_length, utm_y + base_length)

            delta_lon = lon_d - ref_lon
            delta_lat = lat_d - ref_lat

            max_lon = self.fix_max(self.min_lon, self.max_lon, delta_lon)
            max_lat = self.fix_max(self.min_lat, self.max_lat, delta_lat)

            self.grid_data[base_length] = Grid(self.min_lon, max_lon, self.min_lat, max_lat,
                                               delta_lon, delta_lat,
                                               Geometry.default_srid)

        return self.grid_data[base_length]


class GridElement:
    """
    raster data entry
    """

    __slots__ = ['count', 'timestamp']

    count: int
    timestamp: Timestamp | None

    def __init__(self, count: int, timestamp: Timestamp | None) -> None:
        self.count = count
        self.timestamp = timestamp

    def __gt__(self, other: GridElement) -> bool:
        return self.count > other.count

    def __repr__(self) -> str:
        return "GridElement(%d, %s)" % (self.count, str(self.timestamp))
