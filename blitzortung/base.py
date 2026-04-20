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

import math
from typing import Any, Tuple, Union

import pyproj


class EqualityAndHash:
    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False

    def __ne__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return True

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.__dict__.items())))


class Point:
    """
    Base class for Point like objects
    """

    __radians_factor = math.pi / 180

    __geod = pyproj.Geod(ellps='WGS84', units='m')

    __slots__ = ('x', 'y')

    x: float
    y: float

    def __init__(self, x_coord_or_point: Union[float, Point], y_coord: float | None = None) -> None:
        (self.x, self.y) = self.__get_point_coordinates(x_coord_or_point, y_coord)

    def distance_to(self, other: Point) -> float:
        return self.geodesic_relation_to(other)[1]

    def azimuth_to(self, other: Point) -> float:
        return self.geodesic_relation_to(other)[0]

    def geodesic_shift(self, azimuth: float, distance: float) -> Point:
        result = self.__geod.fwd(self.x, self.y, azimuth / self.__radians_factor, distance, radians=False)
        return Point(result[0], result[1])

    def geodesic_relation_to(self, other: Point) -> Tuple[float, float]:
        result = self.__geod.inv(self.x, self.y, other.x, other.y, radians=False)
        return result[0] * self.__radians_factor, result[2]

    @staticmethod
    def __get_point_coordinates(x_coord_or_point: Union[float, Point], y_coord: float | None) -> Tuple[float, float]:
        if isinstance(x_coord_or_point, Point):
            return x_coord_or_point.x, x_coord_or_point.y
        else:
            return x_coord_or_point, y_coord if y_coord is not None else 0.0

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Point):
            return False
        return self.equal(self.x, other.x) and self.equal(self.y, other.y)

    @staticmethod
    def equal(a: float, b: float) -> bool:
        return abs(a - b) < 1e-4

    def __str__(self) -> str:
        return "(%.4f, %.4f)" % (self.x, self.y)

    def __hash__(self) -> int:
        return hash(self.x) ^ hash(self.y)
