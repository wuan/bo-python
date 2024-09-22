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

import math

import pyproj


class EqualityAndHash:
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return NotImplemented

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))


class Point:
    """
    Base class for Point like objects
    """

    __radians_factor = math.pi / 180

    __geod = pyproj.Geod(ellps='WGS84', units='m')

    __slots__ = ('x', 'y')

    def __init__(self, x_coord_or_point, y_coord=None):
        (self.x, self.y) = self.__get_point_coordinates(x_coord_or_point, y_coord)

    def distance_to(self, other):
        return self.geodesic_relation_to(other)[1]

    def azimuth_to(self, other):
        return self.geodesic_relation_to(other)[0]

    def geodesic_shift(self, azimuth, distance):
        result = self.__geod.fwd(self.x, self.y, azimuth / self.__radians_factor, distance, radians=False)
        return Point(result[0], result[1])

    def geodesic_relation_to(self, other):
        result = self.__geod.inv(self.x, self.y, other.x, other.y, radians=False)
        return result[0] * self.__radians_factor, result[2]

    @staticmethod
    def __get_point_coordinates(x_coord_or_point, y_coord):
        if isinstance(x_coord_or_point, Point):
            return x_coord_or_point.x, x_coord_or_point.y
        else:
            return x_coord_or_point, y_coord

    def __eq__(self, other):
        return self.equal(self.x, other.x) and self.equal(self.y, other.y)

    @staticmethod
    def equal(a, b):
        return abs(a - b) < 1e-4

    def __str__(self):
        return "(%.4f, %.4f)" % (self.x, self.y)

    def __hash__(self):
        return hash(self.x) ^ hash(self.y)
