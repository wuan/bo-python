# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import math
import pyproj


class Point(object):
    """
    Base class for Point like objects
    """

    __radians_factor = math.pi / 180

    __geod = pyproj.Geod(ellps='WGS84', units='m')

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
