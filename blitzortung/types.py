# -*- coding: utf8 -*-

"""

@author Andreas Würl

"""

import sys

import math
import pyproj


class Point(object):
    """
    Base class for Point like objects
    """

    __radians_factor = math.pi / 180

    __geod = pyproj.Geod(ellps='WGS84', units='m')

    def __init__(self, x_coord_or_point, y_coord=None):
        (self.x_coord, self.y_coord) = self.__get_point_coordinates(x_coord_or_point, y_coord)

    def get_x(self):
        """ returns x coordinate of point """
        return self.x_coord

    def get_y(self):
        """ returns y coordinate of point """
        return self.y_coord

    def distance_to(self, other):
        return self.geodesic_relation_to(other)[1]

    def azimuth_to(self, other):
        return self.geodesic_relation_to(other)[0]

    def geodesic_shift(self, azimuth, distance):
        result = self.__geod.fwd(self.x_coord, self.y_coord, azimuth / self.__radians_factor, distance, radians=False)
        return Point(result[0], result[1])

    def geodesic_relation_to(self, other):
        result = self.__geod.inv(self.x_coord, self.y_coord, other.x_coord, other.y_coord, radians=False)
        return result[0] * self.__radians_factor, result[2]

    @staticmethod
    def __get_point_coordinates(x_coord_or_point, y_coord):
        if isinstance(x_coord_or_point, Point):
            return x_coord_or_point.get_x(), x_coord_or_point.get_y()
        else:
            return x_coord_or_point, y_coord

    def __eq__(self, other):
        return self.equal(self.x_coord, other.x_coord) and self.equal(self.y_coord, other.y_coord)

    @staticmethod
    def equal(a, b):
        return abs(a - b) < 1e-4

    def __str__(self):
        return "(%.4f, %.4f)" % (self.x_coord, self.y_coord)

    def __hash__(self):
        return hash(self.x_coord) ^ hash(self.y_coord)
