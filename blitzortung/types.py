# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

import datetime, pytz
import pyproj

class Point(object):
    '''
    Base class for Point like objects
    '''

    __geod = pyproj.Geod(ellps='WGS84', units='m')

    def __init__(self, x_coord, y_coord):
        self.x_coord = x_coord
        self.y_coord = y_coord

    def __invgeod(self, other):
        return Point.__geod.inv(self.x_coord, self.y_coord, other.x_coord, other.y_coord)

    def set_x(self, x_coord):
        self.x_coord = x_coord
        
    def get_x(self):
        ' returns x coordinate of point '
        return self.x_coord

    def set_y(self, y_coord):
        self.y_coord = y_coord
        
    def get_y(self):
        ' returns y coordinate of point '
        return self.y_coord

    def distance_to(self, other):
        return self.__invgeod(other)[2]

    def azimuth_to(self, other):
        return self.__invgeod(other)[0]

    def geodesic_relation_to(self, other):
        result = self.__invgeod(other)
        return result[2], result[0]

    def __str__(self):
        return "(%.4f, %.4f)" %(self.x_coord, self.y_coord)
