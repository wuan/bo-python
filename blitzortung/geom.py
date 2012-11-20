# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

import re
import pyproj
import numpy
import math
import shapely

from abc import ABCMeta, abstractmethod

class Geometry(object):
    '''
    abstract base class for geometries
    '''

    __metaclass__ = ABCMeta

    DefaultSrid = 4326

    def __init__(self, srid = DefaultSrid):
        self.srid = srid

    def get_srid(self):
        return self.srid

    def set_srid(self, srid):
        self.srid = srid

    @abstractmethod
    def get_env(self):
        pass


class Envelope(Geometry):
    ''' class for definition of coordinate envelopes '''

    def __init__(self, xmin, xmax, ymin, ymax, srid=Geometry.DefaultSrid):
        Geometry.__init__(self, srid)
        self.xmin = xmin
        self.xmax = xmax
        self.ymin = ymin
        self.ymax = ymax

    def get_x_min(self):
        return self.xmin

    def get_x_max(self):
        return self.xmax

    def get_y_min(self):
        return self.ymin

    def get_y_max(self):
        return self.ymax

    def get_y_delta(self):
        return abs(self.ymax - self.ymin)

    def get_x_delta(self):
        return abs(self.xmax - self.xmin)

    def contains(self, point):
        if ((point.getX() >= self.xmin) and
            (point.getX() <= self.xmax) and
            (point.getY() >= self.ymin) and
            (point.getY() <= self.ymax)):
            return True
        else:
            return False

    def get_env(self):
        return shapely.geometry.Polygon(((self.xmin, self.ymin), (self.xmin, self.ymax), (self.xmax, self.ymax), (self.xmax, self.ymin)))

    def __str__(self):
        return 'longitude: %.2f .. %.2f, latitude: %.2f .. %.2f' % (self.xmin, self.xmax, self.ymin, self.ymax)


class Raster(Envelope):
    ' class for raster characteristics and data '

    def __init__(self, xmin, xmax, ymin, ymax, xdiv, ydiv, srid=Geometry.DefaultSrid, nodata = None):
        Envelope.__init__(self, xmin, xmax, ymin, ymax, srid)
        self.xdiv = xdiv
        self.ydiv = ydiv
        self.nodata = nodata if nodata else RasterElement(0, None)
	self.clear()

    def clear(self):
        self.data = numpy.empty((self.get_y_bin_count(), self.get_x_bin_count()), dtype=type(self.nodata))

    def get_x_div(self):
        return self.xdiv

    def get_y_div(self):
        return self.ydiv

    def get_x_bin_count(self):
        return int(math.ceil(1.0 * (self.xmax - self.xmin) / self.xdiv))

    def get_y_bin_count(self):
        return int(math.ceil(1.0 * (self.ymax - self.ymin) / self.ydiv))

    def get_x_center(self, cellindex):
        return self.xmin + (cellindex + 0.5) * self.xdiv

    def get_y_center(self, rowindex):
        return self.ymin + (rowindex + 0.5) * self.ydiv

    def set(self, x_index, y_index, value):
        try:
          self.data[y_index][x_index] = value
        except IndexError:
          pass

    def get(self, x_index, y_index):
        return self.data[y_index][x_index]

    def get_nodata_value(self):
        return self.nodata

    def to_arcgrid(self):
        result  = 'NCOLS %d\n' % self.get_x_bin_count()
        result += 'NROWS %d\n' % self.get_y_bin_count()
        result += 'XLLCORNER %.4f\n' % self.get_x_min()
        result += 'YLLCORNER %.4f\n' % self.get_y_min()
        result += 'CELLSIZE %.4f\n' % self.get_x_div()
        result += 'NODATA_VALUE %s\n' % str(self.get_nodata_value())

        for row in self.data[::-1]:
            for cell in row:
                result += str(cell.get_count() if cell else 0) + ' '
            result += '\n'

        return result.strip()

    def to_map(self):
        chars = " .-o*O8"
        maximum = 0
        total = 0

        for row in self.data[::-1]:
            for cell in row:
                if cell:
                    total += cell.get_count()
                    if maximum < cell.get_count():
                        maximum = cell.get_count()

        if maximum > len(chars):
            divider = float(maximum) / (len(chars) - 1)
        else:
            divider = 1

        result = (self.get_x_bin_count() + 2) * '-' + '\n'
        for row in self.data[::-1]:
            result += "|"
            for cell in row:
                if cell:
                    index = int(math.floor((cell.get_count() - 1) / divider + 1))
                else:
                    index = 0
                result += chars[index]
            result += "|\n"

        result += (self.get_x_bin_count() + 2) * '-' + '\n'
        result += 'total count: %d, max per area: %d' % (total, maximum)
        return result

    def to_reduced_array(self, reference_time):

        reduced_array = []

        rowindex = 0
        for row in self.data[::-1]:
            cellindex = 0
            for cell in row:
                if cell:
                    reduced_array.append([cellindex, rowindex,
                                          int(cell.get_count()),
                                          -((reference_time - cell.get_timestamp()).seconds)])
                cellindex += 1
            rowindex += 1

        return reduced_array

class RasterElement(object):

    def __init__(self, count, timestamp):
        self.count = count
        self.timestamp = timestamp

    def __gt__(self, other):
        return self.count > other

    def __str__(self):
        return "RasterElement(%d, %s)" % (self.count, str(self.timestamp))

    def get_count(self):
        return self.count

    def get_timestamp(self):
        return self.timestamp
