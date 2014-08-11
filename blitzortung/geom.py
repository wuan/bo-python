# -*- coding: utf8 -*-

"""

@author Andreas Würl

"""

import math
from abc import ABCMeta, abstractmethod

import shapely.geometry

import numpy


class Geometry(object):
    """
    abstract base class for geometries
    """

    __metaclass__ = ABCMeta

    DefaultSrid = 4326

    def __init__(self, srid=DefaultSrid):
        self.srid = srid

    def get_srid(self):
        return self.srid

    def set_srid(self, srid):
        self.srid = srid

    @abstractmethod
    def get_env(self):
        pass


class Envelope(Geometry):
    """
    definition of a coordinate envelope
    """

    def __init__(self, x_min, x_max, y_min, y_max, srid=Geometry.DefaultSrid):
        super(Envelope, self).__init__(srid)
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max

    def get_x_min(self):
        return self.x_min

    def get_x_max(self):
        return self.x_max

    def get_y_min(self):
        return self.y_min

    def get_y_max(self):
        return self.y_max

    def get_y_delta(self):
        return abs(self.y_max - self.y_min)

    def get_x_delta(self):
        return abs(self.x_max - self.x_min)

    def contains(self, point):
        return (point.get_x() >= self.x_min) and \
               (point.get_x() <= self.x_max) and \
               (point.get_y() >= self.y_min) and \
               (point.get_y() <= self.y_max)

    def get_env(self):
        return shapely.geometry.LinearRing(
            [(self.x_min, self.y_min), (self.x_min, self.y_max), (self.x_max, self.y_max), (self.x_max, self.y_min)])

    def __repr__(self):
        return 'Envelope(x: %.4f..%.4f, y: %.4f..%.4f)' % (
            self.x_min, self.x_max, self.y_min, self.y_max)


class Grid(Envelope):
    """ grid characteristics"""

    def __init__(self, x_min, x_max, y_min, y_max, x_div, y_div, srid=Geometry.DefaultSrid):
        super(Grid, self).__init__(x_min, x_max, y_min, y_max, srid)
        self.x_div = x_div
        self.y_div = y_div
        self.x_bin_count = None
        self.y_bin_count = None

    def get_x_div(self):
        return self.x_div

    def get_y_div(self):
        return self.y_div

    def get_x_bin(self, x_pos):
        return int(math.ceil(float(x_pos - self.x_min) / self.x_div)) - 1

    def get_y_bin(self, y_pos):
        return int(math.ceil(float(y_pos - self.y_min) / self.y_div)) - 1

    def get_x_bin_count(self):
        if not self.x_bin_count:
            self.x_bin_count = self.get_x_bin(self.x_max) + 1
        return self.x_bin_count

    def get_y_bin_count(self):
        if not self.y_bin_count:
            self.y_bin_count = self.get_y_bin(self.y_max) + 1
        return self.y_bin_count

    def get_x_center(self, cell_index):
        return self.x_min + (cell_index + 0.5) * self.x_div

    def get_y_center(self, row_index):
        return self.y_min + (row_index + 0.5) * self.y_div

    def __repr__(self):
        return 'Grid(x: %.4f..%.4f (%.4f), y: %.4f..%.4f (%.4f))' % (
            self.get_x_min(), self.get_x_max(), self.get_x_div(),
            self.get_y_min(), self.get_y_max(), self.get_y_div())


class Raster(Grid):
    """ class for grid characteristics"""

    def __init__(self, x_min, x_max, y_min, y_max, x_div, y_div, srid=Geometry.DefaultSrid, no_data=None):
        super(Raster, self).__init__(x_min, x_max, y_min, y_max, x_div, y_div, srid)
        self.no_data = no_data if no_data else RasterElement(0, None)
        self.data = []
        self.clear()

    def clear(self):
        self.data = numpy.empty((self.get_y_bin_count(), self.get_x_bin_count()), dtype=type(self.no_data))

    def set(self, x_index, y_index, value):
        try:
            self.data[y_index][x_index] = value
        except IndexError:
            pass

    def get(self, x_index, y_index):
        return self.data[y_index][x_index]

    def get_nodata_value(self):
        return self.no_data

    def to_arcgrid(self):
        result = 'NCOLS %d\n' % self.get_x_bin_count()
        result += 'NROWS %d\n' % self.get_y_bin_count()
        result += 'XLLCORNER %.4f\n' % self.get_x_min()
        result += 'YLLCORNER %.4f\n' % self.get_y_min()
        result += 'CELLSIZE %.4f\n' % self.get_x_div()
        result += 'NODATA_VALUE %s\n' % str(self.get_nodata_value())

        cell_to_string = lambda current_cell: str(current_cell.get_count()) if current_cell else '0'
        result += '\n'.join([' '.join([cell_to_string(cell) for cell in row]) for row in self.data[::-1]])

        return result

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

        for row_index, row in enumerate(self.data[::-1]):
            for column_index, cell in enumerate(row):
                if cell:
                    reduced_array.append([column_index, row_index,
                                          int(cell.get_count()),
                                          -(reference_time - cell.get_timestamp()).seconds])

        return tuple(reduced_array)


class RasterElement(object):
    """
    raster data entry
    """

    def __init__(self, count, timestamp):
        self.count = count
        self.timestamp = timestamp

    def __gt__(self, other):
        return self.count > other.count

    def get_count(self):
        return self.count

    def get_timestamp(self):
        return self.timestamp

    def __str__(self):
        if self.timestamp is None:
            return str(self.count)

    def __repr__(self):
        return "RasterElement(%d, %s)" % (self.count, str(self.timestamp))
