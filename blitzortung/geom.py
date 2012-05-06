# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

import subprocess
import re
import pyproj
import numpy
import math
import shapely

from abc import ABCMeta, abstractmethod

class Point(object):

  __geod = pyproj.Geod(ellps='WGS84', units='m')

  __whitespaceRe = re.compile('\s+')

  def __init__(self, x, y):
    self.x = x
    self.y = y

  def get_x(self):
    return self.x

  def get_y(self):
    return self.y

  def __invgeod(self, other):
    return Point.__geod.inv(self.x, self.y, other.x, other.y)

  def distance(self, other):
    return self.__invgeod(other)[2]

  def azimuth(self, other):
    return self.__invgeod(other)[0]

class Geometry(object):
  '''
  abstract base class for geometries
  '''
  
  __metaclass__ = ABCMeta
    
  DefaultSrid = 4326
  
  def __init__(self, srid = DefaultSrid):
    self.srid = srid
    
  def getSrid(self):
    return self.srid
  
  def setSrid(self, srid):
    self.srid = srid

  @abstractmethod
  def getEnv(self):
    pass


class Envelope(Geometry):
  ''' class for definition of coordinate envelopes '''

  def __init__(self, xmin, xmax, ymin, ymax, srid=Geometry.DefaultSrid):
    Geometry.__init__(self, srid)
    self.xmin = xmin
    self.xmax = xmax
    self.ymin = ymin
    self.ymax = ymax

  def getXMin(self):
    return self.xmin

  def getXMax(self):
    return self.xmax

  def getYMin(self):
    return self.ymin

  def getYMax(self):
    return self.ymax
  
  def getSrid(self):
    return self.srid

  def getDeltaY(self):
    return abs(self.ymax - self.ymin)

  def getDeltaX(self):
    return abs(self.xmax - self.xmin)

  def contains(self, point):
    if ((point.getX() >= self.xmin) and 
        (point.getX() <= self.xmax) and
        (point.getY() >= self.ymin) and
        (point.getY() <= self.ymax)):
      return True
    else:
      return False
    
  def getEnv(self):
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
    self.data = numpy.empty((self.getYBinCount(), self.getXBinCount()), dtype=type(self.nodata))

  def getXDiv(self):
    return self.xdiv
  
  def getYDiv(self):
    return self.ydiv
  
  def getXBinCount(self):
    return int(math.ceil(1.0 * (self.xmax - self.xmin) / self.xdiv))

  def getYBinCount(self):
    return int(math.ceil(1.0 * (self.ymax - self.ymin) / self.ydiv))

  def getXCenter(self, cellindex):
    return self.xmin + (cellindex + 0.5) * self.xdiv

  def getYCenter(self, rowindex):
    return self.ymin + (rowindex + 0.5) * self.ydiv

  def set(self, x, y, data):
    self.data[y][x] = data
    
  def get(self, x, y):
    return self.data[y][x]
  
  def getNodataValue(self):
    return self.nodata

  def toArcGrid(self):
    result  = 'NCOLS %d\n' % self.getXBinCount()
    result += 'NROWS %d\n' % self.getYBinCount()
    result += 'XLLCORNER %.4f\n' % self.getXMin()
    result += 'YLLCORNER %.4f\n' % self.getYMin()
    result += 'CELLSIZE %.4f\n' % self.getXDiv()
    result += 'NODATA_VALUE %s\n' % str(self.getNodataValue())
    
    for row in self.data[::-1]:
      for cell in row:
        result += str(cell.get_count()) if cell else 0 + ' '
      result += '\n'
      
    return result.strip()

  def toMap(self):
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

    result = (self.getXBinCount() + 2) * '-' + '\n'
    for row in self.data[::-1]:
      result += "|"
      for cell in row:
        if cell:
	  index = int(math.floor((cell.get_count() - 1) / divider + 1))
        else:
	  index = 0
	result += chars[index]
      result += "|\n"

    result += (self.getXBinCount() + 2) * '-' + '\n'
    result += 'total count: %d, max per area: %d' %(total, maximum)
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
    return "RasterElement(%d, %s)" %(self.count, str(self.timestamp))

  def get_count(self):
    return self.count

  def get_timestamp(self):
    return self.timestamp

