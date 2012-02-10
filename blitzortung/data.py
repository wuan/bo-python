# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

import datetime, pytz
import math
import urllib
#import shapely.geometry
import pyproj
import HTMLParser

import geom
import files
import time

html_parser = HTMLParser.HTMLParser()

class TimeRange(object):

  def __init__(self, endTime, deltaTime=datetime.timedelta(hours=2)):
    self.endTime = endTime
    self.startTime = endTime - deltaTime

  def __str__(self):
    return "['" + str(self.startTime) + "':'" + str(self.endTime) + "']"

  def getStartTime(self):
    return self.startTime

  def getEndTime(self):
    return self.endTime

  def getEndMinute(self):
    return self.getEndTime() - datetime.timedelta(minutes=1)

  def contains(self, time):
    return time >= self.getStartTime() and time <= self.getEndTime()

class TimeInterval(TimeRange):

  def __init__(self, endTime, deltaTime=datetime.timedelta(hours=1)):
    self.deltaTime = deltaTime
    TimeRange.__init__(self, self.roundTime(endTime), deltaTime)

  def __str__(self):
    return "['" + str(self.startTime) + "':'" + str(self.endTime) + "'," + str(self.deltaTime) + "]"

  def totalSeconds(self, time):
    ' return the total seconds of the given time or datetime (relative to midnight) '

    if isinstance(time, datetime.datetime):
      return time.hour * 3600 + time.minute * 60 + time.second
    elif isinstance(time, datetime.timedelta):
      return time.seconds + time.days * 24 * 3600
    else:
      raise Exception("unhandled type " + type(time))

  def roundTime(self, time):
    deltaSeconds = self.totalSeconds(self.deltaTime)

    seconds =  self.totalSeconds(time)
    seconds /= deltaSeconds
    seconds *= deltaSeconds

    if isinstance(time, datetime.datetime):
      return time.replace(hour = seconds/3600, minute= seconds / 60 % 60, second= seconds % 60, microsecond=0)
    else:
      return datetime.timedelta(seconds=seconds)

  def hasNext(self):
    return False

  def next(self):
    raise Exception(' no next interval ')

  def getCenterTime(self):
    return self.startTime + self.deltaTime / 2

class TimeIntervals(TimeInterval):

  def __init__(self, endTime, deltaTime=datetime.timedelta(minutes=15), totalDuration=datetime.timedelta(days=1)):
    TimeInterval.__init__(self, endTime, deltaTime)

    self.totalDuration = self.roundTime(totalDuration)

    self.startTime = self.endTime - self.totalDuration

  def hasNext(self):
    return self.startTime + self.deltaTime < self.endTime

  def next(self):
    if self.hasNext():
      self.startTime += self.deltaTime
      return self.startTime
    else:
      raise Exception('no more time intervals')

  def getEndTime(self):
    return self.startTime + self.deltaTime

class Point(object):

  __geod = pyproj.Geod(ellps='WGS84', units='m')

  def __init__(self, x, y):
    self.x = x
    self.y = y

  def __invgeod(self, other):
    return Point.__geod.inv(self.x, self.y, other.x, other.y)

  def get_x(self):
    return self.x
  
  def get_y(self):
    return self.y
  
  def distance_to(self, other):
    return self.__invgeod(other)[2]

  def azimuth_to(self, other):
    return self.__invgeod(other)[0]
  
  def __str__(self):
    return "(%.4f, %.4f)" %(self.x, self.y)

class Timestamp(object):
  timeformat = '%Y-%m-%d %H:%M:%S'
  timeformat_fractional_seconds = timeformat + '.%f'
  timestamp_string_minimal_fractional_seconds_length = 20
  timestamp_string_microseconds_length = 26
  
  def __init__(self, timestamp_value):
    self.init_timestamp(timestamp_value)
    
  def init_timestamp(self, timestamp_value):
    if isinstance(timestamp_value, datetime.datetime):
      self.timestamp = timestamp_value
    elif isinstance(timestamp_value, str):
      if len(timestamp_value) > Timestamp.timestamp_string_minimal_fractional_seconds_length:
        timestamp_format = Timestamp.timeformat_fractional_seconds
        if len(timestamp_value) > Timestamp.timestamp_string_microseconds_length:
          timestamp_value = timestamp_value[:self.timestamp_string_microseconds_length]
      else:
        timestamp_format = Timestamp.timeformat
      
      timestamp = datetime.datetime.strptime(timestamp_value, timestamp_format)
      self.timestamp = timestamp.replace(tzinfo=pytz.UTC)
    else:
      raise ValueError("init_timestamp can only be called with datetime or string")
  
  def set_timestamp(self, timestamp):
    self.timestamp = timestamp

  def get_timestamp(self):
    return self.timestamp

  def difference(self, other):
    return self.timestamp - other.timestamp

class NanosecondTimestamp(Timestamp):
  
  def __init__(self, timestamp_value):
    self.init_timestamp(timestamp_value)
    self.init_nanosecond_timestamp(timestamp_value)
    
  def init_nanosecond_timestamp(self, timestamp_value):
    if isinstance(timestamp_value, datetime.datetime):
      self.nanoseconds = 0
    elif isinstance(timestamp_value, str):
      if len(timestamp_value) > Timestamp.timestamp_string_microseconds_length:
        nanoseconds_string = timestamp_value[self.timestamp_string_microseconds_length:self.timestamp_string_microseconds_length + 3]
        self.nanoseconds = int(nanoseconds_string.ljust(3).replace(' ', '0'))
      else:
        self.nanoseconds = 0
      
  def set_nanoseconds(self, nanoseconds):
    self.nanoseconds = nanoseconds

  def get_nanoseconds(self):
    return self.nanoseconds

  def nanoseconds_difference(self, other):
    return self.nanoseconds - other.nanoseconds  
      
class Event(Point, NanosecondTimestamp):

  def __init__(self, x, y, timestamp_value):
    super(Event, self).__init__(self, x, y)
    self.init_nanosecond_timestamp(timestamp_value)


class RawEvent(Event):
#2011-02-20 15:16:26.723987041 11.5436 48.1355 521 8 3125 -0.12 0.20 14
  def __init__(self, data = None):
    if data != None:
      fields = data.split(' ')
      Event.__init__(self, float(fields[2]), float(fields[3]), ' '.join(fields[0:2]))
      self.time = self.time + datetime.timedelta(seconds=1)
      if len(fields) >= 8:
        self.height = int(fields[4])
        self.numberOfSatellites = int(fields[5])
        self.samplePeriod = int(fields[6])
        self.amplitudeX = float(fields[7])
        self.amplitudeY = float(fields[8])
      else:
        raise Error("not enough data fields for raw event data '%s'" %(data))

  def __str__(self):
    return "%s%03d %.4f %.4f %d %d %d %.2f %.2f" %(self.time.strftime(Timestamp.timeformat_fractional_seconds), self.get_nanoseconds(), self.x, self.y, self.height, self.numberOfSatellites, self.samplePeriod, self.amplitudeX, self.amplitudeY)

  def getXAmplitude(self):
    return self.amplitudeX

  def getYAmplitude(self):
    return self.amplitudeY

class Station(Point, Timestamp):
  
  html_parser = HTMLParser.HTMLParser()
  
  def __init__(self, data = None):
    if data != None:
      fields = data.split(' ')
      self.number = int(fields[0])
      self.short_name = fields[1]
      self.name = unicode(self._unquote(fields[2]))
      self.location_name = unicode(self._unquote(fields[3]))
      self.country = unicode(self._unquote(fields[4]))
      super(Station, self).__init__(float(fields[6]), float(fields[5]))
      super(Station, self).init_timestamp(self._unquote(fields[7]).encode('ascii'))
      self.gps_status = fields[8]
      self.tracker_version = self._unquote(fields[9])
      self.samples_per_hour = int(fields[10])
      
  def __str__(self):
    return "%d %s %s %s %s %s" %(self.number, self.short_name, self.location_name, self.country, super(Station, self).__str__(), self.get_timestamp().strftime(Timestamp.timeformat))
     
  def _unquote(self, html_coded_string):
    return Station.html_parser.unescape(html_coded_string.replace('&nbsp;', ' '))
      
  def get_number(self):
    return self.number
  
  def get_short_name(self):
    return self.short_name
  
  def get_name(self):
    return self.name
  
  def get_location_name(self):
    return self.location_name
  
  def get_country(self):
    return self.country
  
  def get_gps_status(self):
    return self.gps_status
  
  def get_tracker_version(self):
    return self.tracker_version
  
  def get_samples_per_hour(self):
    return self.samples_per_hour
  
class Stroke(Event):
  '''
  classdocs
  '''

  def __init__(self, data = None):
    if data != None:
      ' Construct stroke from blitzortung text format data line '
      fields = data.split(' ')
      Event.__init__(self, float(fields[3]), float(fields[2]), ' '.join(fields[0:2]))
      if len(fields) >= 5:
        self.amplitude = float(fields[4][:-2])
        self.typeVal = int(fields[5])
        self.error2d = int(fields[6][:-1])
        if self.error2d < 0:
          self.error2d = 0
        self.stationcount = int(fields[7])
        self.participants = []
        if (len(fields) >=9):
          for index in range(8,len(fields)):
            self.participants.append(fields[index])
      else:
        raise Error("not enough data fields from stroke data line '%s'" %(data))
    self.height = 0.0

  def set_location(self, location):
    Point.__init__(self, location.x, location.y)

  def get_location(self):
    return self

  def get_height(self):
    return self.height

  def set_height(self, height):
    self.height = height

  def get_amplitude(self):
    return self.amplitude

  def set_amplitude(self, amplitude):
    self.amplitude = amplitude

  def get_type(self):
    return self.typeVal

  def set_type(self, typeVal):
    self.typeVal = typeVal

  def set_id(self, id):
    self.id = id

  def get_id(self):
    return self.id

  def get_lateral_error(self):
    return self.error2d

  def set_lateral_error(self, error2d):
    self.error2d = error2d

  def get_station_count(self):
    return self.stationcount

  def set_station_count(self, stationcount):
    self.stationcount = stationcount

  def has_participant(self, participant):
    return self.participants.count(participant) > 0

  def is_detected_by_user(self):
    return False

  def __str__(self):
    return "%s%03d%s %.4f %.4f %d %.1f %d %.1f %d" %(self.time.strftime(Timestamp.timeformat_fractional_seconds), self.get_nanoseconds(), self.time.strftime('%z'), self.x, self.y, self.height, self.amplitude, self.typeVal, self.error2d, self.stationcount)

class Histogram(object):

  def __init__(self, fileNames, time):
    data = files.StatisticsData(fileNames, time)

    while True:

      data.get()

      print time.getCenterTime(), data.getCount(), data.getMean(), data.getVariance()

      if not time.hasNext():
        break

      time.next()

class AmplitudeHistogram(object):

  def __init__(self, fileNames, time):
    data = files.HistogramData(fileNames, time)

    data.list()
