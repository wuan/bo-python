# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

import datetime, pytz
import math
import urllib
import pyproj


import builder
import geom
import files
import time
import types

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

      
class Event(types.Point):

  def __init__(self, x, y, timestamp, timestamp_nanoseconds = 0):
    super(Event, self).__init__(x, y)
    self.timestamp = timestamp
    self.timestamp_nanoseconds = timestamp_nanoseconds
    
  def get_timestamp(self):
    return self.timestamp
  
  def get_timestamp_nanoseconds(self):
    return self.timestamp_nanoseconds

  def difference(self, other):
    return self.timestamp - other.timestamp

  def difference_nanoseconds(self, other):
    return self.timestamp_nanoseconds - other.timestamp_nanoseconds

class RawEvent(Event):
  def __init__(self, x, y, timestamp, timestamp_nanoseconds, height, number_of_satellites, sample_period, amplitude_x, amplitude_y):
    super(RawEvent, self).__init__(x, y, timestamp, timestamp_nanoseconds)
    self.height = height
    self.number_of_satellites = number_of_satellites
    self.sample_period = sample_period
    self.amplitude_x = amplitude_x
    self.amplitude_y = amplitude_y

  def __str__(self):
    return "%s%03d %.4f %.4f %d %d %d %.2f %.2f" %(self.get_timestamp().strftime(builder.Base.timeformat_fractional_seconds), self.get_timestamp_nanoseconds(), self.x_coord, self.y_coord, self.height, self.number_of_satellites, self.sample_period, self.amplitude_x, self.amplitude_y)

  def getXAmplitude(self):
    return self.amplitude_x

  def getYAmplitude(self):
    return self.amplitude_y

class Station(Event):
  
  def __init__(self, number, short_name, name, location_name, country, x, y, last_data, offline_since, gps_status, tracker_version, samples_per_hour):
    super(Station, self).__init__(x, y, last_data, 0)
    self.number = number
    self.short_name = short_name
    self.name = name
    self.location_name = location_name
    self.country = country
    self.offline_since = offline_since
    self.gps_status = gps_status
    self.tracker_version = tracker_version
    self.samples_per_hour = samples_per_hour
      
  def __str__(self):
    return u"%d %s %s %s %s %s" %(self.number, self.short_name, self.location_name, self.country, super(Station, self).__str__(), self.get_timestamp().strftime(builder.Base.timeformat))
   
  def __eq__(self, other):
    #return self.number == other.number and self.short_name == other.short_name and self.location_name == other.location_name and self.country == other.country and self.timestamp == other.timestamp   
    return self.number == other.number and self.short_name == other.short_name and self.location_name == other.location_name and self.country == other.country

  def __ne__(self, other):
    return not self == other
  
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
  
  def get_offline_since(self):
    return self.offline_since

  def get_gps_status(self):
    return self.gps_status
  
  def get_tracker_version(self):
    return self.tracker_version
  
  def get_samples_per_hour(self):
    return self.samples_per_hour
  
class StationOffline(object):

  def __init__(self, id_number, number, begin, end=None):
    self.id_number = id_number
    self.number = number
    self.begin = begin
    self.end = end

  def get_id(self):
    return self.id_number

  def get_number(self):
    return self.number

  def get_begin(self):
    return self.begin

  def get_end(self):
    return self.end

  def set_end(self, end):
    if not self.end:
      self.end = end
    else:
      raise ValueError('cannot overwrite end of StationOffline when already set')


class Stroke(Event):
  '''
  classdocs
  '''

  def __init__(self, id, x, y, timestamp, timestamp_ns, amplitude, height, lateral_error, type_val, station_count, participants = []):
    super(Stroke, self).__init__(x, y, timestamp, timestamp_ns)
    self.id = id
    self.amplitude = amplitude
    self.height = height
    self.lateral_error = lateral_error
    self.type_val = type_val
    self.station_count = station_count
    self.participants = participants

  def get_location(self):
    return self

  def get_height(self):
    return self.height

  def get_amplitude(self):
    return self.amplitude

  def get_type(self):
    return self.type_val

  def get_id(self):
    return self.id

  def get_lateral_error(self):
    return self.lateral_error

  def get_station_count(self):
    return self.station_count

  def has_participant(self, participant):
    return self.participants.count(participant) > 0

  def is_detected_by_user(self):
    return False

  def __str__(self):
    return "%s%03d%s %.4f %.4f %d %.1f %d %.1f %d" %(self.timestamp.strftime(builder.Base.timeformat_fractional_seconds), self.get_timestamp_nanoseconds(), self.timestamp.strftime('%z'), self.x_coord, self.y_coord, self.height, self.amplitude, self.type_val, self.lateral_error, self.station_count)

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
