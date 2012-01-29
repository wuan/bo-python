# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

import datetime, pytz
import math
import urllib
#import shapely.geometry
import pyproj

import geom
import files

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
      raise Exception("unhandled type"+type(time))

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

  def distance(self, other):
    return self.__invgeod(other)[2]

  def azimuth(self, other):
    return self.__invgeod(other)[0]

class Event(Point):

  timeformat = '%Y-%m-%d %H:%M:%S.%f'

  def __init__(self, x, y, time):
    Point.__init__(self, x, y)
    if isinstance(time, datetime.datetime):
      self.time = time
      self.nanoseconds = 0
    elif isinstance(time, str):
      self.set_time_from_string(time)
      
  def set_time_from_string(self, time):
    self.nanoseconds = int(time[-3:])
    time = datetime.datetime.strptime(time[:-3], Event.timeformat)
    self.time = time.replace(tzinfo=pytz.UTC)

  def set_time(self, time):
    self.time = time

  def get_time(self):
    return self.time

  def set_nanoseconds(self, nanoseconds):
    self.nanoseconds = nanoseconds

  def get_nanoseconds(self):
    return self.nanoseconds

  def difference(self, other):
    return self.time - other.time

  def nanoseconds_difference(self, other):
    return self.nanoseconds - other.nanoseconds

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
    return "%s%03d %.4f %.4f %d %d %d %.2f %.2f" %(self.time.strftime(Event.timeformat), self.get_nanoseconds(), self.x, self.y, self.height, self.numberOfSatellites, self.samplePeriod, self.amplitudeX, self.amplitudeY)

  def getXAmplitude(self):
    return self.amplitudeX

  def getYAmplitude(self):
    return self.amplitudeY

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
    return "%s%03d%s %.4f %.4f %d %.1f %d %.1f %d" %(self.time.strftime(Event.timeformat), self.get_nanoseconds(), self.time.strftime('%z'), self.x, self.y, self.height, self.amplitude, self.typeVal, self.error2d, self.stationcount)

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

class StrokesUrl:

  def __init__(self, baseurl):
    self.url = baseurl

  def add(self, name, value):
    self.url += '&' + str(name).strip() + '=' + str(value).strip()

  def readData(self):
    urlconnection = urllib.urlopen(self.url)
    data = urlconnection.read().strip()
    urlconnection.close()
    return data

  def get(self, timeInterval=None):
    strokes = []
    for line in self.readData().split('\n'):
      stroke = Stroke(line)
      if timeInterval==None or timeInterval.contains(stroke.get_time()):
        strokes.append(stroke)
    return strokes

class Strokes(StrokesUrl):

  def __init__(self, config):
    StrokesUrl.__init__(self, 'http://'+config.get('username')+':'+config.get('password')+'@blitzortung.tmt.de/Data/Protected/strikes.txt')

class ParticipantStrokes(StrokesUrl):

  def __init__(self, config):
    StrokesUrl.__init__(self, 'http://'+config.get('username')+':'+config.get('password')+'@blitzortung.tmt.de/Data/Protected/participants.txt')
