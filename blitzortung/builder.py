# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

import datetime, pytz
import math
import HTMLParser

import data

class Base(object):
  
  timeformat = '%Y-%m-%d %H:%M:%S'
  timeformat_fractional_seconds = timeformat + '.%f'
  timestamp_string_minimal_fractional_seconds_length = 20
  timestamp_string_microseconds_length = 26  

  def parse_timestamp(self, timestamp_string):
    if len(timestamp_string) > Base.timestamp_string_minimal_fractional_seconds_length:
        timestamp_format = Base.timeformat_fractional_seconds
        if len(timestamp_string) > Base.timestamp_string_microseconds_length:
            timestamp_string = timestamp_string[:self.timestamp_string_microseconds_length]
    else:
        timestamp_format = Base.timeformat

    timestamp = datetime.datetime.strptime(timestamp_string, timestamp_format)
    return timestamp.replace(tzinfo=pytz.UTC)
  
  def parse_timestamp_with_nanoseconds(self, timestamp_string):
    timestamp = self.parse_timestamp(timestamp_string)
    if len(timestamp_string) > Base.timestamp_string_microseconds_length:
      nanoseconds_string = timestamp_string[self.timestamp_string_microseconds_length:self.timestamp_string_microseconds_length + 3]
      nanoseconds = int(nanoseconds_string.ljust(3).replace(' ', '0'))
    else:
      nanoseconds = 0 
    return (timestamp, nanoseconds)

class Stroke(Base):
  
  def __init__(self):
    self.id = -1
    self.height = -1.0
    self.participants = []
    
  def set_id(self, id):
    self.id = id
  
  def set_timestamp(self, timestamp):
    self.timestamp = timestamp
    
  def set_timestamp_nanoseconds(self, timestamp_nanoseconds):
    self.timestamp_nanoseconds = timestamp_nanoseconds
    
  def set_x(self, x):
    self.x = x
    
  def set_y(self, y):
    self.y = y
    
  def set_amplitude(self, amplitude):
    self.amplitude = amplitude
    
  def set_type(self, type_val):
    self.type_val = type_val
    
  def set_lateral_error(self, lateral_error):
    self.lateral_error = lateral_error
    
  def set_station_count(self, station_count):
    self.station_count = station_count
    
  def set_participants(self, participants):
    self.participants = participants
    
  def build(self):
    return data.Stroke(self.id, self.x, self.y, self.timestamp, self.timestamp_nanoseconds, self.amplitude, self.height, self.lateral_error, self.type_val, self.station_count, self.participants)
  
  def from_string(self, string):
    if string != None:
      ' Construct stroke from blitzortung text format data line '
      fields = string.split(' ')
      self.x = float(fields[3])
      self.y = float(fields[2])
      (self.timestamp, self.timestamp_nanoseconds) = self.parse_timestamp_with_nanoseconds(' '.join(fields[0:2]))
      
      if len(fields) >= 5:
        self.amplitude = float(fields[4][:-2])
        self.type_val = int(fields[5])
        self.lateral_error = int(fields[6][:-1])
        if self.lateral_error < 0:
          self.lateral_error = 0
        self.station_count = int(fields[7])
        self.participants = []
        if (len(fields) >=9):
          for index in range(8,len(fields)):
            self.participants.append(fields[index])
      else:
        raise Error("not enough data fields from stroke data line '%s'" %(string))
    self.height = 0.0  


class Station(Base):

  html_parser = HTMLParser.HTMLParser()
  
  def __init__(self):
    self.number = -1
    self.gps_status = 'n/a'
    self.samples_per_hour = -1
    self.tracker_version = 'n/a'
    
  def set_number(self, number):
    self.number = number
    
  def set_short_name(self, short_name):
    self.short_name = short_name
    
  def set_name(self, name):
    self.name = name
    
  def set_location_name(self, location_name):
    self.location_name = location_name
    
  def set_country(self, country):
    self.country = country
  
  def set_x(self, x):
    self.x = x
  
  def set_y(self, y):
    self.y = y
    
  def set_last_data(self, last_data):
    if isinstance(last_data, str):
      self.last_data = self.parse_timestamp(last_data)
    else:
      self.last_data = last_data
    
  def set(self, data):
    fields = data.split(' ')
    self.number = int(fields[0])
    self.short_name = fields[1]
    self.name = unicode(self._unquote(fields[2]))
    self.location_name = unicode(self._unquote(fields[3]))
    self.country = unicode(self._unquote(fields[4]))
    self.x = float(fields[6])
    self.y = float(fields[5])
    (self.last_data, dummy) = self.parse_timestamp_with_nanoseconds(self._unquote(fields[7]).encode('ascii'))
    self.gps_status = fields[8]
    self.tracker_version = self._unquote(fields[9])
    self.samples_per_hour = int(fields[10])
    
  def build(self):
    return data.Station(self.number, self.short_name, self.name, self.location_name, self.country, self.x, self.y, self.last_data, self.gps_status, self.tracker_version, self.samples_per_hour)

  def _unquote(self, html_coded_string):
    return Station.html_parser.unescape(html_coded_string.replace('&nbsp;', ' '))  

