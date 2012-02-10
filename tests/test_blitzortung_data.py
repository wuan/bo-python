import unittest
import datetime
import pytz

import blitzortung

class TimestampTest(unittest.TestCase):

  def setUp(self):
    pass

  def assert_timestamp_base(self, timestamp):
    self.assertEqual(timestamp.day, 10)
    self.assertEqual(timestamp.month, 2)
    self.assertEqual(timestamp.year, 2012)
    self.assertEqual(timestamp.hour, 12)
    self.assertEqual(timestamp.minute, 56)
    self.assertEqual(timestamp.second, 18)
    
  def test_create_from_string(self):

    timestamp_object = blitzortung.data.Timestamp("2012-02-10 12:56:18.096651423")
    timestamp = timestamp_object.get_timestamp()
        
    self.assert_timestamp_base(timestamp)
    
    self.assertEqual(timestamp.microsecond, 96651)

  def test_create_from_millisecond_string(self):

    timestamp_object = blitzortung.data.Timestamp("2012-02-10 12:56:18.096")
    timestamp = timestamp_object.get_timestamp()

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 96000)
    
  def test_create_from_string_wihtout_fractional_seconds(self):

    timestamp_object = blitzortung.data.Timestamp("2012-02-10 12:56:18")
    timestamp = timestamp_object.get_timestamp()

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 0)
    
class NanosecondTimestampTest(unittest.TestCase):
  
  def test_create_from_nanosecond_string(self):
    timestamp_object = blitzortung.data.NanosecondTimestamp("2012-02-10 12:56:18.123456789")
    timestamp = timestamp_object.get_timestamp()    
    
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_object.get_nanoseconds(), 789)
    
    timestamp_object = blitzortung.data.NanosecondTimestamp("2012-02-10 12:56:18.12345678")
    timestamp = timestamp_object.get_timestamp()    
    
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_object.get_nanoseconds(), 780)     
    
    timestamp_object = blitzortung.data.NanosecondTimestamp("2012-02-10 12:56:18.1234567")
    timestamp = timestamp_object.get_timestamp()    
    
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_object.get_nanoseconds(), 700)    
    
  def test_create_from_microsecond_string(self):
    timestamp_object = blitzortung.data.NanosecondTimestamp("2012-02-10 12:56:18.123456")
    timestamp = timestamp_object.get_timestamp()    
    
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_object.get_nanoseconds(), 0)
    
  def test_create_from_microsecond_string(self):
      timestamp_object = blitzortung.data.NanosecondTimestamp("2012-02-10 12:56:18.123456")
      timestamp = timestamp_object.get_timestamp()    
      
      self.assertEqual(timestamp.microsecond, 123456)
      self.assertEqual(timestamp_object.get_nanoseconds(), 0)     
    
class StationTest(unittest.TestCase):
  
  def test_create_station(self):
    line = "364 MustermK Karl&nbsp;Mustermann Neustadt Germany 49.5435 9.7314 2012-02-10&nbsp;14:39:47.410492569 A WT&#32;5.20.3 4"
    station = blitzortung.data.Station(line)
    
    self.assertEqual(station.get_number(), 364)
    self.assertEqual(station.get_short_name(), 'MustermK')
    self.assertEqual(station.get_name(), 'Karl Mustermann')
    self.assertEqual(station.get_country(), 'Germany')
    self.assertEqual(station.get_x(), 9.7314)
    self.assertEqual(station.get_y(), 49.5435)
    self.assertEqual(station.get_timestamp(), datetime.datetime(2012,2,10,14,39,47,410492).replace(tzinfo=pytz.UTC))
    self.assertEqual(station.get_gps_status(), 'A')
    self.assertEqual(station.get_tracker_version(), 'WT 5.20.3')
    self.assertEqual(station.get_samples_per_hour(), 4)
    