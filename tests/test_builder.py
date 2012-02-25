import unittest
import datetime
import pytz

import blitzortung

    
class StationTest(unittest.TestCase):
  
  def test_build_station(self):
    line = "364 MustermK Karl&nbsp;Mustermann Neustadt Germany 49.5435 9.7314 2012-02-10&nbsp;14:39:47.410492569 A WT&#32;5.20.3 4"
    station_builder = blitzortung.builder.Station()
    station_builder.set(line)
    station = station_builder.build()
    
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

    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.096651423")
        
    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 96651)
    self.assertEqual(timestamp_nanoseconds, 423)

  def test_create_from_millisecond_string(self):

    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.096")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 96000)
    self.assertEqual(timestamp_nanoseconds, 0)
    
  def test_create_from_string_wihtout_fractional_seconds(self):

    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 0)
    self.assertEqual(timestamp_nanoseconds, 0)
  
  def test_create_from_nanosecond_string(self):

    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.123456789")  
    
    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_nanoseconds, 789)
    
    
    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.12345678")  
 
    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_nanoseconds, 780)
    
    
    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.1234567")  
    
    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_nanoseconds, 700)    
    