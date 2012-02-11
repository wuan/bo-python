import unittest

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

    timestamp_object = blitzortung.types.Timestamp("2012-02-10 12:56:18.096651423")
    timestamp = timestamp_object.get_timestamp()
        
    self.assert_timestamp_base(timestamp)
    
    self.assertEqual(timestamp.microsecond, 96651)

  def test_create_from_millisecond_string(self):

    timestamp_object = blitzortung.types.Timestamp("2012-02-10 12:56:18.096")
    timestamp = timestamp_object.get_timestamp()

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 96000)
    
  def test_create_from_string_wihtout_fractional_seconds(self):

    timestamp_object = blitzortung.types.Timestamp("2012-02-10 12:56:18")
    timestamp = timestamp_object.get_timestamp()

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 0)
    
class NanosecondTimestampTest(unittest.TestCase):
  
  def test_create_from_nanosecond_string(self):
    timestamp_object = blitzortung.types.NanosecondTimestamp("2012-02-10 12:56:18.123456789")
    timestamp = timestamp_object.get_timestamp()    
    
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_object.get_nanoseconds(), 789)
    
    timestamp_object = blitzortung.types.NanosecondTimestamp("2012-02-10 12:56:18.12345678")
    timestamp = timestamp_object.get_timestamp()    
    
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_object.get_nanoseconds(), 780)     
    
    timestamp_object = blitzortung.types.NanosecondTimestamp("2012-02-10 12:56:18.1234567")
    timestamp = timestamp_object.get_timestamp()    
    
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_object.get_nanoseconds(), 700)    
    
  def test_create_from_microsecond_string(self):
    timestamp_object = blitzortung.types.NanosecondTimestamp("2012-02-10 12:56:18.123456")
    timestamp = timestamp_object.get_timestamp()    
    
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_object.get_nanoseconds(), 0)
    
  def test_create_from_microsecond_string(self):
      timestamp_object = blitzortung.types.NanosecondTimestamp("2012-02-10 12:56:18.123456")
      timestamp = timestamp_object.get_timestamp()    
      
      self.assertEqual(timestamp.microsecond, 123456)
      self.assertEqual(timestamp_object.get_nanoseconds(), 0)     
