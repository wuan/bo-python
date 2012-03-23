import unittest
import mock
import datetime

import blitzortung

class TestTimeRange(unittest.TestCase):
    
    def setUp(self):
        self.end_time = datetime.datetime(2012,3,2,11,20,24)
        self.interval = datetime.timedelta(hours=1, minutes=30, seconds=10)
        self.microsecond_delta = datetime.timedelta(microseconds=1)
        
        self.time_range = blitzortung.data.TimeRange(self.end_time, self.interval)        
    
    def test_get_start_and_end_time(self):
        
        self.assertEquals(self.time_range.get_start_time(), self.end_time - self.interval)
        
        self.assertEquals(self.time_range.get_end_time(), self.end_time)
        
    def test_contains(self):
        
        start_time = self.time_range.get_start_time()
        end_time = self.time_range.get_end_time()
        
        self.assertTrue(self.time_range.contains(start_time))
        self.assertFalse(self.time_range.contains(end_time))
        self.assertFalse(self.time_range.contains(start_time - self.microsecond_delta))
        self.assertTrue(self.time_range.contains(end_time - self.microsecond_delta))
        
