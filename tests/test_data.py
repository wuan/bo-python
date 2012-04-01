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
                                                 
                                                 
class TestEvent(unittest.TestCase):
    
    def test_us_time_difference(self):
        
        now = datetime.datetime.utcnow()
        
        event1 = blitzortung.data.Event(11, 49, now, 0)
        event2 = blitzortung.data.Event(11, 49, now, 100)
        
        self.assertEqual(-0.1, event1.us_time_difference_to(event2))
        self.assertEqual(0.1, event2.us_time_difference_to(event1))
        
        event3 = blitzortung.data.Event(11, 49, now + datetime.timedelta(microseconds=20), 100)
        self.assertEqual(-20.1, event1.us_time_difference_to(event3))
        self.assertEqual(20.1, event3.us_time_difference_to(event1))
        
        event4 = blitzortung.data.Event(11, 49, now + datetime.timedelta(seconds=20), 100)
        self.assertEqual(-20000.1, event1.us_time_difference_to(event4))
        self.assertEqual(20000.1, event4.us_time_difference_to(event1))
        
        
        
