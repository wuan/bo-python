import unittest
import mock
import datetime
import numpy as np
import pandas as pd
import pytz

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
    
    def test_time_difference(self):
        
        now = pd.Timestamp(datetime.datetime.utcnow(), tz=pytz.UTC)
        later = pd.Timestamp(np.datetime64(now.value + 100, 'ns'), tz=pytz.UTC)
    
        event1 = blitzortung.data.Event(now, 11, 49)
        event2 = blitzortung.data.Event(later, 11, 49)
        
        self.assertEqual(datetime.timedelta(), event1.difference_to(event2))
        self.assertEqual(datetime.timedelta(), event2.difference_to(event1))
        self.assertEqual(100, event1.ns_difference_to(event2))
        self.assertEqual(-100, event2.ns_difference_to(event1))
        
        even_later = pd.Timestamp(np.datetime64(now.value + 20150, 'ns'), tz=pytz.UTC)
        event3 = blitzortung.data.Event(even_later, 11, 49)
        self.assertEqual(datetime.timedelta(days=-1,seconds=86399,microseconds=999980), event1.difference_to(event3))
        self.assertEqual(datetime.timedelta(microseconds=20), event3.difference_to(event1))
        self.assertEqual(20150, event1.ns_difference_to(event3))
        self.assertEqual(-20150, event3.ns_difference_to(event1))

        much_later = pd.Timestamp(np.datetime64(now.value + 3000000200, 'ns'), tz=pytz.UTC)
        event4 = blitzortung.data.Event(much_later, 11, 49)
        self.assertEqual(datetime.timedelta(days=-1,seconds=86397,microseconds=0), event1.difference_to(event4))
        self.assertEqual(datetime.timedelta(seconds=3), event4.difference_to(event1))       
        self.assertEqual(3000000200, event1.ns_difference_to(event4))
        self.assertEqual(-3000000200, event4.ns_difference_to(event1))
