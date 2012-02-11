import unittest
import datetime
import pytz

import blitzortung

    
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
    