import unittest
import math

import blitzortung
 
class PointTest(unittest.TestCase):
  
  def setUp(self):
    self.point1 = blitzortung.types.Point(11, 49)
    self.point2 = blitzortung.types.Point(12, 49)
    self.point3 = blitzortung.types.Point(11, 50)
    
  def test_get_coordinate_components(self):
    self.assertEqual(self.point1.get_x(), 11)
    self.assertEqual(self.point1.get_y(), 49)
    
  def test_get_azimuth(self):
    self.assertAlmostEqual(self.point1.azimuth_to(self.point2), 89.62264107)
    self.assertEqual(self.point1.azimuth_to(self.point3), 0)
    
  def test_get_distance(self):
    self.assertAlmostEqual(self.point1.distance_to(self.point2), 73171.2643568)
    self.assertAlmostEqual(self.point1.distance_to(self.point3), 111219.4092149)
    
  def test_get_geodesic_relation(self):
    distance, azimuth = self.point1.geodesic_relation_to(self.point2)
    self.assertAlmostEqual(azimuth, 89.62264107)
    self.assertAlmostEqual(distance, 73171.2643568)
    
