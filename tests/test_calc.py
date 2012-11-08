import unittest
import math

import blitzortung

class ThreePointSolutionTest(unittest.TestCase):
  
  def setUp(self):
    self.radians_factor = math.pi / 180
    
  def test_get_coordinate_components(self):
    center_point = blitzortung.types.Point(11,49)
    solution = blitzortung.calc.ThreePointSolution(center_point, 0, 100000)
    location = solution.get_location()
    
    self.assertAlmostEqual(location.get_x(), 11)
    self.assertAlmostEqual(location.get_y(), 49.89913151)