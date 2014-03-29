import unittest
import math

import blitzortung.types


class PointTest(unittest.TestCase):
    def setUp(self):
        self.point1 = blitzortung.types.Point(11, 49)
        self.point2 = blitzortung.types.Point(12, 49)
        self.point3 = blitzortung.types.Point(11, 50)
        self.radians_factor = math.pi / 180

    def test_get_coordinate_components(self):
        self.assertEqual(self.point1.get_x(), 11)
        self.assertEqual(self.point1.get_y(), 49)

    def test_get_azimuth(self):
        self.assertAlmostEqual(self.point1.azimuth_to(self.point2), 89.62264107 * self.radians_factor)
        self.assertEqual(self.point1.azimuth_to(self.point3), 0)

    def test_get_distance(self):
        self.assertAlmostEqual(self.point1.distance_to(self.point2), 73171.2643568)
        self.assertAlmostEqual(self.point1.distance_to(self.point3), 111219.409, 3)

    def test_get_geodesic_relation(self):
        azimuth, distance = self.point1.geodesic_relation_to(self.point2)
        self.assertAlmostEqual(azimuth, 89.62264107 * self.radians_factor)
        self.assertAlmostEqual(distance, 73171.2643568)

    def test_geodesic_shift(self):
        point = self.point1.geodesic_shift(0, 100000)
        self.assertAlmostEqual(point.get_x(), 11.0)
        self.assertAlmostEqual(point.get_y(), 49.8991315)

    def test_to_string(self):
        self.assertEqual(str(self.point1), "(11.0000, 49.0000)")
