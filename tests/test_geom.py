from unittest import TestCase
import datetime
from mockito import mock, when, verify
from hamcrest import assert_that, is_, instance_of, is_not, same_instance, contains, equal_to
import time

import blitzortung


class GeometryForTest(blitzortung.geom.Geometry):
    def __init__(self, srid=None):
        if srid:
            super(GeometryForTest, self).__init__(srid)
        else:
            super(GeometryForTest, self).__init__()

    def get_env(self):
        return None


class TestGeometry(TestCase):
    def setUp(self):
        self.geometry = GeometryForTest()

    def test_default_values(self):
        assert_that(self.geometry.get_srid(), is_(equal_to(blitzortung.geom.Geometry.DefaultSrid)))

    def test_set_srid(self):
        self.geometry.set_srid(1234)

        assert_that(self.geometry.get_srid(), is_(equal_to(1234)))

    def test_create_with_different_srid(self):
        self.geometry = GeometryForTest(1234)

        assert_that(self.geometry.get_srid(), is_(equal_to(1234)))


class TestEnvelope(TestCase):
    def setUp(self):
        self.envelope = blitzortung.geom.Envelope(-5, 4, -3, 2)

    def test_default_values(self):
        assert_that(self.envelope.get_srid(), is_(equal_to(blitzortung.geom.Geometry.DefaultSrid)))

    def test_custom_srid_value(self):
        self.envelope = blitzortung.geom.Envelope(-5, 4, -3, 2, 1234)
        assert_that(self.envelope.get_srid(), is_(equal_to(1234)))

    def test_get_envelope_coordinate_components(self):
        assert_that(self.envelope.get_x_min(), is_(equal_to(-5)))
        assert_that(self.envelope.get_x_max(), is_(equal_to(4)))
        assert_that(self.envelope.get_y_min(), is_(equal_to(-3)))
        assert_that(self.envelope.get_y_max(), is_(equal_to(2)))

    def test_get_envelope_parameters(self):
        assert_that(self.envelope.get_x_delta(), is_(equal_to(9)))
        assert_that(self.envelope.get_y_delta(), is_(equal_to(5)))

    def test_contains_point_inside_envelope(self):
        self.assertTrue(self.envelope.contains(blitzortung.types.Point(0, 0)))
        self.assertTrue(self.envelope.contains(blitzortung.types.Point(1, 1.5)))
        self.assertTrue(self.envelope.contains(blitzortung.types.Point(-1, -1.5)))

    def test_contains_point_on_border(self):
        self.assertTrue(self.envelope.contains(blitzortung.types.Point(0, -3)))
        self.assertTrue(self.envelope.contains(blitzortung.types.Point(0, 2)))
        self.assertTrue(self.envelope.contains(blitzortung.types.Point(-5, 0)))
        self.assertTrue(self.envelope.contains(blitzortung.types.Point(4, 0)))

    def test_does_not_contain_point_outside_border(self):
        self.assertFalse(self.envelope.contains(blitzortung.types.Point(0, -3.0001)))
        self.assertFalse(self.envelope.contains(blitzortung.types.Point(0, 2.0001)))
        self.assertFalse(self.envelope.contains(blitzortung.types.Point(-5.0001, 0)))
        self.assertFalse(self.envelope.contains(blitzortung.types.Point(4.0001, 0)))
