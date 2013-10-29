from unittest import TestCase
import datetime
from hamcrest import assert_that, is_, instance_of, is_not, same_instance, contains, equal_to, none
import time
import shapely

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

    def test_get_env(self):
        expected_env = shapely.geometry.LinearRing([(-5, -3), (-5, 2), (4, 2), (4, -3)])
        self.assertTrue(expected_env.almost_equals(self.envelope.get_env()))

    def test_str(self):
        assert_that(repr(self.envelope), is_(equal_to('Envelope(x: -5.0000..4.0000, y: -3.0000..2.0000)')))


class TestGrid(TestCase):
    def setUp(self):
        self.grid = blitzortung.geom.Grid(-5, 4, -3, 2, 0.5, 1.25)

    def test_get_x_div(self):
        assert_that(self.grid.get_x_div(), is_(equal_to(0.5)))

    def test_get_y_div(self):
        assert_that(self.grid.get_y_div(), is_(equal_to(1.25)))

    def test_get_x_bin_count(self):
        assert_that(self.grid.get_x_bin_count(), is_(equal_to(18)))

    def test_get_y_bin_count(self):
        assert_that(self.grid.get_y_bin_count(), is_(equal_to(4)))

    def test_get_x_bin(self):
        assert_that(self.grid.get_x_bin(-5), is_(equal_to(-1)))
        assert_that(self.grid.get_x_bin(-4.9999), is_(equal_to(0)))
        assert_that(self.grid.get_x_bin(-4.5), is_(equal_to(0)))
        assert_that(self.grid.get_x_bin(-4.4999), is_(equal_to(1)))
        assert_that(self.grid.get_x_bin(4), is_(equal_to(17)))
        assert_that(self.grid.get_x_bin(4.0001), is_(equal_to(18)))

    def test_get_y_bin(self):
        assert_that(self.grid.get_y_bin(-3), is_(equal_to(-1)))
        assert_that(self.grid.get_y_bin(-2.9999), is_(equal_to(0)))
        assert_that(self.grid.get_y_bin(-1.7500), is_(equal_to(0)))
        assert_that(self.grid.get_y_bin(-1.7499), is_(equal_to(1)))
        assert_that(self.grid.get_y_bin(2), is_(equal_to(3)))
        assert_that(self.grid.get_y_bin(2.0001), is_(equal_to(4)))

    def test_get_x_center(self):
        assert_that(self.grid.get_x_center(0), is_(equal_to(-4.75)))
        assert_that(self.grid.get_x_center(17), is_(equal_to(3.75)))

    def test_get_y_center(self):
        assert_that(self.grid.get_y_center(0), is_(equal_to(-2.375)))
        assert_that(self.grid.get_y_center(3), is_(equal_to(1.375)))

    def test_repr(self):
        assert_that(repr(self.grid), is_(equal_to("Grid(x: -5.0000..4.0000 (0.5000), y: -3.0000..2.0000 (1.2500))")))


class TestRaster(TestCase):
    def setUp(self):
        self.reference_time = datetime.datetime.utcnow()
        self.raster = blitzortung.geom.Raster(-5, 4, -3, 2, 0.5, 1.25)

    def test_empty_raster(self):
        for x_index in range(0, self.raster.get_x_bin_count()):
            for y_index in range(0, self.raster.get_y_bin_count()):
                assert_that(self.raster.get(x_index, y_index), is_(none()))

    def test_empty_raster_to_arcgrid(self):
        assert_that(self.raster.to_arcgrid(), is_(equal_to("""NCOLS 18
NROWS 4
XLLCORNER -5.0000
YLLCORNER -3.0000
CELLSIZE 0.5000
NODATA_VALUE 0
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0""")))

    def test_empty_raster_to_map(self):
        assert_that(self.raster.to_map(), is_(equal_to("""--------------------
|                  |
|                  |
|                  |
|                  |
--------------------
total count: 0, max per area: 0""")))

    def test_empty_raster_to_reduced_array(self):
        assert_that(self.raster.to_reduced_array(self.reference_time), is_(equal_to([])))

    def add_raster_data(self):
        self.raster.set(0, 0, blitzortung.geom.RasterElement(5, self.reference_time - datetime.timedelta(minutes=2)))
        self.raster.set(1, 1, blitzortung.geom.RasterElement(10, self.reference_time - datetime.timedelta(seconds=10)))
        self.raster.set(4, 2, blitzortung.geom.RasterElement(20, self.reference_time - datetime.timedelta(hours=1)))

    def test_raster_to_arcgrid(self):
        self.add_raster_data()
        assert_that(self.raster.to_arcgrid(), is_(equal_to("""NCOLS 18
NROWS 4
XLLCORNER -5.0000
YLLCORNER -3.0000
CELLSIZE 0.5000
NODATA_VALUE 0
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 20 0 0 0 0 0 0 0 0 0 0 0 0 0
0 10 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
5 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0""")))

    def test_raster_to_map(self):
        self.add_raster_data()
        assert_that(self.raster.to_map(), is_(equal_to("""--------------------
|                  |
|    8             |
| o                |
|-                 |
--------------------
total count: 35, max per area: 20""")))

    def test_raster_to_reduced_array(self):
        self.add_raster_data()
        assert_that(self.raster.to_reduced_array(self.reference_time), is_(equal_to(
            [[4, 1, 20, -3600], [1, 2, 10, -10], [0, 3, 5, -120]]
        )))

    def test_raster_set_outside_valid_index_value_does_not_throw_exception(self):
        self.raster.set(1000, 0, blitzortung.geom.RasterElement(20, self.reference_time - datetime.timedelta(hours=1)))
        assert_that(self.raster.to_reduced_array(self.reference_time), is_(equal_to([])))


class TestRasterElement(TestCase):
    def setUp(self):
        self.timestamp = datetime.datetime(2013, 9, 6, 21, 36, 0, 123456)
        self.raster_element = blitzortung.geom.RasterElement(1234, self.timestamp)

    def test_get_count(self):
        assert_that(self.raster_element.get_count(), is_(equal_to(1234)))

    def test_get_timestamp(self):
        assert_that(self.raster_element.get_timestamp(), is_(equal_to(self.timestamp)))

    def test_comparison(self):
        other_raster_element = blitzortung.geom.RasterElement(10, self.timestamp)

        assert_that(other_raster_element < self.raster_element)
        assert_that(self.raster_element > other_raster_element)

    def test_string_representation(self):
        assert_that(repr(self.raster_element), is_(equal_to("RasterElement(1234, 2013-09-06 21:36:00.123456)")))