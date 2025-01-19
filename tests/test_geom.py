# -*- coding: utf8 -*-

"""

   Copyright 2014-2022 Andreas Würl

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

"""

import datetime
from unittest import TestCase

import pyproj
import pytest
import shapely.geometry
from assertpy import assert_that

import blitzortung.geom
import blitzortung.base


class GeometryForTest(blitzortung.geom.Geometry):
    def __init__(self, srid=None):
        if srid:
            super().__init__(srid)
        else:
            super().__init__()

    def get_env(self):
        return None


class TestGeometry(TestCase):
    def setUp(self):
        self.geometry = GeometryForTest()

    def test_default_values(self):
        assert_that(self.geometry.srid).is_equal_to(blitzortung.geom.Geometry.default_srid)

    def test_create_with_different_srid(self):
        self.geometry = GeometryForTest(1234)

        assert_that(self.geometry.srid).is_equal_to(1234)


class TestEnvelope(TestCase):
    def setUp(self):
        self.envelope = blitzortung.geom.Envelope(-5, 4, -3, 2)

    def test_default_values(self):
        assert_that(self.envelope.srid).is_equal_to(blitzortung.geom.Geometry.default_srid)

    def test_custom_srid_value(self):
        self.envelope = blitzortung.geom.Envelope(-5, 4, -3, 2, 1234)
        assert_that(self.envelope.srid).is_equal_to(1234)

    def test_get_envelope_coordinate_components(self):
        assert_that(self.envelope.x_min).is_equal_to(-5)
        assert_that(self.envelope.x_max).is_equal_to(4)
        assert_that(self.envelope.y_min).is_equal_to(-3)
        assert_that(self.envelope.y_max).is_equal_to(2)

    def test_get_envelope_parameters(self):
        assert_that(self.envelope.x_delta).is_equal_to(9)
        assert_that(self.envelope.y_delta).is_equal_to(5)

    def test_contains_point_inside_envelope(self):
        self.assertTrue(self.envelope.contains(blitzortung.base.Point(0, 0)))
        self.assertTrue(self.envelope.contains(blitzortung.base.Point(1, 1.5)))
        self.assertTrue(self.envelope.contains(blitzortung.base.Point(-1, -1.5)))

    def test_contains_point_on_border(self):
        self.assertTrue(self.envelope.contains(blitzortung.base.Point(0, -3)))
        self.assertTrue(self.envelope.contains(blitzortung.base.Point(0, 2)))
        self.assertTrue(self.envelope.contains(blitzortung.base.Point(-5, 0)))
        self.assertTrue(self.envelope.contains(blitzortung.base.Point(4, 0)))

    def test_does_not_contain_point_outside_border(self):
        self.assertFalse(self.envelope.contains(blitzortung.base.Point(0, -3.0001)))
        self.assertFalse(self.envelope.contains(blitzortung.base.Point(0, 2.0001)))
        self.assertFalse(self.envelope.contains(blitzortung.base.Point(-5.0001, 0)))
        self.assertFalse(self.envelope.contains(blitzortung.base.Point(4.0001, 0)))

    def test_get_env(self):
        expected_env = shapely.geometry.LinearRing([(-5, -3), (-5, 2), (4, 2), (4, -3)])
        self.assertTrue(expected_env.equals(self.envelope.env))

    def test_str(self):
        assert_that(repr(self.envelope)).is_equal_to('Envelope(x: -5.0000..4.0000, y: -3.0000..2.0000)')


class TestGrid(TestCase):
    def setUp(self):
        self.grid = blitzortung.geom.Grid(-5, 4, -3, 2, 0.5, 1.25)

    def test_get_x_div(self):
        assert_that(self.grid.x_div).is_equal_to(0.5)

    def test_get_y_div(self):
        assert_that(self.grid.y_div).is_equal_to(1.25)

    def test_get_x_bin_count(self):
        assert_that(self.grid.x_bin_count).is_equal_to(18)

    def test_get_y_bin_count(self):
        assert_that(self.grid.y_bin_count).is_equal_to(4)

    def test_get_x_bin(self):
        assert_that(self.grid.get_x_bin(-5)).is_equal_to(-1)
        assert_that(self.grid.get_x_bin(-4.9999)).is_equal_to(0)
        assert_that(self.grid.get_x_bin(-4.5)).is_equal_to(0)
        assert_that(self.grid.get_x_bin(-4.4999)).is_equal_to(1)
        assert_that(self.grid.get_x_bin(4)).is_equal_to(17)
        assert_that(self.grid.get_x_bin(4.0001)).is_equal_to(18)

    def test_get_y_bin(self):
        assert_that(self.grid.get_y_bin(-3)).is_equal_to(-1)
        assert_that(self.grid.get_y_bin(-2.9999)).is_equal_to(0)
        assert_that(self.grid.get_y_bin(-1.7500)).is_equal_to(0)
        assert_that(self.grid.get_y_bin(-1.7499)).is_equal_to(1)
        assert_that(self.grid.get_y_bin(2)).is_equal_to(3)
        assert_that(self.grid.get_y_bin(2.0001)).is_equal_to(4)

    def test_get_x_center(self):
        assert_that(self.grid.get_x_center(0)).is_equal_to(-4.75)
        assert_that(self.grid.get_x_center(17)).is_equal_to(3.75)

    def test_get_y_center(self):
        assert_that(self.grid.get_y_center(0)).is_equal_to(-2.375)
        assert_that(self.grid.get_y_center(3)).is_equal_to(1.375)

    def test_repr(self):
        assert_that(repr(self.grid)).is_equal_to(
            "Grid(x: -5.0000..4.0000 (0.5000, #18), y: -3.0000..2.0000 (1.2500, #4))")


class TestGridFactory:
    @pytest.fixture
    def base_proj(self):
        return pyproj.CRS('epsg:4326')

    @pytest.fixture
    def proj(self):
        return pyproj.CRS('epsg:32633')

    @pytest.fixture
    def default_factory(self, proj):
        return blitzortung.geom.GridFactory(10, 11, 52, 53, proj)

    @pytest.fixture
    def default_grid(self, default_factory, base_length):
        return default_factory.get_for(base_length)

    @pytest.fixture
    def base_length(self):
        return 5000

    @pytest.fixture
    def epsilon(self):
        return 1e-4

    def test_get_for_srid(self, default_grid):
        assert_that(default_grid.srid).is_equal_to(4326)

    def test_grid_boundaries(self, default_grid, base_proj, proj, base_length, epsilon):
        grid = default_grid

        x_div = grid.x_div
        y_div = grid.y_div

        assert_that(grid.x_min).is_equal_to(10)
        assert_that(grid.y_min).is_equal_to(52)

        assert_that(grid.x_max).is_close_to(10.9648, epsilon)
        assert_that(grid.y_max).is_close_to(52.9994, epsilon)
        assert_that(x_div).is_close_to(0.0689, epsilon)
        assert_that(y_div).is_close_to(0.0475, epsilon)

        x_0, y_0 = pyproj.Transformer.from_proj(base_proj, proj).transform(52.5, 10.5)
        x_1, y_1 = pyproj.Transformer.from_proj(base_proj, proj).transform(52.5 + y_div, 10.5 + x_div)

        assert_that(x_1 - x_0).is_close_to(base_length, epsilon)
        assert_that(y_1 - y_0).is_close_to(base_length, epsilon)

    def test_get_for_cache(self, default_factory, base_length):
        grid_1 = default_factory.get_for(base_length)
        grid_2 = default_factory.get_for(base_length)

        assert_that(grid_1).is_same_as(grid_2)

    def test_grid_outside_upper_range(self, base_length, proj, epsilon):
        factory =  blitzortung.geom.GridFactory(14, 16, 70, 95, proj)

        grid = factory.get_for(base_length)

        assert_that(grid.x_delta).is_close_to(1.8134, epsilon)
        assert_that(grid.y_delta).is_close_to(19.9795, epsilon)

    def test_grid_outside_lower_range(self, base_length, proj, epsilon):
        factory =  blitzortung.geom.GridFactory(14, 16, -95, -70, pyproj.CRS('epsg:32733'))

        grid = factory.get_for(base_length)

        assert_that(grid.x_delta).is_close_to(1.7974, epsilon)
        assert_that(grid.y_delta).is_close_to(19.9785, epsilon)


class TestRasterElement(TestCase):
    def setUp(self):
        self.timestamp = datetime.datetime(2013, 9, 6, 21, 36, 0, 123456)
        self.raster_element = blitzortung.geom.GridElement(1234, self.timestamp)

    def test_count(self):
        assert_that(self.raster_element.count).is_equal_to(1234)

    def test_timestamp(self):
        assert_that(self.raster_element.timestamp).is_equal_to(self.timestamp)

    def test_comparison(self):
        other_raster_element = blitzortung.geom.GridElement(10, self.timestamp)

        assert_that(other_raster_element < self.raster_element)
        assert_that(self.raster_element > other_raster_element)

    def test_string_representation(self):
        assert_that(repr(self.raster_element)).is_equal_to("GridElement(1234, 2013-09-06 21:36:00.123456)")
