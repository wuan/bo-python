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

import pyproj
import pytest
import shapely.geometry
from assertpy import assert_that

import blitzortung.geom
import blitzortung.base


class GeometryForTest(blitzortung.geom.Geometry):
    """Test helper class for Geometry."""

    def __init__(self, srid=None):
        if srid:
            super().__init__(srid)
        else:
            super().__init__()

    def get_env(self):
        """Return environment."""
        return None


class TestGeometry:
    """Test suite for Geometry class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.geometry = GeometryForTest()

    def test_default_values(self):
        """Test default SRID value."""
        assert_that(self.geometry.srid).is_equal_to(
            blitzortung.geom.Geometry.default_srid
        )

    def test_create_with_different_srid(self):
        """Test creating geometry with custom SRID."""
        self.geometry = GeometryForTest(1234)
        assert_that(self.geometry.srid).is_equal_to(1234)


class TestEnvelope:
    """Test suite for Envelope class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.envelope = blitzortung.geom.Envelope(-5, 4, -3, 2)

    def test_default_values(self):
        """Test default SRID value."""
        assert_that(self.envelope.srid).is_equal_to(
            blitzortung.geom.Geometry.default_srid
        )

    def test_custom_srid_value(self):
        """Test custom SRID value."""
        self.envelope = blitzortung.geom.Envelope(-5, 4, -3, 2, 1234)
        assert_that(self.envelope.srid).is_equal_to(1234)

    def test_get_envelope_coordinate_components(self):
        """Test envelope coordinate components."""
        assert_that(self.envelope.x_min).is_equal_to(-5)
        assert_that(self.envelope.x_max).is_equal_to(4)
        assert_that(self.envelope.y_min).is_equal_to(-3)
        assert_that(self.envelope.y_max).is_equal_to(2)

    def test_get_envelope_parameters(self):
        """Test envelope delta parameters."""
        assert_that(self.envelope.x_delta).is_equal_to(9)
        assert_that(self.envelope.y_delta).is_equal_to(5)

    def test_contains_point_inside_envelope(self):
        """Test containing points inside envelope."""
        assert self.envelope.contains(blitzortung.base.Point(0, 0))
        assert self.envelope.contains(blitzortung.base.Point(1, 1.5))
        assert self.envelope.contains(blitzortung.base.Point(-1, -1.5))

    def test_contains_point_on_border(self):
        """Test containing points on envelope border."""
        assert self.envelope.contains(blitzortung.base.Point(0, -3))
        assert self.envelope.contains(blitzortung.base.Point(0, 2))
        assert self.envelope.contains(blitzortung.base.Point(-5, 0))
        assert self.envelope.contains(blitzortung.base.Point(4, 0))

    def test_does_not_contain_point_outside_border(self):
        """Test not containing points outside envelope."""
        assert not self.envelope.contains(blitzortung.base.Point(0, -3.0001))
        assert not self.envelope.contains(blitzortung.base.Point(0, 2.0001))
        assert not self.envelope.contains(blitzortung.base.Point(-5.0001, 0))
        assert not self.envelope.contains(blitzortung.base.Point(4.0001, 0))

    def test_get_env(self):
        """Test getting shapely polygon."""
        expected_env = shapely.geometry.Polygon(
            [
                (-5, -3),
                (-5, 2),
                (4, 2),
                (4, -3),
                (-5, -3),
            ]
        )
        assert expected_env.equals(self.envelope.env)

    def test_str(self):
        """Test string representation."""
        assert_that(repr(self.envelope)).is_equal_to(
            "Envelope(x: -5.0000..4.0000, y: -3.0000..2.0000)"
        )


class TestGrid:
    """Test suite for Grid class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.grid = blitzortung.geom.Grid(-5, 4, -3, 2, 0.5, 1.25)

    def test_get_x_div(self):
        """Test x division size."""
        assert_that(self.grid.x_div).is_equal_to(0.5)

    def test_get_y_div(self):
        """Test y division size."""
        assert_that(self.grid.y_div).is_equal_to(1.25)

    def test_get_x_bin_count(self):
        """Test x bin count."""
        assert_that(self.grid.x_bin_count).is_equal_to(18)

    def test_get_y_bin_count(self):
        """Test y bin count."""
        assert_that(self.grid.y_bin_count).is_equal_to(4)

    def test_get_x_bin(self):
        """Test x bin calculation."""
        assert_that(self.grid.get_x_bin(-5)).is_equal_to(-1)
        assert_that(self.grid.get_x_bin(-4.9999)).is_equal_to(0)
        assert_that(self.grid.get_x_bin(-4.5)).is_equal_to(0)
        assert_that(self.grid.get_x_bin(-4.4999)).is_equal_to(1)
        assert_that(self.grid.get_x_bin(4)).is_equal_to(17)
        assert_that(self.grid.get_x_bin(4.0001)).is_equal_to(18)

    def test_get_y_bin(self):
        """Test y bin calculation."""
        assert_that(self.grid.get_y_bin(-3)).is_equal_to(-1)
        assert_that(self.grid.get_y_bin(-2.9999)).is_equal_to(0)
        assert_that(self.grid.get_y_bin(-1.7500)).is_equal_to(0)
        assert_that(self.grid.get_y_bin(-1.7499)).is_equal_to(1)
        assert_that(self.grid.get_y_bin(2)).is_equal_to(3)
        assert_that(self.grid.get_y_bin(2.0001)).is_equal_to(4)

    def test_get_x_center(self):
        """Test x center coordinate."""
        assert_that(self.grid.get_x_center(0)).is_equal_to(-4.75)
        assert_that(self.grid.get_x_center(17)).is_equal_to(3.75)

    def test_get_y_center(self):
        """Test y center coordinate."""
        assert_that(self.grid.get_y_center(0)).is_equal_to(-2.375)
        assert_that(self.grid.get_y_center(3)).is_equal_to(1.375)

    def test_repr(self):
        """Test string representation."""
        assert_that(repr(self.grid)).is_equal_to(
            "Grid(x: -5.0000..4.0000 (0.5000, #18), y: -3.0000..2.0000 (1.2500, #4))"
        )


class TestGridFactory:
    """Test suite for GridFactory class."""

    @pytest.fixture
    def base_proj(self):
        """Fixture for base projection."""
        return pyproj.CRS("epsg:4326")

    @pytest.fixture
    def proj(self):
        """Fixture for working projection."""
        return pyproj.CRS("epsg:32633")

    @pytest.fixture
    def base_length(self):
        """Fixture for base length."""
        return 5000

    @pytest.fixture
    def epsilon(self):
        """Fixture for epsilon precision."""
        return 1e-4

    @pytest.fixture
    def default_factory(self, proj):
        """Fixture for default grid factory."""
        return blitzortung.geom.GridFactory(10, 11, 52, 53, proj)

    @pytest.fixture
    def default_grid(self, default_factory, base_length):
        """Fixture for default grid."""
        return default_factory.get_for(base_length)

    def test_get_for_srid(self, default_grid):
        """Test grid SRID."""
        assert_that(default_grid.srid).is_equal_to(4326)

    def test_grid_boundaries(self, default_grid, base_proj, proj, base_length, epsilon):
        """Test grid boundaries and cell sizes."""
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
        x_1, y_1 = pyproj.Transformer.from_proj(base_proj, proj).transform(
            52.5 + y_div, 10.5 + x_div
        )

        assert_that(x_1 - x_0).is_close_to(base_length, epsilon)
        assert_that(y_1 - y_0).is_close_to(base_length, epsilon)

    def test_get_for_cache(self, default_factory, base_length):
        """Test grid caching."""
        grid_1 = default_factory.get_for(base_length)
        grid_2 = default_factory.get_for(base_length)

        assert_that(grid_1).is_same_as(grid_2)

    def test_grid_outside_upper_range(self, base_length, proj, epsilon):
        """Test grid in upper latitude range."""
        factory = blitzortung.geom.GridFactory(14, 16, 70, 95, proj)

        grid = factory.get_for(base_length)

        assert_that(grid.x_delta).is_close_to(1.8134, epsilon)
        assert_that(grid.y_delta).is_close_to(19.9795, epsilon)

    def test_grid_outside_lower_range(self, base_length, proj, epsilon):
        """Test grid in lower latitude range."""
        factory = blitzortung.geom.GridFactory(
            14, 16, -95, -70, pyproj.CRS("epsg:32733")
        )

        grid = factory.get_for(base_length)

        assert_that(grid.x_delta).is_close_to(1.7974, epsilon)
        assert_that(grid.y_delta).is_close_to(19.9785, epsilon)


class TestRasterElement:
    """Test suite for GridElement class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.timestamp = datetime.datetime(2013, 9, 6, 21, 36, 0, 123456)
        self.raster_element = blitzortung.geom.GridElement(1234, self.timestamp)

    def test_count(self):
        """Test element count."""
        assert_that(self.raster_element.count).is_equal_to(1234)

    def test_timestamp(self):
        """Test element timestamp."""
        assert_that(self.raster_element.timestamp).is_equal_to(self.timestamp)

    def test_comparison(self):
        """Test element comparison."""
        other_raster_element = blitzortung.geom.GridElement(10, self.timestamp)

        assert_that(other_raster_element < self.raster_element)
        assert_that(self.raster_element > other_raster_element)

    def test_string_representation(self):
        """Test string representation."""
        assert_that(repr(self.raster_element)).is_equal_to(
            "GridElement(1234, 2013-09-06 21:36:00.123456)"
        )
