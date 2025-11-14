# -*- coding: utf8 -*-

"""

Copyright 2014-2016 Andreas Würl

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

import math

import pytest

import blitzortung.base


class TestPoint:
    """Test suite for Point class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.point1 = blitzortung.base.Point(11, 49)
        self.point2 = blitzortung.base.Point(12, 49)
        self.point3 = blitzortung.base.Point(11, 50)
        self.radians_factor = math.pi / 180

    def test_get_coordinate_components(self):
        """Test getting x and y coordinate components."""
        assert self.point1.x == 11
        assert self.point1.y == 49

    def test_get_azimuth(self):
        """Test azimuth calculation between points."""
        assert self.point1.azimuth_to(self.point2) == pytest.approx(
            89.62264107 * self.radians_factor
        )
        assert self.point1.azimuth_to(self.point3) == 0

    def test_get_distance(self):
        """Test distance calculation between points."""
        assert self.point1.distance_to(self.point2) == pytest.approx(73171.2643568)
        assert self.point1.distance_to(self.point3) == pytest.approx(
            111219.409, abs=1e-3
        )

    def test_get_geodesic_relation(self):
        """Test geodesic relation (azimuth and distance) between points."""
        azimuth, distance = self.point1.geodesic_relation_to(self.point2)
        assert azimuth == pytest.approx(89.62264107 * self.radians_factor)
        assert distance == pytest.approx(73171.2643568)

    def test_geodesic_shift(self):
        """Test shifting a point by azimuth and distance."""
        point = self.point1.geodesic_shift(0, 100000)
        assert point.x == pytest.approx(11.0)
        assert point.y == pytest.approx(49.8991315)

    def test_to_string(self):
        """Test string representation of a point."""
        assert str(self.point1) == "(11.0000, 49.0000)"
