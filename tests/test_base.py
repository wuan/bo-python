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
from blitzortung.base import EqualityAndHash, Point


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

    def test_point_from_another_point(self):
        """Test initializing Point from another Point."""
        point_copy = Point(self.point1)
        assert point_copy.x == self.point1.x
        assert point_copy.y == self.point1.y

    def test_point_with_single_arg(self):
        """Test Point initialization with single argument."""
        point = Point(10.5)
        assert point.x == 10.5
        assert point.y == 0.0

    def test_point_equality_same_coordinates(self):
        """Test equality of points with same coordinates."""
        p1 = Point(11, 49)
        p2 = Point(11, 49)
        assert p1 == p2

    def test_point_equality_different_coordinates(self):
        """Test inequality of points with different coordinates."""
        p1 = Point(11, 49)
        p2 = Point(12, 49)
        assert p1 != p2

    def test_point_equality_different_type(self):
        """Test equality comparison with different type."""
        p = Point(11, 49)
        assert (p == "not a point") is False

    def test_point_hash(self):
        """Test hash of points."""
        p1 = Point(11, 49)
        p2 = Point(11, 49)
        assert hash(p1) == hash(p2)

    def test_point_equal_static_method(self):
        """Test Point.equal static method."""
        assert Point.equal(1.0, 1.00001) is True
        assert Point.equal(1.0, 1.001) is False


class TestEqualityAndHash:
    """Test suite for EqualityAndHash mixin class."""

    def test_equality_same_dict(self):
        """Test equality of objects with same __dict__."""
        obj1 = EqualityAndHash()
        obj1.value = 42
        obj2 = EqualityAndHash()
        obj2.value = 42
        assert obj1 == obj2

    def test_equality_different_dict(self):
        """Test inequality of objects with different __dict__."""
        obj1 = EqualityAndHash()
        obj1.value = 42
        obj2 = EqualityAndHash()
        obj2.value = 100
        assert obj1 != obj2

    def test_equality_different_type(self):
        """Test equality with different type."""
        obj = EqualityAndHash()
        assert (obj == "string") is False
        assert (obj != "string") is True

    def test_hash_same_dict(self):
        """Test hash of objects with same __dict__."""
        obj1 = EqualityAndHash()
        obj1.value = 42
        obj2 = EqualityAndHash()
        obj2.value = 42
        assert hash(obj1) == hash(obj2)

    def test_hash_different_dict(self):
        """Test hash of objects with different __dict__."""
        obj1 = EqualityAndHash()
        obj1.value = 42
        obj2 = EqualityAndHash()
        obj2.value = 100
        assert hash(obj1) != hash(obj2)
