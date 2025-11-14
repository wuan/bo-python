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

from assertpy import assert_that  # pylint: disable=import-error

import blitzortung.convert


class TestValueToString:
    """Test suite for value_to_string conversion utility."""

    def test_float_formatting_with_four_decimals(self):
        """Test float values are formatted with 4 decimal places."""
        assert_that(blitzortung.convert.value_to_string(3.14159)).is_equal_to("3.1416")

    def test_float_zero(self):
        """Test float zero is formatted correctly."""
        assert_that(blitzortung.convert.value_to_string(0.0)).is_equal_to("0.0000")

    def test_integer_converted_to_string(self):
        """Test integer values are converted to string."""
        assert_that(blitzortung.convert.value_to_string(42)).is_equal_to("42")

    def test_string_passed_through(self):
        """Test string values are passed through unchanged."""
        assert_that(
            blitzortung.convert.value_to_string("already a string")
        ).is_equal_to("already a string")

    def test_negative_float(self):
        """Test negative float formatting."""
        assert_that(blitzortung.convert.value_to_string(-2.71828)).is_equal_to(
            "-2.7183"
        )

    def test_very_small_float(self):
        """Test very small float is formatted correctly."""
        assert_that(blitzortung.convert.value_to_string(0.0001)).is_equal_to("0.0001")

    def test_very_large_float(self):
        """Test very large float is formatted correctly."""
        assert_that(blitzortung.convert.value_to_string(123456.789)).is_equal_to(
            "123456.7890"
        )

    def test_scientific_notation_float(self):
        """Test float with scientific notation."""
        result = blitzortung.convert.value_to_string(1.23e-5)
        assert_that(result).is_equal_to("0.0000")

    def test_none_value(self):
        """Test None value is converted to string."""
        assert_that(blitzortung.convert.value_to_string(None)).is_equal_to("None")

    def test_bool_true(self):
        """Test boolean True is converted to string."""
        assert_that(blitzortung.convert.value_to_string(True)).is_equal_to("True")

    def test_bool_false(self):
        """Test boolean False is converted to string."""
        assert_that(blitzortung.convert.value_to_string(False)).is_equal_to("False")

    def test_float_infinity(self):
        """Test infinity float formatting."""
        result = blitzortung.convert.value_to_string(float("inf"))
        assert_that(result).is_equal_to("inf")

    def test_float_negative_infinity(self):
        """Test negative infinity float formatting."""
        result = blitzortung.convert.value_to_string(float("-inf"))
        assert_that(result).is_equal_to("-inf")

    def test_float_nan(self):
        """Test NaN float formatting."""
        result = blitzortung.convert.value_to_string(float("nan"))
        assert_that(result).is_equal_to("nan")
