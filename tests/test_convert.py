# -*- coding: utf8 -*-

"""

   Copyright 2025 Andreas Würl

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

from assertpy import assert_that

import blitzortung.convert


class TestValueToString:

    def test_float_with_four_decimals(self):
        assert_that(blitzortung.convert.value_to_string(3.14159)).is_equal_to("3.1416")

    def test_float_zero(self):
        assert_that(blitzortung.convert.value_to_string(0.0)).is_equal_to("0.0000")

    def test_negative_float(self):
        assert_that(blitzortung.convert.value_to_string(-2.71828)).is_equal_to("-2.7183")

    def test_large_float(self):
        assert_that(blitzortung.convert.value_to_string(123456.789)).is_equal_to("123456.7890")

    def test_non_float(self):
        assert_that(blitzortung.convert.value_to_string(42)).is_equal_to("42")
