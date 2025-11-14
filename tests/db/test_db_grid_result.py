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

import datetime

from assertpy import assert_that  # pylint: disable=import-error

import blitzortung.db.grid_result


class TestBuildGridResult:
    """Test suite for build_grid_result function."""

    def test_empty_results_returns_empty_tuple(self):
        """Test that empty results list returns empty tuple."""
        result = blitzortung.db.grid_result.build_grid_result(
            [],
            x_bin_count=10,
            y_bin_count=10,
            end_time=datetime.datetime.now(datetime.timezone.utc),
        )
        assert_that(result).is_empty()

    def test_single_valid_result(self):
        """Test building grid result from a single valid result."""
        end_time = datetime.datetime.now(datetime.timezone.utc)
        result_time = end_time - datetime.timedelta(seconds=10)

        results = [{"rx": 5, "ry": 5, "strike_count": 1, "timestamp": result_time}]

        grid_result = blitzortung.db.grid_result.build_grid_result(
            results, x_bin_count=10, y_bin_count=10, end_time=end_time
        )

        assert_that(grid_result).is_length(1)
        assert_that(grid_result[0][0]).is_equal_to(5)  # rx
        assert_that(grid_result[0][1]).is_equal_to(5)  # y_bin_count - ry = 10 - 5 = 5
        assert_that(grid_result[0][2]).is_equal_to(1)  # strike_count
        assert_that(grid_result[0][3]).is_equal_to(
            -10
        )  # -(end_time - timestamp).seconds

    def test_multiple_valid_results(self):
        """Test building grid result from multiple valid results."""
        end_time = datetime.datetime.now(datetime.timezone.utc)
        result_time_1 = end_time - datetime.timedelta(seconds=10)
        result_time_2 = end_time - datetime.timedelta(seconds=20)

        results = [
            {"rx": 0, "ry": 1, "strike_count": 5, "timestamp": result_time_1},
            {"rx": 9, "ry": 10, "strike_count": 3, "timestamp": result_time_2},
        ]

        grid_result = blitzortung.db.grid_result.build_grid_result(
            results, x_bin_count=10, y_bin_count=10, end_time=end_time
        )

        assert_that(grid_result).is_length(2)

        # First result
        assert_that(grid_result[0][0]).is_equal_to(0)  # rx
        assert_that(grid_result[0][1]).is_equal_to(9)  # y_bin_count - ry = 10 - 1 = 9
        assert_that(grid_result[0][2]).is_equal_to(5)  # strike_count
        assert_that(grid_result[0][3]).is_equal_to(
            -10
        )  # -(end_time - timestamp).seconds

        # Second result
        assert_that(grid_result[1][0]).is_equal_to(9)  # rx
        assert_that(grid_result[1][1]).is_equal_to(0)  # y_bin_count - ry = 10 - 10 = 0
        assert_that(grid_result[1][2]).is_equal_to(3)  # strike_count
        assert_that(grid_result[1][3]).is_equal_to(
            -20
        )  # -(end_time - timestamp).seconds

    def test_filters_out_of_bounds_x(self):
        """Test that results with rx outside [0, x_bin_count) are filtered."""
        end_time = datetime.datetime.now(datetime.timezone.utc)
        result_time = end_time - datetime.timedelta(seconds=5)

        results = [
            {"rx": -1, "ry": 5, "strike_count": 1, "timestamp": result_time},
            {"rx": 0, "ry": 5, "strike_count": 1, "timestamp": result_time},
            {"rx": 9, "ry": 5, "strike_count": 1, "timestamp": result_time},
            {"rx": 10, "ry": 5, "strike_count": 1, "timestamp": result_time},
        ]

        grid_result = blitzortung.db.grid_result.build_grid_result(
            results, x_bin_count=10, y_bin_count=10, end_time=end_time
        )

        # Only the two middle results are valid
        assert_that(grid_result).is_length(2)
        assert_that(grid_result[0][0]).is_equal_to(0)
        assert_that(grid_result[1][0]).is_equal_to(9)

    def test_filters_out_of_bounds_y(self):
        """Test that results with ry outside (0, y_bin_count] are filtered."""
        end_time = datetime.datetime.now(datetime.timezone.utc)
        result_time = end_time - datetime.timedelta(seconds=5)

        results = [
            {
                "rx": 5,
                "ry": 0,
                "strike_count": 1,
                "timestamp": result_time,
            },  # ry = 0 (invalid)
            {
                "rx": 5,
                "ry": 1,
                "strike_count": 1,
                "timestamp": result_time,
            },  # ry = 1 (valid)
            {
                "rx": 5,
                "ry": 10,
                "strike_count": 1,
                "timestamp": result_time,
            },  # ry = 10 (valid)
            {
                "rx": 5,
                "ry": 11,
                "strike_count": 1,
                "timestamp": result_time,
            },  # ry = 11 (invalid)
        ]

        grid_result = blitzortung.db.grid_result.build_grid_result(
            results, x_bin_count=10, y_bin_count=10, end_time=end_time
        )

        # Only two results with ry in (0, 10] are valid
        assert_that(grid_result).is_length(2)

    def test_y_coordinate_transformation(self):
        """Test that y coordinate is properly transformed as y_bin_count - ry."""
        end_time = datetime.datetime.now(datetime.timezone.utc)
        result_time = end_time - datetime.timedelta(seconds=1)

        results = [
            {"rx": 0, "ry": 1, "strike_count": 1, "timestamp": result_time},
            {"rx": 0, "ry": 5, "strike_count": 1, "timestamp": result_time},
            {"rx": 0, "ry": 10, "strike_count": 1, "timestamp": result_time},
        ]

        grid_result = blitzortung.db.grid_result.build_grid_result(
            results, x_bin_count=10, y_bin_count=10, end_time=end_time
        )

        # y coordinates should be: 10-1=9, 10-5=5, 10-10=0
        assert_that(grid_result[0][1]).is_equal_to(9)
        assert_that(grid_result[1][1]).is_equal_to(5)
        assert_that(grid_result[2][1]).is_equal_to(0)

    def test_time_delta_calculation(self):
        """Test that time delta is correctly calculated as negative seconds."""
        end_time = datetime.datetime.now(datetime.timezone.utc)

        results = [
            {
                "rx": 5,
                "ry": 5,
                "strike_count": 1,
                "timestamp": end_time - datetime.timedelta(seconds=30),
            },
            {
                "rx": 5,
                "ry": 5,
                "strike_count": 1,
                "timestamp": end_time - datetime.timedelta(seconds=0),
            },
            {
                "rx": 5,
                "ry": 5,
                "strike_count": 1,
                "timestamp": end_time - datetime.timedelta(seconds=120),
            },
        ]

        grid_result = blitzortung.db.grid_result.build_grid_result(
            results, x_bin_count=10, y_bin_count=10, end_time=end_time
        )

        assert_that(grid_result).is_length(3)
        # Time deltas should be negative
        assert_that(grid_result[0][3]).is_equal_to(-30)
        assert_that(grid_result[1][3]).is_equal_to(0)  # end_time equals result time
        assert_that(grid_result[2][3]).is_equal_to(-120)

    def test_strike_count_preserved(self):
        """Test that strike_count is preserved in output."""
        end_time = datetime.datetime.now(datetime.timezone.utc)
        result_time = end_time - datetime.timedelta(seconds=5)

        results = [
            {"rx": 0, "ry": 1, "strike_count": 0, "timestamp": result_time},
            {"rx": 1, "ry": 2, "strike_count": 5, "timestamp": result_time},
            {"rx": 2, "ry": 3, "strike_count": 100, "timestamp": result_time},
            {"rx": 3, "ry": 4, "strike_count": 999, "timestamp": result_time},
        ]

        grid_result = blitzortung.db.grid_result.build_grid_result(
            results, x_bin_count=10, y_bin_count=10, end_time=end_time
        )

        assert_that(grid_result).is_length(4)
        assert_that(grid_result[0][2]).is_equal_to(0)
        assert_that(grid_result[1][2]).is_equal_to(5)
        assert_that(grid_result[2][2]).is_equal_to(100)
        assert_that(grid_result[3][2]).is_equal_to(999)

    def test_returns_tuple_not_list(self):
        """Test that the result is a tuple, not a list."""
        end_time = datetime.datetime.now(datetime.timezone.utc)
        result_time = end_time - datetime.timedelta(seconds=1)

        results = [{"rx": 5, "ry": 5, "strike_count": 1, "timestamp": result_time}]

        grid_result = blitzortung.db.grid_result.build_grid_result(
            results, x_bin_count=10, y_bin_count=10, end_time=end_time
        )

        assert_that(grid_result).is_instance_of(tuple)
        assert_that(grid_result[0]).is_instance_of(tuple)

    def test_large_grid_dimensions(self):
        """Test with larger grid dimensions."""
        end_time = datetime.datetime.now(datetime.timezone.utc)
        result_time = end_time - datetime.timedelta(seconds=10)

        results = [
            {"rx": 0, "ry": 1, "strike_count": 1, "timestamp": result_time},
            {"rx": 255, "ry": 128, "strike_count": 2, "timestamp": result_time},
        ]

        grid_result = blitzortung.db.grid_result.build_grid_result(
            results, x_bin_count=256, y_bin_count=256, end_time=end_time
        )

        assert_that(grid_result).is_length(2)
        assert_that(grid_result[0][0]).is_equal_to(0)
        assert_that(grid_result[0][1]).is_equal_to(255)  # 256 - 1

        assert_that(grid_result[1][0]).is_equal_to(255)
        assert_that(grid_result[1][1]).is_equal_to(128)  # 256 - 128
