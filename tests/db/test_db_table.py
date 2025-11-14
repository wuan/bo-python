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
import pytest  # pylint: disable=import-error

import blitzortung.db.table


class TestDatabaseTableBase:
    """Test suite for db.table.Base class."""

    def test_base_has_default_timezone(self):
        """Test that Base has default timezone as UTC."""
        assert_that(blitzortung.db.table.Base.default_timezone).is_equal_to(
            datetime.timezone.utc
        )

    def test_full_table_name_without_schema(self):
        """Test full_table_name property without schema."""
        # We can't test this fully without a real DB connection, but we can test the concept
        # by checking the method exists
        assert_that(hasattr(blitzortung.db.table.Base, "full_table_name")).is_true()

    def test_strike_table_name(self):
        """Test that Strike class has correct table name."""
        assert_that(blitzortung.db.table.Strike.table_name).is_equal_to("strikes")

    def test_timezone_property_exists(self):
        """Test that Base has timezone property methods."""
        assert_that(hasattr(blitzortung.db.table.Base, "get_timezone")).is_true()
        assert_that(hasattr(blitzortung.db.table.Base, "set_timezone")).is_true()

    def test_srid_property_exists(self):
        """Test that Base has SRID property methods."""
        assert_that(hasattr(blitzortung.db.table.Base, "get_srid")).is_true()
        assert_that(hasattr(blitzortung.db.table.Base, "set_srid")).is_true()

    def test_connection_check_method_exists(self):
        """Test that Base has is_connected method."""
        assert_that(hasattr(blitzortung.db.table.Base, "is_connected")).is_true()

    def test_transaction_methods_exist(self):
        """Test that Base has transaction methods."""
        assert_that(hasattr(blitzortung.db.table.Base, "commit")).is_true()
        assert_that(hasattr(blitzortung.db.table.Base, "rollback")).is_true()

    def test_execute_methods_exist(self):
        """Test that Base has various execute methods."""
        assert_that(hasattr(blitzortung.db.table.Base, "execute")).is_true()
        assert_that(hasattr(blitzortung.db.table.Base, "execute_single")).is_true()
        assert_that(hasattr(blitzortung.db.table.Base, "execute_many")).is_true()

    def test_timezone_conversion_methods_exist(self):
        """Test that Base has timezone conversion methods."""
        assert_that(hasattr(blitzortung.db.table.Base, "fix_timezone")).is_true()
        assert_that(
            hasattr(blitzortung.db.table.Base, "from_bare_utc_to_timezone")
        ).is_true()
        assert_that(
            hasattr(blitzortung.db.table.Base, "from_timezone_to_bare_utc")
        ).is_true()


class TestStrikeTable:
    """Test suite for db.table.Strike class."""

    def test_strike_inherits_from_base(self):
        """Test that Strike inherits from Base."""
        assert_that(
            issubclass(blitzortung.db.table.Strike, blitzortung.db.table.Base)
        ).is_true()

    def test_strike_table_name_is_strikes(self):
        """Test that Strike table name is 'strikes'."""
        assert_that(blitzortung.db.table.Strike.table_name).is_equal_to("strikes")

    def test_strike_has_insert_method(self):
        """Test that Strike has insert method."""
        assert_that(hasattr(blitzortung.db.table.Strike, "insert")).is_true()
        assert_that(callable(blitzortung.db.table.Strike.insert)).is_true()

    def test_strike_has_select_method(self):
        """Test that Strike has select method."""
        assert_that(hasattr(blitzortung.db.table.Strike, "select")).is_true()
        assert_that(callable(blitzortung.db.table.Strike.select)).is_true()

    def test_strike_has_grid_methods(self):
        """Test that Strike has grid query methods."""
        assert_that(hasattr(blitzortung.db.table.Strike, "select_grid")).is_true()
        assert_that(
            hasattr(blitzortung.db.table.Strike, "select_global_grid")
        ).is_true()

    def test_strike_has_histogram_method(self):
        """Test that Strike has histogram method."""
        assert_that(hasattr(blitzortung.db.table.Strike, "select_histogram")).is_true()

    def test_strike_has_get_latest_time_method(self):
        """Test that Strike has get_latest_time method."""
        assert_that(hasattr(blitzortung.db.table.Strike, "get_latest_time")).is_true()

    def test_strike_initialization_requires_connection_pool(self):
        """Test that Strike initialization signature includes db_connection_pool."""
        import inspect

        sig = inspect.signature(blitzortung.db.table.Strike.__init__)
        param_names = list(sig.parameters.keys())
        # Should have self and db_connection_pool (plus other injected deps)
        assert_that("db_connection_pool" in param_names).is_true()

    def test_strike_initialization_requires_query_builder(self):
        """Test that Strike initialization includes query_builder."""
        import inspect

        sig = inspect.signature(blitzortung.db.table.Strike.__init__)
        param_names = list(sig.parameters.keys())
        # Should include query builder parameter
        assert_that(any("query_builder" in p for p in param_names)).is_true()

    def test_strike_initialization_requires_mapper(self):
        """Test that Strike initialization includes strike_mapper."""
        import inspect

        sig = inspect.signature(blitzortung.db.table.Strike.__init__)
        param_names = list(sig.parameters.keys())
        # Should include mapper parameter
        assert_that(any("mapper" in p for p in param_names)).is_true()
