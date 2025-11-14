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

import logging

from assertpy import assert_that  # pylint: disable=import-error

import blitzortung.logger


class TestCreateConsoleHandler:
    """Test suite for console handler creation."""

    def test_creates_stream_handler(self):
        """Test that a StreamHandler is created."""
        handler = blitzortung.logger.create_console_handler()
        assert_that(handler).is_instance_of(logging.StreamHandler)

    def test_handler_has_formatter(self):
        """Test that handler has a formatter configured."""
        handler = blitzortung.logger.create_console_handler()
        assert_that(handler.formatter).is_not_none()

    def test_formatter_format_string(self):
        """Test that formatter uses the correct format string."""
        handler = blitzortung.logger.create_console_handler()
        formatter = handler.formatter
        # The format string should include timestamp, name, level, and message
        fmt = formatter._fmt  # pylint: disable=protected-access
        assert_that(fmt).contains("%(asctime)s")
        assert_that(fmt).contains("%(name)s")
        assert_that(fmt).contains("%(levelname)s")
        assert_that(fmt).contains("%(message)s")

    def test_formatter_produces_expected_output(self):
        """Test that formatter produces expected log output format."""
        handler = blitzortung.logger.create_console_handler()
        formatter = handler.formatter

        # Create a test log record
        record = logging.LogRecord(
            name="test.module",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="test message",
            args=(),
            exc_info=None,
        )

        formatted = formatter.format(record)  # pylint: disable=protected-access
        # Check that formatted message contains expected components
        assert_that(formatted).contains("test.module")
        assert_that(formatted).contains("INFO")
        assert_that(formatted).contains("test message")


class TestGetLoggerName:
    """Test suite for logger name generation."""

    def test_logger_name_from_class(self):
        """Test generating logger name from a class."""

        class TestClass:  # pylint: disable=missing-class-docstring,too-few-public-methods
            pass

        logger_name = blitzortung.logger.get_logger_name(TestClass)
        assert_that(logger_name).is_equal_to("tests.test_logger.TestClass")

    def test_logger_name_includes_module(self):
        """Test that logger name includes module path."""
        logger_name = blitzortung.logger.get_logger_name(
            blitzortung.logger.create_console_handler().__class__
        )
        assert_that(logger_name).contains("logging")

    def test_logger_name_format(self):
        """Test that logger name follows module.class format."""

        class MyTestClass:  # pylint: disable=missing-class-docstring,too-few-public-methods
            pass

        logger_name = blitzortung.logger.get_logger_name(MyTestClass)
        # Should be in format: module.ClassName
        assert_that(logger_name).contains(".")
        assert_that(logger_name).ends_with("MyTestClass")

    def test_logger_name_for_builtin_class(self):
        """Test logger name for built-in classes."""
        logger_name = blitzortung.logger.get_logger_name(dict)
        assert_that(logger_name).is_equal_to("builtins.dict")

    def test_logger_name_for_imported_class(self):
        """Test logger name for imported classes."""
        logger_name = blitzortung.logger.get_logger_name(logging.Logger)
        assert_that(logger_name).contains("Logger")
        assert_that(logger_name).contains("logging")
