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

import os
import tempfile
from unittest.mock import Mock, patch, MagicMock

from assertpy import assert_that  # pylint: disable=import-error
import pytest  # pylint: disable=import-error

import blitzortung.dataimport.base


class TestFileTransport:
    """Test suite for FileTransport class."""

    def test_file_transport_is_transport_abstract(self):
        """Test that FileTransport is a subclass of TransportAbstract."""
        assert_that(
            issubclass(
                blitzortung.dataimport.base.FileTransport,
                blitzortung.dataimport.base.TransportAbstract,
            )
        ).is_true()

    def test_read_lines_from_nonexistent_file(self):
        """Test that read_lines returns nothing for nonexistent file."""
        transport = blitzortung.dataimport.base.FileTransport()
        result = list(transport.read_lines("/nonexistent/path/to/file.txt"))
        assert_that(result).is_empty()

    def test_read_lines_from_existing_file(self):
        """Test that read_lines yields lines from existing file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as tmp:
            tmp.write("line 1\n")
            tmp.write("line 2\n")
            tmp.write("line 3\n")
            tmp_path = tmp.name

        try:
            transport = blitzortung.dataimport.base.FileTransport()
            lines = list(transport.read_lines(tmp_path))
            assert_that(lines).is_length(3)
            assert_that(lines[0]).is_equal_to("line 1\n")
            assert_that(lines[1]).is_equal_to("line 2\n")
            assert_that(lines[2]).is_equal_to("line 3\n")
        finally:
            os.unlink(tmp_path)

    def test_read_lines_is_generator(self):
        """Test that read_lines returns a generator."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as tmp:
            tmp.write("line 1\n")
            tmp_path = tmp.name

        try:
            transport = blitzortung.dataimport.base.FileTransport()
            result = transport.read_lines(tmp_path)
            # Check if it's a generator
            assert_that(hasattr(result, "__iter__")).is_true()
            assert_that(hasattr(result, "__next__")).is_true()
        finally:
            os.unlink(tmp_path)

    def test_read_lines_with_post_process(self):
        """Test that post_process parameter is accepted."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as tmp:
            tmp.write("line 1\n")
            tmp_path = tmp.name

        try:
            transport = blitzortung.dataimport.base.FileTransport()
            result = list(transport.read_lines(tmp_path, post_process=lambda x: x))
            # FileTransport ignores post_process
            assert_that(result).is_length(1)
        finally:
            os.unlink(tmp_path)

    def test_read_lines_empty_file(self):
        """Test that read_lines handles empty file correctly."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as tmp:
            tmp_path = tmp.name

        try:
            transport = blitzortung.dataimport.base.FileTransport()
            lines = list(transport.read_lines(tmp_path))
            assert_that(lines).is_empty()
        finally:
            os.unlink(tmp_path)


class TestBlitzortungDataPath:
    """Test suite for BlitzortungDataPath class."""

    def test_initialization_with_default_base_path(self):
        """Test initialization with default base path."""
        path = blitzortung.dataimport.base.BlitzortungDataPath()
        assert_that(path.data_path).contains("https://{host_name}.blitzortung.org")

    def test_initialization_with_custom_base_path(self):
        """Test initialization with custom base path."""
        custom_path = "/custom/base"
        path = blitzortung.dataimport.base.BlitzortungDataPath(custom_path)
        assert_that(path.data_path).contains(custom_path)

    def test_default_host_name(self):
        """Test default host name is 'data'."""
        assert_that(
            blitzortung.dataimport.base.BlitzortungDataPath.default_host_name
        ).is_equal_to("data")

    def test_default_region(self):
        """Test default region is 1."""
        assert_that(
            blitzortung.dataimport.base.BlitzortungDataPath.default_region
        ).is_equal_to(1)

    def test_build_path_with_defaults(self):
        """Test build_path with default parameters."""
        path = blitzortung.dataimport.base.BlitzortungDataPath()
        result = path.build_path("subpath")
        assert_that(result).contains("data.blitzortung.org")
        assert_that(result).contains("subpath")

    def test_build_path_with_custom_host_name(self):
        """Test build_path with custom host name."""
        path = blitzortung.dataimport.base.BlitzortungDataPath()
        result = path.build_path("subpath", host_name="custom")
        assert_that(result).contains("custom.blitzortung.org")

    def test_build_path_with_custom_region(self):
        """Test build_path with custom region."""
        path = blitzortung.dataimport.base.BlitzortungDataPath()
        result = path.build_path("subpath/{region}", region=5)
        assert_that(result).contains("5")

    def test_build_path_formats_template(self):
        """Test build_path correctly formats template strings."""
        custom_base = "https://example.com"
        path = blitzortung.dataimport.base.BlitzortungDataPath(custom_base)
        result = path.build_path("year/{region}/data")
        assert_that(result).contains("example.com")
        assert_that(result).contains("1")  # default region


class TestBlitzortungDataPathGenerator:

    def test_time_granularity(self):
        generator = blitzortung.dataimport.base.BlitzortungDataPathGenerator()
        assert_that(generator.time_granularity.total_seconds()).is_equal_to(600)

    def test_url_path_format(self):
        generator = blitzortung.dataimport.base.BlitzortungDataPathGenerator()
        assert_that(generator.url_path_format).is_equal_to("%Y/%m/%d/%H/%M.log")

    def test_get_paths_single_interval(self):
        import datetime
        generator = blitzortung.dataimport.base.BlitzortungDataPathGenerator()
        start = datetime.datetime(2025, 1, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2025, 1, 15, 12, 10, 0, tzinfo=datetime.timezone.utc)

        paths = list(generator.get_paths(start, end))

        assert_that(paths).is_equal_to(["2025/01/15/12/00.log", "2025/01/15/12/10.log"])

    def test_get_paths_multiple_intervals(self):
        import datetime
        generator = blitzortung.dataimport.base.BlitzortungDataPathGenerator()
        start = datetime.datetime(2025, 1, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2025, 1, 15, 12, 30, 0, tzinfo=datetime.timezone.utc)

        paths = list(generator.get_paths(start, end))

        assert_that(paths).is_equal_to([
            "2025/01/15/12/00.log",
            "2025/01/15/12/10.log",
            "2025/01/15/12/20.log",
            "2025/01/15/12/30.log"
        ])

    def test_get_paths_without_end_time(self):
        import datetime
        generator = blitzortung.dataimport.base.BlitzortungDataPathGenerator()
        start = datetime.datetime.now(datetime.timezone.utc)

        paths = list(generator.get_paths(start))

        assert_that(paths).is_not_empty()
        assert_that(paths[0]).matches(r'\d{4}/\d{2}/\d{2}/\d{2}/\d{2}\.log')

    def test_get_paths_rounds_to_intervals(self):
        import datetime
        generator = blitzortung.dataimport.base.BlitzortungDataPathGenerator()
        start = datetime.datetime(2025, 11, 14, 13, 45, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2025, 11, 14, 13, 55, 0, tzinfo=datetime.timezone.utc)

        paths = list(generator.get_paths(start, end))

        assert_that(paths).is_equal_to(["2025/11/14/13/40.log", "2025/11/14/13/50.log"])

    def test_get_paths_returns_generator(self):
        import datetime
        generator = blitzortung.dataimport.base.BlitzortungDataPathGenerator()
        start = datetime.datetime(2025, 1, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)

        result = generator.get_paths(start)

        assert_that(hasattr(result, "__iter__")).is_true()
        assert_that(hasattr(result, "__next__")).is_true()
