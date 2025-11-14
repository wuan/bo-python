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
import time
from unittest.mock import Mock

from assertpy import assert_that  # pylint: disable=import-error
import pytest  # pylint: disable=import-error

import blitzortung.service.general
from blitzortung.db import query


class TestCreateTimeInterval:
    """Test suite for create_time_interval function."""

    def test_creates_time_interval_object(self):
        """Test that a TimeInterval object is created."""
        interval = blitzortung.service.general.create_time_interval(60, 0)
        assert_that(interval).is_instance_of(query.TimeInterval)

    def test_interval_duration_matches_minute_length(self):
        """Test that interval duration matches the specified minute length."""
        minute_length = 120
        interval = blitzortung.service.general.create_time_interval(minute_length, 0)
        duration = interval.end - interval.start
        expected_duration = datetime.timedelta(minutes=minute_length)
        assert_that(duration).is_equal_to(expected_duration)

    def test_interval_with_zero_offset(self):
        """Test that zero offset results in end time close to current time."""
        before = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        interval = blitzortung.service.general.create_time_interval(60, 0)
        after = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        # End time should be between before and after with small tolerance
        assert_that(interval.end).is_greater_than_or_equal_to(before)
        assert_that(interval.end).is_less_than_or_equal_to(after)

    def test_interval_with_positive_offset(self):
        """Test that positive offset shifts end time forward."""
        minute_offset = 30
        before = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        interval = blitzortung.service.general.create_time_interval(60, minute_offset)
        expected_end_min = before + datetime.timedelta(minutes=minute_offset)
        assert_that(interval.end).is_greater_than_or_equal_to(expected_end_min)

    def test_interval_with_negative_offset(self):
        """Test that negative offset shifts end time backward."""
        minute_offset = -30
        before = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        interval = blitzortung.service.general.create_time_interval(60, minute_offset)
        expected_end_max = before + datetime.timedelta(minutes=minute_offset)
        assert_that(interval.end).is_less_than_or_equal_to(expected_end_max)

    def test_interval_has_utc_timezone(self):
        """Test that interval times have UTC timezone."""
        interval = blitzortung.service.general.create_time_interval(60, 0)
        assert_that(interval.start.tzinfo).is_equal_to(datetime.timezone.utc)
        assert_that(interval.end.tzinfo).is_equal_to(datetime.timezone.utc)


class TestTimingState:
    """Test suite for TimingState class."""

    @pytest.fixture
    def mock_statsd(self):
        """Create a mock statsd client."""
        return Mock()

    @pytest.fixture
    def timing_state(self, mock_statsd):
        """Create a TimingState instance with mock statsd."""
        return blitzortung.service.general.TimingState("test_timer", mock_statsd)

    def test_initialization_sets_name(self, timing_state):
        """Test that initialization sets the name correctly."""
        assert_that(timing_state.name).is_equal_to("test_timer")

    def test_initialization_sets_statsd_client(self, timing_state, mock_statsd):
        """Test that initialization sets the statsd client."""
        assert_that(timing_state.statsd_client).is_equal_to(mock_statsd)

    def test_initialization_sets_empty_info_text(self, timing_state):
        """Test that initialization sets empty info text list."""
        assert_that(timing_state.info_text).is_equal_to([])

    def test_initialization_sets_reference_time(self):
        """Test that initialization sets a reference time."""
        before = time.time()
        ts = blitzortung.service.general.TimingState("test", Mock())
        after = time.time()
        assert_that(ts.reference_time).is_greater_than_or_equal_to(before)
        assert_that(ts.reference_time).is_less_than_or_equal_to(after)

    def test_get_seconds_returns_elapsed_time(self, timing_state):
        """Test that get_seconds returns approximate elapsed time."""
        time.sleep(0.1)
        elapsed = timing_state.get_seconds()
        assert_that(elapsed).is_greater_than(0.09)
        assert_that(elapsed).is_less_than(0.2)

    def test_get_seconds_with_custom_reference_time(self, timing_state):
        """Test get_seconds with custom reference time."""
        custom_ref = time.time() - 1.5
        elapsed = timing_state.get_seconds(custom_ref)
        assert_that(elapsed).is_greater_than(1.4)
        assert_that(elapsed).is_less_than(1.6)

    def test_get_milliseconds_returns_elapsed_milliseconds(self, timing_state):
        """Test that get_milliseconds returns elapsed time in milliseconds."""
        time.sleep(0.1)
        elapsed_ms = timing_state.get_milliseconds()
        assert_that(elapsed_ms).is_greater_than(90)
        assert_that(elapsed_ms).is_less_than(200)

    def test_get_milliseconds_minimum_is_one(self, timing_state):
        """Test that get_milliseconds returns at least 1 millisecond."""
        elapsed_ms = timing_state.get_milliseconds()
        assert_that(elapsed_ms).is_greater_than_or_equal_to(1)

    def test_reset_timer_updates_reference_time(self, timing_state):
        """Test that reset_timer updates the reference time."""
        initial_ref = timing_state.reference_time
        time.sleep(0.05)
        timing_state.reset_timer()
        new_ref = timing_state.reference_time
        assert_that(new_ref).is_greater_than(initial_ref)

    def test_log_timing_calls_statsd_timing(self, timing_state, mock_statsd):
        """Test that log_timing calls statsd timing method."""
        time.sleep(0.05)
        timing_state.log_timing("test.timer")
        mock_statsd.timing.assert_called_once()
        call_args = mock_statsd.timing.call_args
        assert_that(call_args[0][0]).is_equal_to("test.timer")
        assert_that(call_args[0][1]).is_greater_than_or_equal_to(1)

    def test_log_timing_with_custom_reference_time(self, timing_state, mock_statsd):
        """Test log_timing with custom reference time."""
        custom_ref = time.time() - 2.0
        timing_state.log_timing("test.timer", custom_ref)
        mock_statsd.timing.assert_called_once()
        call_args = mock_statsd.timing.call_args
        assert_that(call_args[0][1]).is_greater_than(
            1900
        )  # At least 1.9 seconds = 1900ms

    def test_log_gauge_calls_statsd_gauge(self, timing_state, mock_statsd):
        """Test that log_gauge calls statsd gauge method."""
        timing_state.log_gauge("test.gauge", 42)
        mock_statsd.gauge.assert_called_once_with("test.gauge", 42)

    def test_log_incr_calls_statsd_incr(self, timing_state, mock_statsd):
        """Test that log_incr calls statsd incr method."""
        timing_state.log_incr("test.counter")
        mock_statsd.incr.assert_called_once_with("test.counter")

    def test_add_info_text_appends_to_list(self, timing_state):
        """Test that add_info_text appends to info text list."""
        timing_state.add_info_text("First message")
        assert_that(timing_state.info_text).is_equal_to(["First message"])

    def test_add_info_text_multiple_calls(self, timing_state):
        """Test that multiple add_info_text calls append sequentially."""
        timing_state.add_info_text("First message")
        timing_state.add_info_text("Second message")
        timing_state.add_info_text("Third message")
        assert_that(timing_state.info_text).is_equal_to(
            ["First message", "Second message", "Third message"]
        )

    def test_add_info_text_preserves_existing_items(self, timing_state):
        """Test that add_info_text preserves existing items."""
        timing_state.info_text = ["Existing"]
        timing_state.add_info_text("New")
        assert_that(timing_state.info_text).is_equal_to(["Existing", "New"])
