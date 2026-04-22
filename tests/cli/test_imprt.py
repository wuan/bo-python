"""Tests for blitzortung.cli.imprt module."""

import datetime
import sys
from unittest.mock import MagicMock, patch

import pytest


class TestTimestampIsNewerThan:
    """Tests for timestamp_is_newer_than function."""

    @pytest.fixture(autouse=True)
    def mock_dependencies(self, monkeypatch):
        """Mock optional dependencies to allow importing the module."""
        mock_stopit = MagicMock()
        mock_requests = MagicMock()
        mock_statsd = MagicMock()
        monkeypatch.setitem(sys.modules, 'stopit', mock_stopit)
        monkeypatch.setitem(sys.modules, 'requests', mock_requests)
        monkeypatch.setitem(sys.modules, 'statsd', mock_statsd)

    def test_returns_true_when_latest_time_is_none(self):
        """Test that timestamp is newer when latest_time is None."""
        from blitzortung.cli import imprt
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        result = imprt.timestamp_is_newer_than(timestamp, None)
        assert result is True

    def test_returns_true_when_timestamp_is_newer(self):
        """Test that timestamp is newer when it's greater than latest_time."""
        from blitzortung.cli import imprt
        latest_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        timestamp = datetime.datetime(2025, 1, 1, 12, 1, 0, tzinfo=datetime.timezone.utc)
        result = imprt.timestamp_is_newer_than(timestamp, latest_time)
        assert result is True

    def test_returns_false_when_timestamp_is_older(self):
        """Test that timestamp is not newer when it's less than latest_time."""
        from blitzortung.cli import imprt
        latest_time = datetime.datetime(2025, 1, 1, 12, 1, 0, tzinfo=datetime.timezone.utc)
        timestamp = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        result = imprt.timestamp_is_newer_than(timestamp, latest_time)
        assert result is False

    def test_returns_false_when_timestamps_are_equal(self):
        """Test that timestamp is not newer when it's equal to latest_time."""
        from blitzortung.cli import imprt
        timestamp = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        result = imprt.timestamp_is_newer_than(timestamp, timestamp)
        assert result is False

    def test_returns_false_when_timestamp_is_one_day_older(self):
        """Test that timestamp is not newer when it's one day older."""
        from blitzortung.cli import imprt
        latest_time = datetime.datetime(2025, 1, 2, 12, 0, 0, tzinfo=datetime.timezone.utc)
        timestamp = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        result = imprt.timestamp_is_newer_than(timestamp, latest_time)
        assert result is False


class TestUpdateStartTime:
    """Tests for update_start_time function."""

    @pytest.fixture(autouse=True)
    def mock_dependencies(self, monkeypatch):
        """Mock optional dependencies to allow importing the module."""
        mock_stopit = MagicMock()
        mock_requests = MagicMock()
        mock_statsd = MagicMock()
        monkeypatch.setitem(sys.modules, 'stopit', mock_stopit)
        monkeypatch.setitem(sys.modules, 'requests', mock_requests)
        monkeypatch.setitem(sys.modules, 'statsd', mock_statsd)

    def test_update_start_time_returns_datetime(self):
        """Test that update_start_time returns a datetime object."""
        from blitzortung.cli import imprt
        result = imprt.update_start_time()
        assert isinstance(result, datetime.datetime)

    def test_update_start_time_is_30_minutes_ago(self):
        """Test that update_start_time returns time 30 minutes in the past."""
        from blitzortung.cli import imprt
        result = imprt.update_start_time()
        expected = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=30)

        # Allow 1 second tolerance for test execution time
        diff = abs((result - expected).total_seconds())
        assert diff < 1

    def test_update_start_time_has_timezone(self):
        """Test that update_start_time returns timezone-aware datetime."""
        from blitzortung.cli import imprt
        result = imprt.update_start_time()
        assert result.tzinfo is not None
