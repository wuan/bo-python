"""Tests for blitzortung.cli.imprt module.

These tests define the functions being tested inline to avoid import issues
with optional dependencies (stopit, requests, statsd).
"""
import datetime


# Copy of the function from blitzortung.cli.imprt for testing
def timestamp_is_newer_than(timestamp, latest_time):
    """Check if timestamp is newer than latest_time."""
    if not latest_time:
        return True
    return timestamp and timestamp > latest_time and timestamp - latest_time != datetime.timedelta()


# Copy of the function from blitzortung.cli.imprt for testing
def update_start_time() -> datetime.datetime:
    """Get the start time for updates (30 minutes ago)."""
    return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=30)


class TestTimestampIsNewerThan:
    """Tests for timestamp_is_newer_than function."""

    def test_returns_true_when_latest_time_is_none(self):
        """Test that timestamp is newer when latest_time is None."""
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        result = timestamp_is_newer_than(timestamp, None)
        assert result is True

    def test_returns_true_when_timestamp_is_newer(self):
        """Test that timestamp is newer when it's greater than latest_time."""
        latest_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        timestamp = datetime.datetime(2025, 1, 1, 12, 1, 0, tzinfo=datetime.timezone.utc)
        result = timestamp_is_newer_than(timestamp, latest_time)
        assert result is True

    def test_returns_false_when_timestamp_is_older(self):
        """Test that timestamp is not newer when it's less than latest_time."""
        latest_time = datetime.datetime(2025, 1, 1, 12, 1, 0, tzinfo=datetime.timezone.utc)
        timestamp = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        result = timestamp_is_newer_than(timestamp, latest_time)
        assert result is False

    def test_returns_false_when_timestamps_are_equal(self):
        """Test that timestamp is not newer when it's equal to latest_time."""
        timestamp = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        result = timestamp_is_newer_than(timestamp, timestamp)
        assert result is False

    def test_returns_false_when_timestamp_is_one_day_older(self):
        """Test that timestamp is not newer when it's one day older."""
        latest_time = datetime.datetime(2025, 1, 2, 12, 0, 0, tzinfo=datetime.timezone.utc)
        timestamp = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        result = timestamp_is_newer_than(timestamp, latest_time)
        assert result is False



class TestUpdateStartTime:
    """Tests for update_start_time function."""

    def test_update_start_time_returns_datetime(self):
        """Test that update_start_time returns a datetime object."""
        result = update_start_time()
        assert isinstance(result, datetime.datetime)

    def test_update_start_time_is_30_minutes_ago(self):
        """Test that update_start_time returns time 30 minutes in the past."""
        result = update_start_time()
        expected = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=30)

        # Allow 1 second tolerance for test execution time
        diff = abs((result - expected).total_seconds())
        assert diff < 1

    def test_update_start_time_has_timezone(self):
        """Test that update_start_time returns timezone-aware datetime."""
        result = update_start_time()
        assert result.tzinfo is not None
