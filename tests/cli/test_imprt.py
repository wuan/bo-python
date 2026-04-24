"""Tests for blitzortung.cli.imprt module."""

import datetime
import sys
from unittest.mock import MagicMock, Mock, patch

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


class TestImportStrikesFor:
    """Tests for import_strikes_for function."""

    @pytest.fixture(autouse=True)
    def mock_dependencies(self, monkeypatch):
        """Mock dependencies for import_strikes_for tests."""
        mock_stopit = MagicMock()
        mock_requests = MagicMock()
        mock_statsd = MagicMock()
        mock_timer = MagicMock()
        mock_timer.lap.return_value = 0.001

        monkeypatch.setitem(sys.modules, 'stopit', mock_stopit)
        monkeypatch.setitem(sys.modules, 'requests', mock_requests)
        monkeypatch.setitem(sys.modules, 'statsd', mock_statsd)
        monkeypatch.setitem(sys.modules, 'blitzortung.util', MagicMock())
        monkeypatch.setitem(sys.modules, 'blitzortung.util.Timer', mock_timer)

    def test_import_strikes_for_with_no_strikes(self):
        """Test import_strikes_for when no strikes are returned."""
        import datetime

        mock_strike_db = Mock()
        mock_strike_db.get_latest_time.return_value = None
        mock_strike_db.insert = Mock()
        mock_strike_db.commit = Mock()

        mock_strike_source = Mock()
        mock_strike_source.get_strikes_since.return_value = []

        mock_blitzortung = MagicMock()
        mock_blitzortung.db.strike.return_value = mock_strike_db
        mock_blitzortung.dataimport.strikes.return_value = mock_strike_source

        with patch('blitzortung.cli.imprt.blitzortung', mock_blitzortung), \
             patch('blitzortung.cli.imprt.util.Timer') as mock_timer_class:
            mock_timer_class.return_value = Mock(lap=Mock(return_value=0.001))

            from blitzortung.cli import imprt
            import_strikes_for = imprt.import_strikes_for

            start_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
            import_strikes_for(1, start_time, is_update=False)

        # Verify database methods were called
        mock_strike_db.get_latest_time.assert_called_once()
        mock_strike_source.get_strikes_since.assert_called_once()

    def test_import_strikes_for_inserts_strikes(self):
        """Test import_strikes_for inserts strikes into database."""
        import datetime

        mock_strike = Mock()
        mock_strike.timestamp = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

        mock_strike_db = Mock()
        mock_strike_db.get_latest_time.return_value = None
        mock_strike_db.insert = Mock()
        mock_strike_db.commit = Mock()

        mock_strike_source = Mock()
        mock_strike_source.get_strikes_since.return_value = [mock_strike]

        mock_blitzortung = MagicMock()
        mock_blitzortung.db.strike.return_value = mock_strike_db
        mock_blitzortung.dataimport.strikes.return_value = mock_strike_source

        with patch('blitzortung.cli.imprt.blitzortung', mock_blitzortung), \
             patch('blitzortung.cli.imprt.util.Timer') as mock_timer_class:
            mock_timer_class.return_value = Mock(lap=Mock(return_value=0.001))

            from blitzortung.cli import imprt
            import_strikes_for = imprt.import_strikes_for

            start_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
            import_strikes_for(1, start_time, is_update=False)

        # Verify insert was called
        mock_strike_db.insert.assert_called_once_with(mock_strike, 1)
        mock_strike_db.commit.assert_called()


class TestImportStrikes:
    """Tests for import_strikes function."""

    @pytest.fixture(autouse=True)
    def mock_dependencies(self, monkeypatch):
        """Mock dependencies for import_strikes tests."""
        mock_stopit = MagicMock()
        mock_requests = MagicMock()
        mock_statsd = MagicMock()

        monkeypatch.setitem(sys.modules, 'stopit', mock_stopit)
        monkeypatch.setitem(sys.modules, 'requests', mock_requests)
        monkeypatch.setitem(sys.modules, 'statsd', mock_statsd)

    def test_import_strikes_single_region(self):
        """Test import_strikes with a single region."""
        import datetime

        with patch('blitzortung.cli.imprt.import_strikes_for') as mock_import:
            from blitzortung.cli import imprt

            start_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
            imprt.import_strikes([1], start_time, no_timeout=True)

        mock_import.assert_called_once()
