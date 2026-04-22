"""Tests for blitzortung.cli.db module."""

from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest

from blitzortung.cli import db


class TestParseTime:
    """Tests for parse_time function."""

    def test_parse_time_basic(self):
        """Test basic time parsing."""
        tz = ZoneInfo("UTC")
        result = db.parse_time("20250101", "1200", tz, "starttime")
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 1
        assert result.hour == 12
        assert result.minute == 0

    def test_parse_time_with_seconds(self):
        """Test time parsing with seconds."""
        tz = ZoneInfo("UTC")
        result = db.parse_time("20250101", "123045", tz, "starttime")
        assert result.second == 45

    def test_parse_time_end_time_adds_minute(self):
        """Test that end time adds a minute."""
        tz = ZoneInfo("UTC")
        result = db.parse_time("20250101", "1200", tz, "endtime", is_end_time=True)
        assert result.hour == 12
        assert result.minute == 1

    def test_parse_time_end_time_with_seconds_adds_second(self):
        """Test that end time with seconds adds a second."""
        tz = ZoneInfo("UTC")
        result = db.parse_time("20250101", "120030", tz, "endtime", is_end_time=True)
        assert result.second == 31


class TestPrepareGridIfApplicable:
    """Tests for prepare_grid_if_applicable function."""

    def test_prepare_grid_returns_none_when_no_grid_options(self):
        """Test that None is returned when no grid options specified."""
        options = Mock()
        options.grid = None
        options.xgrid = None
        options.ygrid = None

        area = Mock()
        area.envelope.bounds = (1.0, 2.0, 3.0, 4.0)

        result = db.prepare_grid_if_applicable(options, area)
        assert result is None

    def test_prepare_grid_uses_single_grid_option(self):
        """Test that single grid option sets both x and y."""
        options = Mock()
        options.grid = 0.5
        options.xgrid = None
        options.ygrid = None
        options.srid = 4326
        options.area = Mock()

        area = Mock()
        area.envelope.bounds = (1.0, 3.0, 2.0, 4.0)

        result = db.prepare_grid_if_applicable(options, area)
        assert result is not None

    def test_prepare_grid_uses_xgrid_option(self):
        """Test that xgrid option is used."""
        options = Mock()
        options.grid = None
        options.xgrid = 0.3
        options.ygrid = None
        options.srid = 4326
        options.area = Mock()

        area = Mock()
        area.envelope.bounds = (1.0, 3.0, 2.0, 4.0)

        result = db.prepare_grid_if_applicable(options, area)
        assert result is not None

    def test_prepare_grid_uses_ygrid_option(self):
        """Test that ygrid option is used."""
        options = Mock()
        options.grid = None
        options.xgrid = None
        options.ygrid = 0.4
        options.srid = 4326
        options.area = Mock()

        area = Mock()
        area.envelope.bounds = (1.0, 3.0, 2.0, 4.0)

        result = db.prepare_grid_if_applicable(options, area)
        assert result is not None

    def test_prepare_grid_requires_area_for_grid_options(self):
        """Test that grid options require area to be defined."""
        options = Mock()
        options.grid = 0.5
        options.xgrid = None
        options.ygrid = None
        options.area = None

        area = Mock()

        with pytest.raises(SystemExit) as exc_info:
            db.prepare_grid_if_applicable(options, area)

        assert exc_info.value.code == 1


class TestParseOptions:
    """Tests for parse_options function."""

    def test_parse_options_defaults(self):
        """Test default option values."""
        with patch('sys.argv', ['db.py']):
            options = db.parse_options()

        assert options.startdate == "default"
        assert options.starttime == "default"
        assert options.enddate == "default"
        assert options.endtime == "default"
        assert options.area is None
        assert options.tz == "UTC"
        assert options.useenv is False
        assert options.srid == 4326
        assert options.precision == 4

    def test_parse_options_with_custom_args(self):
        """Test parsing with custom arguments."""
        with patch('sys.argv', [
            'db.py',
            '--startdate', '20250101',
            '--starttime', '1200',
            '--area', 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))',
            '--tz', 'Europe/Berlin',
            '--precision', '2'
        ]):
            options = db.parse_options()

        assert options.startdate == "20250101"
        assert options.starttime == "1200"
        assert options.area == "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        assert options.tz == "Europe/Berlin"
        assert options.precision == 2

    def test_parse_options_with_grid(self):
        """Test parsing with grid options."""
        with patch('sys.argv', [
            'db.py',
            '--grid', '0.5',
            '--x-grid', '0.3',
            '--y-grid', '0.4'
        ]):
            options = db.parse_options()

        assert options.grid == 0.5
        assert options.xgrid == 0.3
        assert options.ygrid == 0.4

    def test_parse_options_flag_options(self):
        """Test boolean flag options."""
        with patch('sys.argv', [
            'db.py',
            '--useenv',
            '--map'
        ]):
            options = db.parse_options()

        assert options.useenv is True
        assert options.map is True
