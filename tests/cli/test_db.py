"""Tests for blitzortung.cli.db module."""

from io import StringIO
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


class TestFetchStrikes:
    """Tests for fetch_strikes function."""

    @pytest.fixture
    def mock_strike_db(self):
        """Create a mock strike database."""
        mock_db = Mock()
        mock_db.select = Mock(return_value=[])
        return mock_db

    @pytest.fixture
    def mock_options(self):
        """Create mock options."""
        options = Mock()
        options.precision = 4
        return options

    def test_fetch_strikes_empty_result(self, mock_strike_db, mock_options):
        """Test fetch_strikes with no strikes."""
        mock_strike_db.select.return_value = []

        with patch('sys.stdout', new_callable=StringIO) as mock_stdout, \
             patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            db.fetch_strikes(None, mock_options, 'timestamp', mock_strike_db, Mock())

        mock_strike_db.select.assert_called_once()

    def test_fetch_strikes_with_results(self, mock_strike_db, mock_options):
        """Test fetch_strikes with strike results."""
        mock_strike = Mock()
        mock_strike.x = 10.123456
        mock_strike.y = 20.654321
        mock_strike.__str__ = Mock(return_value="strike_data")
        mock_strike_db.select.return_value = [mock_strike]

        with patch('sys.stdout', new_callable=StringIO) as mock_stdout, \
             patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            db.fetch_strikes(None, mock_options, 'timestamp', mock_strike_db, Mock())

        # Verify strike coordinates were rounded
        assert mock_strike.x == 10.1235
        assert mock_strike.y == 20.6543
        mock_strike_db.select.assert_called_once()

    def test_fetch_strikes_precision(self, mock_strike_db):
        """Test that precision option is applied correctly."""
        mock_strike = Mock()
        mock_strike.x = 10.123456789
        mock_strike.y = 20.987654321
        mock_strike.__str__ = Mock(return_value="strike")
        mock_strike_db.select.return_value = [mock_strike]

        options = Mock()
        options.precision = 2

        with patch('sys.stdout', new_callable=StringIO), \
             patch('sys.stderr', new_callable=StringIO):
            db.fetch_strikes(None, options, 'timestamp', mock_strike_db, Mock())

        # With precision 2, should round to 2 decimal places
        assert mock_strike.x == 10.12
        assert mock_strike.y == 20.99


class TestFetchStrikesGrid:
    """Tests for fetch_strikes_grid function."""

    @pytest.fixture
    def mock_strike_db(self):
        """Create a mock strike database."""
        mock_db = Mock()
        return mock_db

    @pytest.fixture
    def mock_grid(self):
        """Create a mock grid."""
        grid = Mock()
        return grid

    def test_fetch_strikes_grid_default(self, mock_strike_db, mock_grid):
        """Test fetch_strikes_grid outputs arcgrid by default."""
        mock_result = Mock()
        mock_result.to_arcgrid = Mock(return_value="grid_data")
        mock_strike_db.select_grid.return_value = mock_result

        options = Mock()
        options.map = False

        with patch('sys.stdout', new_callable=StringIO) as mock_stdout, \
             patch('sys.stderr', new_callable=StringIO):
            db.fetch_strikes_grid(mock_grid, options, mock_strike_db, Mock())

        mock_result.to_arcgrid.assert_called_once()

    def test_fetch_strikes_grid_with_map(self, mock_strike_db, mock_grid):
        """Test fetch_strikes_grid outputs map when requested."""
        mock_result = Mock()
        mock_result.to_map = Mock(return_value="map_data")
        mock_strike_db.select_grid.return_value = mock_result

        options = Mock()
        options.map = True

        with patch('sys.stdout', new_callable=StringIO) as mock_stdout, \
             patch('sys.stderr', new_callable=StringIO):
            db.fetch_strikes_grid(mock_grid, options, mock_strike_db, Mock())

        mock_result.to_map.assert_called_once()


class TestMain:
    """Tests for main function."""

    def test_main_with_invalid_timezone(self):
        """Test that main exits with error for invalid timezone."""
        with patch('sys.argv', ['db.py', '--tz', 'InvalidTimezone']):
            with pytest.raises(SystemExit) as exc_info:
                db.main()

            assert exc_info.value.code == 1
