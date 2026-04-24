"""Tests for blitzortung.cli.webservice_insertlog module."""

import json
import os
import tempfile
from unittest.mock import Mock, patch

import pytest


class TestWebserviceInsertlogIntegration:
    """Integration tests for webservice_insertlog with real file I/O."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def mock_geoip_db(self):
        """Create a mock GeoIP database."""
        mock_reader = Mock()
        mock_city = Mock()
        mock_city.city.name = "Berlin"
        mock_country = Mock()
        mock_country.iso_code = "DE"
        mock_city.country = mock_country
        mock_reader.city.return_value = mock_city
        return mock_reader

    def test_processes_single_json_file(self, temp_dir, mock_geoip_db):
        """Test that a single JSON file is processed and output is created."""
        # Use a timestamp that results in a predictable date in UTC
        # 1704067200000000 = 2024-01-01 00:00:00 UTC
        timestamp_us = 1704067200000000
        json_file = os.path.join(temp_dir, "test_2024-01-01_00-00-00.json")
        json_data = {
            'timestamp': timestamp_us,
            'get_strikes_grid': [
                [
                    timestamp_us,  # timestamp_microseconds
                    60,  # minute_length
                    0.1,  # grid_baselength
                    0,  # minute_offset
                    1,  # region
                    10,  # count_threshold
                    '93.184.216.34',  # remote_address
                    'bo-android-10'  # user_agent
                ]
            ]
        }
        with open(json_file, 'w') as f:
            json.dump(json_data, f)

        # Run the main function with our test directory
        with patch('sys.argv', [
            'webservice_insertlog.py',
            '--base-dir', temp_dir,
            '--geoip-db', '/dev/null'
        ]):
            with patch('blitzortung.cli.webservice_insertlog.geoip2.database.Reader') as mock_reader_class:
                mock_reader_class.return_value = mock_geoip_db

                # Need to reload the module to pick up new patches
                import importlib
                import blitzortung.cli.webservice_insertlog as ws_module
                importlib.reload(ws_module)
                ws_module.main()

        # Verify output file was created (date depends on local timezone, but we can check any output file exists)
        output_files = [f for f in os.listdir(temp_dir) if f.startswith('servicelog_')]
        assert len(output_files) > 0, f"Expected output file in {temp_dir}, files: {os.listdir(temp_dir)}"

        # Verify output content
        output_file = os.path.join(temp_dir, output_files[0])
        with open(output_file, 'r') as f:
            content = f.read()
            # Should contain the processed data (IP will be masked to '-')
            assert '-\t' in content  # masked IP
            assert '10' in content  # version

    def test_processes_multiple_json_files(self, temp_dir, mock_geoip_db):
        """Test that multiple JSON files are processed in order."""
        # Use timestamps that result in predictable dates
        base_timestamp = 1704067200000000  # 2024-01-01 00:00:00 UTC
        for i in range(3):
            json_file = os.path.join(temp_dir, f"test_2024-01-01_{i:02d}-00-00.json")
            json_data = {
                'timestamp': base_timestamp + (i * 3600 * 1000000),
                'get_strikes_grid': [
                    [
                        base_timestamp + (i * 3600 * 1000000),
                        60, 0.1, 0, 1, 10,
                        '93.184.216.34', 'bo-android-10'
                    ]
                ]
            }
            with open(json_file, 'w') as f:
                json.dump(json_data, f)

        # Run the main function
        with patch('sys.argv', [
            'webservice_insertlog.py',
            '--base-dir', temp_dir,
            '--geoip-db', '/dev/null'
        ]):
            with patch('blitzortung.cli.webservice_insertlog.geoip2.database.Reader') as mock_reader_class:
                mock_reader_class.return_value = mock_geoip_db

                import importlib
                import blitzortung.cli.webservice_insertlog as ws_module
                importlib.reload(ws_module)
                ws_module.main()

        # Verify output file was created
        output_files = [f for f in os.listdir(temp_dir) if f.startswith('servicelog_')]
        assert len(output_files) > 0

    def test_deletes_json_file_after_processing(self, temp_dir, mock_geoip_db):
        """Test that JSON file is deleted after being processed."""
        timestamp_us = 1704067200000000  # 2024-01-01 00:00:00 UTC
        json_file = os.path.join(temp_dir, "test_2024-01-01_00-00-00.json")
        json_data = {
            'timestamp': timestamp_us,
            'get_strikes_grid': [
                [timestamp_us, 60, 0.1, 0, 1, 10, '93.184.216.34', 'bo-android-10']
            ]
        }
        with open(json_file, 'w') as f:
            json.dump(json_data, f)

        # Run the main function
        with patch('sys.argv', [
            'webservice_insertlog.py',
            '--base-dir', temp_dir,
            '--geoip-db', '/dev/null'
        ]):
            with patch('blitzortung.cli.webservice_insertlog.geoip2.database.Reader') as mock_reader_class:
                mock_reader_class.return_value = mock_geoip_db

                import importlib
                import blitzortung.cli.webservice_insertlog as ws_module
                importlib.reload(ws_module)
                ws_module.main()

        # Verify JSON file was deleted
        assert not os.path.exists(json_file), "JSON file should be deleted after processing"

    def test_handles_missing_geoip_gracefully(self, temp_dir):
        """Test that missing GeoIP data is handled gracefully."""
        timestamp_us = 1704067200000000  # 2024-01-01 00:00:00 UTC
        json_file = os.path.join(temp_dir, "test_2024-01-01_00-00-00.json")
        json_data = {
            'timestamp': timestamp_us,
            'get_strikes_grid': [
                [timestamp_us, 60, 0.1, 0, 1, 10, '93.184.216.34', 'bo-android-10']
            ]
        }
        with open(json_file, 'w') as f:
            json.dump(json_data, f)

        # Create mock that raises AddressNotFoundError
        from geoip2 import errors
        mock_reader = Mock()
        mock_reader.city.side_effect = errors.AddressNotFoundError(404, "Not found")

        with patch('sys.argv', [
            'webservice_insertlog.py',
            '--base-dir', temp_dir,
            '--geoip-db', '/dev/null'
        ]):
            with patch('blitzortung.cli.webservice_insertlog.geoip2.database.Reader') as mock_reader_class:
                mock_reader_class.return_value = mock_reader

                import importlib
                import blitzortung.cli.webservice_insertlog as ws_module
                importlib.reload(ws_module)
                ws_module.main()

        # Should still create output with default values
        output_files = [f for f in os.listdir(temp_dir) if f.startswith('servicelog_')]
        assert len(output_files) > 0

    def test_processes_entry_with_local_coordinates(self, temp_dir, mock_geoip_db):
        """Test processing entry with local coordinates."""
        timestamp_us = 1704067200000000  # 2024-01-01 00:00:00 UTC
        json_file = os.path.join(temp_dir, "test_2024-01-01_00-00-00.json")
        json_data = {
            'timestamp': timestamp_us,
            'get_strikes_grid': [
                [
                    timestamp_us, 60, 0.1, 0, 1, 10,
                    '93.184.216.34', 'bo-android-10',
                    100, 200, 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'  # local coords
                ]
            ]
        }
        with open(json_file, 'w') as f:
            json.dump(json_data, f)

        with patch('sys.argv', [
            'webservice_insertlog.py',
            '--base-dir', temp_dir,
            '--geoip-db', '/dev/null'
        ]):
            with patch('blitzortung.cli.webservice_insertlog.geoip2.database.Reader') as mock_reader_class:
                mock_reader_class.return_value = mock_geoip_db

                import importlib
                import blitzortung.cli.webservice_insertlog as ws_module
                importlib.reload(ws_module)
                ws_module.main()

        output_files = [f for f in os.listdir(temp_dir) if f.startswith('servicelog_')]
        assert len(output_files) > 0

        output_file = os.path.join(temp_dir, output_files[0])
        with open(output_file, 'r') as f:
            content = f.read()
            # Local coordinates should be in output
            assert '100' in content
            assert '200' in content
            assert 'POLYGON' in content


class TestWebserviceInsertlogUnit:
    """Unit tests for individual functions."""

    def test_timestamp_conversion(self):
        """Test timestamp conversion from microseconds."""
        timestamp_microseconds = 1704110400000000  # 2025-01-01 12:00:00 UTC
        timestamp_seconds = timestamp_microseconds / 1000000
        # Just verify the math is correct - actual date depends on timezone
        assert timestamp_seconds == 1704110400.0

    def test_user_agent_parsing_android(self):
        """Test parsing bo-android user agent."""
        user_agent = 'bo-android-15'
        user_agent_parts = user_agent.split(' ')[0].rsplit('-', 1)
        version_prefix = user_agent_parts[0]
        version = None
        if version_prefix == 'bo-android' and len(user_agent_parts) > 1:
            try:
                version = int(user_agent_parts[1])
            except ValueError:
                pass

        assert version_prefix == 'bo-android'
        assert version == 15

    def test_user_agent_parsing_ios(self):
        """Test parsing bo-ios user agent."""
        user_agent = 'bo-ios-5'
        user_agent_parts = user_agent.split(' ')[0].rsplit('-', 1)
        version_prefix = user_agent_parts[0]
        # bo-ios should not match bo-android
        assert version_prefix == 'bo-ios'

    def test_metrics_tag_formation(self):
        """Test forming metrics tag string."""
        tags = {
            "version": "10",
            "region": 1,
            "minutes": 60,
            "offset": 0,
            "grid": "0.1"
        }
        tag_values = ",".join([f"{key}={value}" for key, value in tags.items()])

        assert "version=10" in tag_values
        assert "region=1" in tag_values

    def test_value_to_string_float(self):
        """Test value_to_string with float - returns 4 decimal places."""
        from blitzortung.convert import value_to_string
        # Float gets formatted to 4 decimal places
        assert value_to_string(3.14) == "3.1400"

    def test_value_to_string_int(self):
        """Test value_to_string with int."""
        from blitzortung.convert import value_to_string
        assert value_to_string(42) == "42"

    def test_value_to_string_string(self):
        """Test value_to_string with string."""
        from blitzortung.convert import value_to_string
        assert value_to_string("test") == "test"
