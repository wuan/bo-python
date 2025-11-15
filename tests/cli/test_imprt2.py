import datetime
import json
import pytest
from assertpy import assert_that
from mock import Mock, patch, MagicMock

import blitzortung.cli.imprt2 as imprt2
from blitzortung.data import Strike, Timestamp
from blitzortung.db.query import TimeInterval


# Example strike data in JSON format (line-by-line)
example_data = """{"time":1763202124325980200,"lat":-15.296556,"lon":134.589548,"alt":0,"pol":0,"mds":12581,"mcg":162,"status":0,"region":2}
{"time":1763202124297904000,"lat":44.283328,"lon":8.910987,"alt":0,"pol":0,"mds":6830,"mcg":84,"status":2,"region":9}
{"time":1763202124297904000,"lat":44.283328,"lon":8.910987,"alt":0,"pol":0,"mds":6830,"mcg":84,"status":1,"region":8}
{"time":1763202124297897200,"lat":44.288127,"lon":8.927448,"alt":0,"pol":0,"mds":4934,"mcg":136,"status":2,"region":9}
{"time":1763202124297897200,"lat":44.288127,"lon":8.927448,"alt":0,"pol":0,"mds":4934,"mcg":136,"status":1,"region":8}
{"time":1763202124297892000,"lat":44.2774,"lon":8.929396,"alt":0,"pol":0,"mds":8913,"mcg":138,"status":0,"region":1}
{"time":1763202124101646800,"lat":-41.585918,"lon":152.926124,"alt":0,"pol":0,"mds":13391,"mcg":176,"status":0,"region":2}
{"time":1763202123983937500,"lat":44.284989,"lon":8.915263,"alt":0,"pol":0,"mds":10832,"mcg":178,"status":2,"region":9}
{"time":1763202123983937500,"lat":44.284989,"lon":8.915263,"alt":0,"pol":0,"mds":10832,"mcg":178,"status":1,"region":8}
{"time":1763202123983890000,"lat":44.27408,"lon":8.892988,"alt":0,"pol":0,"mds":7469,"mcg":197,"status":2,"region":9}
{"time":1763202123983890000,"lat":44.27408,"lon":8.892988,"alt":0,"pol":0,"mds":7469,"mcg":197,"status":1,"region":8}
{"time":1763202123983889200,"lat":44.279756,"lon":8.924568,"alt":0,"pol":0,"mds":7770,"mcg":82,"status":2,"region":9}
{"time":1763202123983889200,"lat":44.279756,"lon":8.924568,"alt":0,"pol":0,"mds":7770,"mcg":82,"status":1,"region":8}
{"time":1763202123983885800,"lat":44.276457,"lon":8.920456,"alt":0,"pol":0,"mds":5713,"mcg":159,"status":2,"region":9}
{"time":1763202123983885800,"lat":44.276457,"lon":8.920456,"alt":0,"pol":0,"mds":5713,"mcg":159,"status":1,"region":8}
{"time":1763202123702520300,"lat":-38.981925,"lon":151.55461,"alt":0,"pol":0,"mds":14906,"mcg":97,"status":0,"region":2}
{"time":1763202122363942000,"lat":-24.57577,"lon":148.610239,"alt":0,"pol":0,"mds":11794,"mcg":259,"status":0,"region":2}
{"time":1763202122363767300,"lat":-24.252154,"lon":148.741951,"alt":0,"pol":0,"mds":8582,"mcg":124,"status":0,"region":2}
{"time":1763202121942625800,"lat":24.982319,"lon":-59.714592,"alt":0,"pol":0,"mds":11143,"mcg":179,"status":0,"region":5}
{"time":1763202121942529000,"lat":24.770698,"lon":-59.515226,"alt":0,"pol":0,"mds":11771,"mcg":203,"status":1,"region":0}
{"time":1763202121942523000,"lat":24.785201,"lon":-59.502499,"alt":0,"pol":0,"mds":8231,"mcg":211,"status":0,"region":5}
{"time":1763202121735116800,"lat":-41.281446,"lon":152.188824,"alt":0,"pol":0,"mds":14916,"mcg":201,"status":0,"region":2}
{"time":1763202120567548200,"lat":-25.461695,"lon":149.862769,"alt":0,"pol":0,"mds":8506,"mcg":252,"status":0,"region":2}
{"time":1763202120439335400,"lat":-25.508039,"lon":149.845275,"alt":0,"pol":0,"mds":9619,"mcg":261,"status":0,"region":2}
{"time":1763202117207764500,"lat":36.865984,"lon":-9.198683,"alt":0,"pol":0,"mds":9862,"mcg":165,"status":0,"region":1}
{"time":1763202117194445000,"lat":36.918935,"lon":-9.123588,"alt":0,"pol":0,"mds":10229,"mcg":169,"status":0,"region":1}
{"time":1763202117194435300,"lat":36.959022,"lon":-9.187297,"alt":0,"pol":0,"mds":12547,"mcg":169,"status":2,"region":9}
{"time":1763202117194435300,"lat":36.959022,"lon":-9.187297,"alt":0,"pol":0,"mds":12547,"mcg":169,"status":1,"region":8}
{"time":1763202117194433500,"lat":36.845773,"lon":-9.083014,"alt":0,"pol":0,"mds":14208,"mcg":228,"status":0,"region":1}"""




@pytest.fixture
def mock_response():
    """Create a mock HTTP response with example data."""
    response = Mock()
    response.status_code = 200
    response.text = example_data
    response.raise_for_status = Mock()
    return response


@pytest.fixture
def mock_strike_db():
    """Create a mock strike database."""
    db = Mock()
    db.select = Mock(return_value=[])
    db.insert = Mock()
    db.commit = Mock()
    db.rollback = Mock()
    db.close = Mock()
    return db


class TestFetchStrikesFromUrl:
    """Tests for fetching strikes from URL."""

    def test_fetch_strikes_successfully(self, mock_response):
        """Test successful fetch and parse of strike data."""
        with patch('blitzortung.cli.imprt2.requests.get', return_value=mock_response):
            strikes = list(imprt2.fetch_strikes_from_url('http://example.com/strikes'))

        # Should parse all valid lines (28 strikes in example data)
        assert_that(strikes).is_not_empty()
        assert_that(len(strikes)).is_greater_than(0)

        # Verify first strike has expected attributes
        first_strike = strikes[0]
        assert_that(first_strike.x).is_equal_to(134.589548)
        assert_that(first_strike.y).is_equal_to(-15.296556)
        assert_that(first_strike.amplitude).is_equal_to(0)

    def test_fetch_handles_empty_lines(self):
        """Test that empty lines are skipped."""
        response = Mock()
        response.status_code = 200
        response.text = '\n\n{"time":1763202124325980200,"lat":-15.296556,"lon":134.589548,"alt":0,"pol":100}\n\n'
        response.raise_for_status = Mock()

        with patch('blitzortung.cli.imprt2.requests.get', return_value=response):
            strikes = list(imprt2.fetch_strikes_from_url('http://example.com/strikes'))

        assert_that(strikes).is_length(1)
        assert_that(strikes[0].x).is_equal_to(134.589548)
        assert_that(strikes[0].y).is_equal_to(-15.296556)

    def test_fetch_handles_invalid_strike_data(self):
        """Test that invalid strikes are logged and skipped."""
        response = Mock()
        response.status_code = 200
        response.text = 'invalid json line\n{"time":1763202124325980200,"lat":-15.296556,"lon":134.589548,"alt":0,"pol":50}'
        response.raise_for_status = Mock()

        with patch('blitzortung.cli.imprt2.requests.get', return_value=response):
            strikes = list(imprt2.fetch_strikes_from_url('http://example.com/strikes'))

        # Should skip invalid line but parse valid one
        assert_that(strikes).is_length(1)
        assert_that(strikes[0].amplitude).is_equal_to(50)

    def test_fetch_raises_on_http_error(self):
        """Test that HTTP errors are propagated."""
        import requests

        with patch('blitzortung.cli.imprt2.requests.get', side_effect=requests.RequestException("Connection error")):
            with pytest.raises(requests.RequestException):
                list(imprt2.fetch_strikes_from_url('http://example.com/strikes'))

    def test_fetch_with_authentication(self, mock_response):
        """Test fetch with authentication credentials."""
        with patch('blitzortung.cli.imprt2.requests.get', return_value=mock_response) as mock_get:
            list(imprt2.fetch_strikes_from_url('http://example.com/strikes', auth=('user', 'pass')))

            mock_get.assert_called_once()
            call_kwargs = mock_get.call_args[1]
            assert_that(call_kwargs['auth']).is_equal_to(('user', 'pass'))


class TestCreateStrikeKey:
    """Tests for creating unique strike keys."""

    def test_create_strike_key_with_timestamp_value(self):
        """Test strike key creation with Timestamp object."""
        strike = Mock(spec=Strike)
        strike.timestamp = Mock()
        strike.timestamp.value = 1234567890123456789
        strike.x = 12.345678
        strike.y = 45.678901
        strike.amplitude = 100

        key = imprt2.create_strike_key(strike)

        assert_that(key).is_equal_to((1234567890123456789, 12.345678, 45.678901, 100))

    def test_create_strike_key_rounds_location(self):
        """Test that location is rounded to 6 decimal places."""
        strike = Mock(spec=Strike)
        strike.timestamp = Mock()
        strike.timestamp.value = 1000000000000000000
        strike.x = 12.34567890123  # More than 6 decimals
        strike.y = 45.67890123456
        strike.amplitude = 50

        key = imprt2.create_strike_key(strike)

        # Should be rounded to 6 decimals
        assert_that(key[1]).is_equal_to(12.345679)
        assert_that(key[2]).is_equal_to(45.678901)


class TestGetExistingStrikeKeys:
    """Tests for querying existing strikes from database."""

    def test_get_existing_strikes_empty_result(self, mock_strike_db):
        """Test with no existing strikes."""
        mock_strike_db.select.return_value = []

        start = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2025, 1, 1, 13, 0, 0, tzinfo=datetime.timezone.utc)
        time_interval = TimeInterval(start, end)

        result = imprt2.get_existing_strike_keys(mock_strike_db, time_interval)

        assert_that(result).is_empty()
        mock_strike_db.select.assert_called_once()

    def test_get_existing_strikes_with_results(self, mock_strike_db):
        """Test with existing strikes."""
        # Create mock strikes with unique characteristics
        strike1 = Mock(spec=Strike)
        strike1.timestamp = Mock()
        strike1.timestamp.value = 1000000000000000001
        strike1.x = 10.5
        strike1.y = 20.5
        strike1.amplitude = 100

        strike2 = Mock(spec=Strike)
        strike2.timestamp = Mock()
        strike2.timestamp.value = 1000000000000000002
        strike2.x = 11.5
        strike2.y = 21.5
        strike2.amplitude = 200

        mock_strike_db.select.return_value = [strike1, strike2]

        start = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2025, 1, 1, 13, 0, 0, tzinfo=datetime.timezone.utc)
        time_interval = TimeInterval(start, end)

        result = imprt2.get_existing_strike_keys(mock_strike_db, time_interval)

        assert_that(result).is_length(2)
        assert_that(result).contains(
            (1000000000000000001, 10.5, 20.5, 100),
            (1000000000000000002, 11.5, 21.5, 200)
        )



class TestUpdateStrikes:
    """Integration tests for update_strikes function."""

    @patch('blitzortung.cli.imprt2.blitzortung.db.strike')
    @patch('blitzortung.cli.imprt2.fetch_strikes_from_url')
    @patch('blitzortung.cli.imprt2.blitzortung.config.config')
    def test_update_strikes_inserts_new_strikes(self, mock_config, mock_fetch, mock_db_func):
        """Test that new strikes are inserted."""
        # Setup mocks
        mock_strike_db = Mock()
        mock_strike_db.select.return_value = []  # No existing strikes
        mock_strike_db.insert = Mock()
        mock_strike_db.commit = Mock()
        mock_strike_db.close = Mock()
        mock_db_func.return_value = mock_strike_db

        now = datetime.datetime.now(datetime.timezone.utc)

        # Create mock strikes from URL (no IDs, identified by timestamp/location/amplitude)
        strike1 = Mock(spec=Strike)
        strike1.timestamp = Mock()
        strike1.timestamp.value = int(now.timestamp() * 1_000_000_000)
        strike1.timestamp.__le__ = Mock(return_value=True)
        strike1.timestamp.__ge__ = Mock(return_value=True)
        strike1.x = 10.5
        strike1.y = 20.5
        strike1.amplitude = 100

        strike2 = Mock(spec=Strike)
        strike2.timestamp = Mock()
        strike2.timestamp.value = int(now.timestamp() * 1_000_000_000) + 1000
        strike2.timestamp.__le__ = Mock(return_value=True)
        strike2.timestamp.__ge__ = Mock(return_value=True)
        strike2.x = 11.5
        strike2.y = 21.5
        strike2.amplitude = 200

        mock_fetch.return_value = [strike1, strike2]

        # Run update
        result = imprt2.update_strikes(hours=1)

        # Verify
        assert_that(result).is_equal_to(2)
        assert_that(mock_strike_db.insert.call_count).is_equal_to(2)
        mock_strike_db.commit.assert_called()
        mock_strike_db.close.assert_called_once()

    @patch('blitzortung.cli.imprt2.blitzortung.db.strike')
    @patch('blitzortung.cli.imprt2.fetch_strikes_from_url')
    @patch('blitzortung.cli.imprt2.blitzortung.config.config')
    def test_update_strikes_skips_duplicates(self, mock_config, mock_fetch, mock_db_func):
        """Test that existing strikes are not re-inserted."""
        # Setup mocks
        mock_strike_db = Mock()

        now = datetime.datetime.now(datetime.timezone.utc)
        timestamp_value = int(now.timestamp() * 1_000_000_000)

        # Existing strike in database (identified by timestamp/location/amplitude)
        existing_strike = Mock(spec=Strike)
        existing_strike.timestamp = Mock()
        existing_strike.timestamp.value = timestamp_value
        existing_strike.x = 10.5
        existing_strike.y = 20.5
        existing_strike.amplitude = 100

        mock_strike_db.select.return_value = [existing_strike]
        mock_strike_db.insert = Mock()
        mock_strike_db.commit = Mock()
        mock_strike_db.close = Mock()
        mock_db_func.return_value = mock_strike_db

        # Strike from URL (same timestamp/location/amplitude as existing)
        strike_from_url = Mock(spec=Strike)
        strike_from_url.timestamp = Mock()
        strike_from_url.timestamp.value = timestamp_value
        strike_from_url.timestamp.__le__ = Mock(return_value=True)
        strike_from_url.timestamp.__ge__ = Mock(return_value=True)
        strike_from_url.x = 10.5
        strike_from_url.y = 20.5
        strike_from_url.amplitude = 100

        mock_fetch.return_value = [strike_from_url]

        # Run update
        result = imprt2.update_strikes(hours=1)

        # Verify - no inserts should happen
        assert_that(result).is_equal_to(0)
        mock_strike_db.insert.assert_not_called()
        mock_strike_db.close.assert_called_once()

    @patch('blitzortung.cli.imprt2.blitzortung.db.strike')
    @patch('blitzortung.cli.imprt2.fetch_strikes_from_url')
    @patch('blitzortung.cli.imprt2.blitzortung.config.config')
    def test_update_strikes_filters_by_time_interval(self, mock_config, mock_fetch, mock_db_func):
        """Test that strikes outside time interval are filtered."""
        # Setup mocks
        mock_strike_db = Mock()
        mock_strike_db.select.return_value = []
        mock_strike_db.insert = Mock()
        mock_strike_db.commit = Mock()
        mock_strike_db.close = Mock()
        mock_db_func.return_value = mock_strike_db

        now = datetime.datetime.now(datetime.timezone.utc)

        # One strike within interval, one outside
        strike_in_interval = Mock(spec=Strike)
        strike_in_interval.timestamp = Mock()
        strike_in_interval.timestamp.value = int((now - datetime.timedelta(minutes=30)).timestamp() * 1_000_000_000)
        strike_in_interval.timestamp.__le__ = Mock(return_value=True)
        strike_in_interval.timestamp.__ge__ = Mock(return_value=True)
        strike_in_interval.x = 10.5
        strike_in_interval.y = 20.5
        strike_in_interval.amplitude = 100

        strike_outside_interval = Mock(spec=Strike)
        strike_outside_interval.timestamp = Mock()
        strike_outside_interval.timestamp.value = int((now - datetime.timedelta(hours=2)).timestamp() * 1_000_000_000)
        strike_outside_interval.timestamp.__le__ = Mock(return_value=False)  # Outside interval
        strike_outside_interval.timestamp.__ge__ = Mock(return_value=True)
        strike_outside_interval.x = 11.5
        strike_outside_interval.y = 21.5
        strike_outside_interval.amplitude = 200

        mock_fetch.return_value = [strike_in_interval, strike_outside_interval]

        # Run update with 1 hour lookback
        result = imprt2.update_strikes(hours=1)

        # Verify - only the strike within interval should be inserted
        assert_that(result).is_equal_to(1)
        assert_that(mock_strike_db.insert.call_count).is_equal_to(1)
        mock_strike_db.insert.assert_called_with(strike_in_interval)
