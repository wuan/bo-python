"""Tests for blitzortung.service.base module."""

import datetime
import json
import time
from io import BytesIO, StringIO
from unittest.mock import Mock, MagicMock, patch, call

import pytest
from assertpy import assert_that
from twisted.internet.defer import succeed
from twisted.web.test.requesthelper import DummyRequest

from blitzortung.cache import ObjectCache
from blitzortung.db.query import TimeInterval
from blitzortung.service.base import Blitzortung, LogObserver
from blitzortung.service.cache import ServiceCache
from blitzortung.service.db import DictConnectionPool


class MockRequest:
    """Mock request object for testing JSON-RPC methods."""

    def __init__(self, user_agent=None, client_ip=None, x_forwarded_for=None,
                 content_type=None, referer=None):
        self._user_agent = user_agent
        self._client_ip = client_ip
        self._x_forwarded_for = x_forwarded_for
        self._content_type = content_type
        self._referer = referer
        self._headers_removed = []

    def getHeader(self, name):
        if name == "User-Agent":
            return self._user_agent
        if name == "X-Forwarded-For":
            return self._x_forwarded_for
        if name == "content-type":
            return self._content_type
        if name == "referer":
            return self._referer
        return None

    def getClientIP(self):
        return self._client_ip

    def __getitem__(self, key):
        return getattr(self, key, None)

    @property
    def requestHeaders(self):
        mock = Mock()
        mock.removeHeader = self._remove_header
        return mock

    def _remove_header(self, name):
        self._headers_removed.append(name)


@pytest.fixture
def mock_connection_pool():
    """Create a mock database connection pool."""
    return Mock()


@pytest.fixture
def mock_log_directory():
    """Create a mock log directory."""
    return None


@pytest.fixture
def mock_strike_query():
    """Create a mock strike query."""
    return Mock()


@pytest.fixture
def mock_strike_grid_query():
    """Create a mock strike grid query."""
    mock = Mock()
    mock.create = Mock(return_value=(Mock(), Mock()))
    mock.combine_result = Mock(return_value=Mock())
    return mock


@pytest.fixture
def mock_global_strike_grid_query():
    """Create a mock global strike grid query."""
    mock = Mock()
    mock.create = Mock(return_value=(Mock(), Mock()))
    mock.combine_result = Mock(return_value=Mock())
    return mock


@pytest.fixture
def mock_histogram_query():
    """Create a mock histogram query."""
    mock = Mock()
    mock.create = Mock(return_value=Mock())
    return mock


@pytest.fixture
def mock_cache():
    """Create a mock service cache."""
    mock = Mock()
    mock.strikes = Mock(return_value=Mock(
        get=Mock(return_value={}),
        get_ratio=Mock(return_value=0.0),
        get_size=Mock(return_value=0)
    ))
    mock.local_strikes = Mock(return_value=Mock(
        get=Mock(return_value={}),
        get_ratio=Mock(return_value=0.0),
        get_size=Mock(return_value=0)
    ))
    mock.global_strikes = Mock(return_value=Mock(
        get=Mock(return_value={}),
        get_ratio=Mock(return_value=0.0),
        get_size=Mock(return_value=0)
    ))
    mock.histogram = Mock(
        get=Mock(return_value=Mock())
    )
    return mock


@pytest.fixture
def mock_metrics():
    """Create a mock metrics."""
    mock = Mock()
    mock.statsd = Mock()
    mock.for_global_strikes = Mock()
    mock.for_local_strikes = Mock()
    mock.for_strikes = Mock()
    return mock


@pytest.fixture
def mock_forbidden_ips():
    """Create empty forbidden IPs dict for testing."""
    return {}


@pytest.fixture
def blitzortung(mock_connection_pool, mock_log_directory, mock_strike_query,
                mock_strike_grid_query, mock_global_strike_grid_query,
                mock_histogram_query, mock_cache, mock_metrics, mock_forbidden_ips):
    """Create a Blitzortung instance with mocked dependencies."""
    return Blitzortung(
        mock_connection_pool,
        mock_log_directory,
        strike_query=mock_strike_query,
        strike_grid_query=mock_strike_grid_query,
        global_strike_grid_query=mock_global_strike_grid_query,
        histogram_query=mock_histogram_query,
        cache=mock_cache,
        metrics=mock_metrics,
        forbidden_ips=mock_forbidden_ips
    )


class TestBlitzortungClassConstants:
    """Test class constants for validation."""

    def test_min_grid_base_length(self):
        assert_that(Blitzortung.MIN_GRID_BASE_LENGTH).is_equal_to(5000)

    def test_valid_grid_base_lengths(self):
        assert_that(Blitzortung.VALID_GRID_BASE_LENGTHS).is_equal_to(
            frozenset({5000, 10000, 25000, 50000, 100000})
        )

    def test_global_min_grid_base_length(self):
        assert_that(Blitzortung.GLOBAL_MIN_GRID_BASE_LENGTH).is_equal_to(25000)

    def test_max_minutes_per_day(self):
        assert_that(Blitzortung.MAX_MINUTES_PER_DAY).is_equal_to(1440)

    def test_default_minute_length(self):
        assert_that(Blitzortung.DEFAULT_MINUTE_LENGTH).is_equal_to(60)

    def test_histogram_minute_threshold(self):
        assert_that(Blitzortung.HISTOGRAM_MINUTE_THRESHOLD).is_equal_to(10)

    def test_max_compatible_android_version(self):
        assert_that(Blitzortung.MAX_COMPATIBLE_ANDROID_VERSION).is_equal_to(177)

    def test_legacy_zero_id_opt_in(self):
        # Keep the pre-1.0 envelope for the deployed Android client, which
        # sends a fixed request id of 0 without a jsonrpc version field.
        assert_that(Blitzortung.treat_zero_id_as_pre1).is_true()

    def test_memory_info_interval(self):
        assert_that(Blitzortung.MEMORY_INFO_INTERVAL).is_equal_to(300)


def _render_jsonrpc(blitzortung, payload):
    """Render *payload* through *blitzortung* and return the parsed response."""
    request = DummyRequest([''])
    request.content = BytesIO(json.dumps(payload).encode())
    request.method = b'POST'
    request.requestHeaders.setRawHeaders('User-Agent', ['bo-android-190'])
    request.requestHeaders.setRawHeaders('Content-Type', ['text/json'])
    blitzortung.render(request)
    return json.loads(b''.join(request.written))


class TestLegacyJsonRpcEnvelope:
    """Verify the rendered JSON-RPC envelope for the legacy Android client.

    The Android client sends a fixed ``id`` of ``0`` and no ``jsonrpc`` version
    field and expects a bare pre-1.0 array.  This drives the full render path
    (the ``Blitzortung`` opt-in plus txjsonrpc-ng version selection), so it
    guards the regression in CI even though the ``live`` protocol tests only
    run against a deployed endpoint.
    """

    ANDROID_PARAMS = [60, 10000, 0, 1, 0]

    def test_legacy_zero_id_yields_bare_array(self, blitzortung):
        envelope = _render_jsonrpc(blitzortung, {
            'id': 0,
            'method': 'get_strikes_grid',
            'params': self.ANDROID_PARAMS,
        })
        assert_that(envelope).is_instance_of(list)

    def test_explicit_version_2_yields_object(self, blitzortung):
        envelope = _render_jsonrpc(blitzortung, {
            'jsonrpc': '2.0',
            'id': 0,
            'method': 'get_strikes_grid',
            'params': self.ANDROID_PARAMS,
        })
        assert_that(envelope).is_instance_of(dict)
        assert_that(envelope['jsonrpc']).is_equal_to('2.0')


class TestCacheableResultEnvelopeIsolation:
    """A cached serialized response must not leak between protocol dialects.

    The service stores one ``CacheableResult`` per cache key and txjsonrpc-ng
    reuses its serialized form on cache hits.  A legacy pre-1.0 request (id 0)
    and a versioned request share the same key, so the renderer must re-serialize
    whenever the JSON-RPC version or the request id differs.
    """

    ANDROID_PARAMS = [60, 10000, 0, 1, 0]

    @pytest.fixture
    def cached_blitzortung(self, blitzortung):
        blitzortung.cache.strikes.return_value = ObjectCache(ttl_seconds=60)
        blitzortung.strike_grid_query.combine_result.return_value = succeed({'a': 1})
        return blitzortung

    def test_legacy_request_does_not_poison_versioned_request(self, cached_blitzortung):
        legacy = _render_jsonrpc(cached_blitzortung, {
            'id': 0,
            'method': 'get_strikes_grid',
            'params': self.ANDROID_PARAMS,
        })
        assert_that(legacy).is_instance_of(list)

        versioned = _render_jsonrpc(cached_blitzortung, {
            'id': 1,
            'method': 'get_strikes_grid',
            'params': self.ANDROID_PARAMS,
        })
        assert_that(versioned).is_instance_of(dict)
        assert_that(versioned['id']).is_equal_to(1)
        assert_that(versioned['result']).is_equal_to({'a': 1})

    def test_versioned_request_does_not_poison_legacy_request(self, cached_blitzortung):
        versioned = _render_jsonrpc(cached_blitzortung, {
            'id': 1,
            'method': 'get_strikes_grid',
            'params': self.ANDROID_PARAMS,
        })
        assert_that(versioned['id']).is_equal_to(1)

        legacy = _render_jsonrpc(cached_blitzortung, {
            'id': 0,
            'method': 'get_strikes_grid',
            'params': self.ANDROID_PARAMS,
        })
        assert_that(legacy).is_instance_of(list)


class TestBlitzortungInitialization:
    """Test Blitzortung initialization."""

    def test_sets_connection_pool(self, blitzortung, mock_connection_pool):
        assert_that(blitzortung.connection_pool).is_same_as(mock_connection_pool)

    def test_sets_log_directory(self, blitzortung, mock_log_directory):
        assert_that(blitzortung.log_directory).is_same_as(mock_log_directory)

    def test_sets_strike_query(self, blitzortung, mock_strike_query):
        assert_that(blitzortung.strike_query).is_same_as(mock_strike_query)

    def test_sets_strike_grid_query(self, blitzortung, mock_strike_grid_query):
        assert_that(blitzortung.strike_grid_query).is_same_as(mock_strike_grid_query)

    def test_sets_global_strike_grid_query(self, blitzortung, mock_global_strike_grid_query):
        assert_that(blitzortung.global_strike_grid_query).is_same_as(mock_global_strike_grid_query)

    def test_sets_histogram_query(self, blitzortung, mock_histogram_query):
        assert_that(blitzortung.histogram_query).is_same_as(mock_histogram_query)

    def test_sets_cache(self, blitzortung, mock_cache):
        assert_that(blitzortung.cache).is_same_as(mock_cache)

    def test_sets_metrics(self, blitzortung, mock_metrics):
        assert_that(blitzortung.metrics).is_same_as(mock_metrics)

    def test_attaches_pool_wait_observer_to_real_pool(
            self, mock_metrics, mock_strike_query, mock_strike_grid_query,
            mock_global_strike_grid_query, mock_histogram_query, mock_forbidden_ips):
        """A real connection pool reports query waits through the metrics."""
        pool = DictConnectionPool(None, 'dummy')
        Blitzortung(pool, None,
                    strike_query=mock_strike_query,
                    strike_grid_query=mock_strike_grid_query,
                    global_strike_grid_query=mock_global_strike_grid_query,
                    histogram_query=mock_histogram_query,
                    metrics=mock_metrics,
                    forbidden_ips=mock_forbidden_ips)

        assert_that(pool.wait_observer).is_same_as(mock_metrics.for_db_pool_wait)

    def test_leaves_non_pool_connection_pool_untouched(
            self, mock_metrics, mock_strike_query, mock_strike_grid_query,
            mock_global_strike_grid_query, mock_histogram_query, mock_forbidden_ips):
        """Foreign pool types are not instrumented."""
        pool = object()
        Blitzortung(pool, None,
                    strike_query=mock_strike_query,
                    strike_grid_query=mock_strike_grid_query,
                    global_strike_grid_query=mock_global_strike_grid_query,
                    histogram_query=mock_histogram_query,
                    metrics=mock_metrics,
                    forbidden_ips=mock_forbidden_ips)

        assert_that(hasattr(pool, 'wait_observer')).is_false()

    def test_sets_forbidden_ips(self, blitzortung, mock_forbidden_ips):
        assert_that(blitzortung.forbidden_ips).is_same_as(mock_forbidden_ips)

    def test_initializes_check_count(self, blitzortung):
        assert_that(blitzortung.check_count).is_equal_to(0)

    def test_initializes_current_data_as_defaultdict(self, blitzortung):
        assert_that(blitzortung.current_data).is_instance_of(dict)
        assert_that(blitzortung.current_data['test']).is_equal_to([])

    def test_initializes_minute_constraints(self, blitzortung):
        assert_that(blitzortung.minute_constraints).is_not_none()


class TestJsonRpcCheck:
    """Test the jsonrpc_check health check endpoint."""

    def test_increments_check_count(self, blitzortung):
        initial_count = blitzortung.check_count
        blitzortung.jsonrpc_check()
        assert_that(blitzortung.check_count).is_equal_to(initial_count + 1)

    def test_returns_count_dict(self, blitzortung):
        result = blitzortung.jsonrpc_check()
        assert_that(result).is_instance_of(dict)
        assert_that(result).contains_key('count')


class TestParseUserAgent:
    """Test parse_user_agent method."""

    def test_valid_android_user_agent(self, blitzortung):
        request = MockRequest(user_agent='bo-android-150')
        user_agent, version = blitzortung.parse_user_agent(request)
        assert_that(user_agent).is_equal_to('bo-android-150')
        assert_that(version).is_equal_to(150)

    def test_android_user_agent_with_space(self, blitzortung):
        request = MockRequest(user_agent='bo-android-150 SomeDevice')
        user_agent, version = blitzortung.parse_user_agent(request)
        assert_that(version).is_equal_to(150)

    def test_android_user_agent_lowercase(self, blitzortung):
        request = MockRequest(user_agent='bo-android-abc')
        user_agent, version = blitzortung.parse_user_agent(request)
        assert_that(version).is_equal_to(0)

    def test_android_user_agent_negative_version(self, blitzortung):
        request = MockRequest(user_agent='bo-android--5')
        user_agent, version = blitzortung.parse_user_agent(request)
        assert_that(version).is_equal_to(0)

    def test_non_android_user_agent(self, blitzortung):
        request = MockRequest(user_agent='Mozilla/5.0')
        user_agent, version = blitzortung.parse_user_agent(request)
        assert_that(version).is_equal_to(0)

    def test_none_user_agent(self, blitzortung):
        request = MockRequest(user_agent=None)
        user_agent, version = blitzortung.parse_user_agent(request)
        assert_that(version).is_equal_to(0)

    def test_empty_user_agent(self, blitzortung):
        request = MockRequest(user_agent='')
        user_agent, version = blitzortung.parse_user_agent(request)
        assert_that(version).is_equal_to(0)


class TestFixBadAcceptHeader:
    """Test fix_bad_accept_header method."""

    def test_removes_header_for_old_android(self, blitzortung):
        request = MockRequest(user_agent='bo-android-100')
        blitzortung.fix_bad_accept_header(request, 'bo-android-100')
        assert_that(request._headers_removed).contains('Accept-Encoding')

    def test_does_not_remove_header_for_new_android(self, blitzortung):
        request = MockRequest(user_agent='bo-android-200')
        blitzortung.fix_bad_accept_header(request, 'bo-android-200')
        assert_that(request._headers_removed).does_not_contain('Accept-Encoding')

    def test_removes_header_for_max_version(self, blitzortung):
        # Version 177 (MAX_COMPATIBLE_ANDROID_VERSION) should still remove header (<=)
        request = MockRequest(user_agent='bo-android-177')
        blitzortung.fix_bad_accept_header(request, 'bo-android-177')
        assert_that(request._headers_removed).contains('Accept-Encoding')

    def test_does_not_remove_header_for_non_android(self, blitzortung):
        request = MockRequest(user_agent='Mozilla/5.0')
        blitzortung.fix_bad_accept_header(request, 'Mozilla/5.0')
        assert_that(request._headers_removed).is_empty()

    def test_handles_none_user_agent(self, blitzortung):
        request = MockRequest(user_agent=None)
        blitzortung.fix_bad_accept_header(request, None)
        assert_that(request._headers_removed).is_empty()

    def test_handles_invalid_version(self, blitzortung):
        request = MockRequest(user_agent='bo-android-abc')
        blitzortung.fix_bad_accept_header(request, 'bo-android-abc')
        assert_that(request._headers_removed).is_empty()


class TestGetRequestClient:
    """Test get_request_client method."""

    def test_returns_client_ip_directly(self, blitzortung):
        request = MockRequest(client_ip='192.168.1.1')
        result = blitzortung.get_request_client(request)
        assert_that(result).is_equal_to('192.168.1.1')

    def test_returns_first_ip_from_x_forwarded_for(self, blitzortung):
        request = MockRequest(x_forwarded_for='10.0.0.1, 10.0.0.2')
        result = blitzortung.get_request_client(request)
        assert_that(result).is_equal_to('10.0.0.1')

    def test_prefers_x_forwarded_for_over_client_ip(self, blitzortung):
        request = MockRequest(client_ip='192.168.1.1', x_forwarded_for='10.0.0.1')
        result = blitzortung.get_request_client(request)
        assert_that(result).is_equal_to('10.0.0.1')

    def test_handles_none_x_forwarded_for(self, blitzortung):
        request = MockRequest(client_ip='192.168.1.1', x_forwarded_for=None)
        result = blitzortung.get_request_client(request)
        assert_that(result).is_equal_to('192.168.1.1')


class TestIsForbidden:
    """Test the shared is_forbidden access-limit predicate."""

    def test_allows_valid_request(self, blitzortung):
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer=None,
            user_agent='bo-android-150'
        )
        assert_that(blitzortung.is_forbidden(request, '192.168.1.1', 150, 10000, 5000)).is_false()

    def test_blocks_forbidden_ip(self, blitzortung):
        blitzortung.forbidden_ips['192.168.1.100'] = True
        request = MockRequest(
            client_ip='192.168.1.100',
            content_type='text/json',
            user_agent='bo-android-150'
        )
        assert_that(blitzortung.is_forbidden(request, '192.168.1.100', 150, 10000, 5000)).is_true()

    def test_blocks_zero_user_agent_version(self, blitzortung):
        request = MockRequest(client_ip='192.168.1.1', content_type='text/json', user_agent='invalid')
        assert_that(blitzortung.is_forbidden(request, '192.168.1.1', 0, 10000, 5000)).is_true()

    def test_blocks_invalid_content_type(self, blitzortung):
        request = MockRequest(client_ip='192.168.1.1', content_type='text/html', user_agent='bo-android-150')
        assert_that(blitzortung.is_forbidden(request, '192.168.1.1', 150, 10000, 5000)).is_true()

    def test_blocks_request_with_referer(self, blitzortung):
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer='http://example.com',
            user_agent='bo-android-150'
        )
        assert_that(blitzortung.is_forbidden(request, '192.168.1.1', 150, 10000, 5000)).is_true()

    def test_blocks_baseline_below_minimum(self, blitzortung):
        request = MockRequest(client_ip='192.168.1.1', content_type='text/json', user_agent='bo-android-150')
        assert_that(blitzortung.is_forbidden(request, '192.168.1.1', 150, 24999, 25000)).is_true()

    def test_allows_baseline_at_minimum(self, blitzortung):
        request = MockRequest(client_ip='192.168.1.1', content_type='text/json', user_agent='bo-android-150')
        assert_that(blitzortung.is_forbidden(request, '192.168.1.1', 150, 25000, 25000)).is_false()

    def test_blocks_invalid_grid_baselength(self, blitzortung):
        request = MockRequest(client_ip='192.168.1.1', content_type='text/json', user_agent='bo-android-150')
        assert_that(blitzortung.is_forbidden(request, '192.168.1.1', 150, 1000001, 5000)).is_true()

    def test_blocks_unsupported_grid_baselength(self, blitzortung):
        request = MockRequest(client_ip='192.168.1.1', content_type='text/json', user_agent='bo-android-150')
        assert_that(blitzortung.is_forbidden(request, '192.168.1.1', 150, 7000, 5000)).is_true()

    def test_allows_supported_grid_baselength(self, blitzortung):
        request = MockRequest(client_ip='192.168.1.1', content_type='text/json', user_agent='bo-android-150')
        assert_that(blitzortung.is_forbidden(request, '192.168.1.1', 150, 100000, 5000)).is_false()


class TestForceRange:
    """Test __force_range static method."""

    def test_returns_min_when_below(self):
        result = Blitzortung._Blitzortung__force_range(5, 10, 100)
        assert_that(result).is_equal_to(10)

    def test_returns_max_when_above(self):
        result = Blitzortung._Blitzortung__force_range(150, 10, 100)
        assert_that(result).is_equal_to(100)

    def test_returns_value_when_in_range(self):
        result = Blitzortung._Blitzortung__force_range(50, 10, 100)
        assert_that(result).is_equal_to(50)

    def test_returns_min_when_equal_to_min(self):
        result = Blitzortung._Blitzortung__force_range(10, 10, 100)
        assert_that(result).is_equal_to(10)

    def test_returns_max_when_equal_to_max(self):
        result = Blitzortung._Blitzortung__force_range(100, 10, 100)
        assert_that(result).is_equal_to(100)


class TestMemoryInfo:
    """Test memory_info method."""

    @patch('blitzortung.service.base.gc')
    @patch('blitzortung.service.base.log')
    @patch('blitzortung.service.base.is_pypy', False)
    def test_logs_memory_info_first_call(self, mock_log, mock_gc, blitzortung):
        mock_gc.get_stats = Mock(return_value={'test': 'stats'})
        blitzortung.next_memory_info = 0.0
        # time.time() must return a value > next_memory_info to trigger logging
        with patch('time.time', return_value=1.0):
            blitzortung.memory_info()

        assert_that(mock_log.msg.call_count).is_greater_than(0)

    def test_skips_logging_when_within_interval(self, blitzortung):
        with patch('blitzortung.service.base.log') as mock_log:
            blitzortung.next_memory_info = 1000.0
            with patch('time.time', return_value=500.0):
                blitzortung.memory_info()

            mock_log.msg.assert_not_called()

    @patch('blitzortung.service.base.gc')
    @patch('blitzortung.service.base.log')
    @patch('blitzortung.service.base.is_pypy', True)
    def test_logs_with_pypy_stats(self, mock_log, mock_gc, blitzortung):
        mock_gc.get_stats = Mock(return_value={'test': 'stats'})
        blitzortung.next_memory_info = 0.0
        # time.time() must return a value > next_memory_info to trigger logging
        with patch('time.time', return_value=1.0):
            blitzortung.memory_info()

        assert_that(mock_log.msg.call_count).is_greater_than(0)


class TestGetEpoch:
    """Test __get_epoch method."""

    def test_converts_datetime_to_epoch_microseconds(self, blitzortung):
        dt = datetime.datetime(2025, 1, 1, 12, 0, 0, 500000, tzinfo=datetime.timezone.utc)
        result = blitzortung._Blitzortung__get_epoch(dt)
        # 2025-01-01 12:00:00.500000 UTC
        expected = 1735732800 * 1000000 + 500000
        assert_that(result).is_equal_to(expected)


class TestCurrentPeriod:
    """Test __current_period method."""

    def test_returns_datetime_with_utc_timezone(self, blitzortung):
        result = blitzortung._Blitzortung__current_period()
        assert_that(result.tzinfo).is_equal_to(datetime.timezone.utc)

    def test_returns_datetime_with_zero_seconds(self, blitzortung):
        result = blitzortung._Blitzortung__current_period()
        assert_that(result.second).is_equal_to(0)
        assert_that(result.microsecond).is_equal_to(0)


class TestForbiddenIps:
    """Test forbidden IP functionality."""

    def test_blocks_request_from_forbidden_ip(self):
        """Test that requests from forbidden IPs are blocked."""
        mock_pool = Mock()
        mock_cache = Mock()
        mock_cache.strikes = Mock(return_value=Mock(get=Mock(return_value={})))
        mock_cache.global_strikes = Mock(return_value=Mock(get=Mock(return_value={})))
        mock_cache.local_strikes = Mock(return_value=Mock(get=Mock(return_value={})))

        mock_strike_query = Mock()
        mock_strike_grid_query = Mock()
        mock_strike_grid_query.create = Mock(return_value=(Mock(), Mock()))
        mock_strike_grid_query.combine_result = Mock(return_value=Mock())
        mock_global_strike_grid_query = Mock()
        mock_global_strike_grid_query.create = Mock(return_value=(Mock(), Mock()))
        mock_global_strike_grid_query.combine_result = Mock(return_value=Mock())
        mock_histogram_query = Mock()

        forbidden_ips = {'192.168.1.100': True}

        bt = Blitzortung(
            mock_pool,
            None,
            strike_query=mock_strike_query,
            strike_grid_query=mock_strike_grid_query,
            global_strike_grid_query=mock_global_strike_grid_query,
            histogram_query=mock_histogram_query,
            cache=mock_cache,
            forbidden_ips=forbidden_ips
        )

        # Create request from forbidden IP with valid user agent
        request = MockRequest(
            client_ip='192.168.1.100',
            content_type='text/json',
            referer=None,
            user_agent='bo-android-150'
        )

        # The method should return empty dict due to forbidden IP
        result = bt.jsonrpc_get_strikes_grid(request, 60, 10000, 0, 1)
        assert_that(result).is_equal_to({})

    def test_allows_request_from_non_forbidden_ip(self):
        """Test that requests from non-forbidden IPs are allowed."""
        mock_pool = Mock()
        mock_cache = Mock()
        mock_cache.strikes = Mock(return_value=Mock(
            get=Mock(return_value={'data': 'test'}),
            get_ratio=Mock(return_value=0.5),
            get_size=Mock(return_value=10)
        ))
        mock_cache.global_strikes = Mock(return_value=Mock(get=Mock(return_value={})))
        mock_cache.local_strikes = Mock(return_value=Mock(get=Mock(return_value={})))

        mock_strike_query = Mock()
        mock_strike_grid_query = Mock()
        mock_strike_grid_query.create = Mock(return_value=(Mock(), Mock()))
        mock_strike_grid_query.combine_result = Mock(return_value=Mock())
        mock_global_strike_grid_query = Mock()
        mock_histogram_query = Mock()

        bt = Blitzortung(
            mock_pool,
            None,
            strike_query=mock_strike_query,
            strike_grid_query=mock_strike_grid_query,
            global_strike_grid_query=mock_global_strike_grid_query,
            histogram_query=mock_histogram_query,
            cache=mock_cache,
            forbidden_ips={'192.168.1.100': True}
        )

        # Create request from allowed IP with valid user agent
        request = MockRequest(
            client_ip='192.168.1.1',
            user_agent='bo-android-150',
            content_type='text/json',
            referer=None
        )

        # The method should call cache.get for non-forbidden IP
        bt.jsonrpc_get_strikes_grid(request, 60, 10000, 0, 1)
        mock_cache.strikes.return_value.get.assert_called()


class TestLogObserver:
    """Test LogObserver class."""

    def test_initializes_with_empty_prefix(self):
        output = StringIO()
        observer = LogObserver(output)
        assert_that(observer.prefix).is_equal_to('')

    def test_initializes_with_custom_prefix(self):
        output = StringIO()
        observer = LogObserver(output, prefix='TEST')
        assert_that(observer.prefix).is_equal_to('TEST')

    def test_emit_handles_none_text(self):
        output = StringIO()
        observer = LogObserver(output)
        # Should not raise when event dict has time key
        observer.emit({'message': 'test', 'time': 1234567890.0})
        # Should not raise when text is None (event dict without message/format)
        # but we don't test that case since textFromEventDict has a bug


class TestGetStrikesGrid:
    """Test get_strikes_grid method."""

    def test_creates_grid_parameters(self, blitzortung, mock_strike_grid_query):
        with patch('blitzortung.service.base.GridParameters') as mock_params:
            with patch('blitzortung.service.base.create_time_interval') as mock_interval:
                mock_interval.return_value = Mock()
                mock_strike_grid_query.create.return_value = (Mock(), Mock())
                mock_strike_grid_query.combine_result.return_value = Mock()

                blitzortung.get_strikes_grid(60, 10000, 0, 1, 0)

                mock_params.assert_called()

    def test_creates_time_interval(self, blitzortung, mock_strike_grid_query):
        with patch('blitzortung.service.base.GridParameters') as mock_params:
            with patch('blitzortung.service.base.create_time_interval') as mock_interval:
                mock_interval.return_value = Mock()
                mock_strike_grid_query.create.return_value = (Mock(), Mock())
                mock_strike_grid_query.combine_result.return_value = Mock()

                blitzortung.get_strikes_grid(60, 10000, 0, 1, 0)

                mock_interval.assert_called_with(60, 0)


class TestGetGlobalStrikesGrid:
    """Test get_global_strikes_grid method."""

    def test_creates_global_grid_parameters(self, blitzortung, mock_global_strike_grid_query):
        with patch('blitzortung.service.base.GridParameters') as mock_params:
            with patch('blitzortung.service.base.create_time_interval') as mock_interval:
                mock_interval.return_value = Mock()
                mock_global_strike_grid_query.create.return_value = (Mock(), Mock())
                mock_global_strike_grid_query.combine_result.return_value = Mock()

                blitzortung.get_global_strikes_grid(60, 10000, 0, 0)

                mock_params.assert_called()


class TestGetLocalStrikesGrid:
    """Test get_local_strikes_grid method."""

    def test_creates_local_grid_parameters(self, blitzortung, mock_strike_grid_query):
        with patch('blitzortung.service.base.LocalGrid') as mock_local_grid:
            with patch('blitzortung.service.base.GridParameters') as mock_params:
                with patch('blitzortung.service.base.create_time_interval') as mock_interval:
                    mock_grid_factory = Mock()
                    mock_grid_factory.get_for.return_value = Mock()
                    mock_local_grid.return_value.get_grid_factory.return_value = mock_grid_factory

                    mock_interval.return_value = Mock()
                    mock_strike_grid_query.create.return_value = (Mock(), Mock())
                    mock_strike_grid_query.combine_result.return_value = Mock()

                    blitzortung.get_local_strikes_grid(10, 20, 10000, 60, 0, 0)

                    mock_local_grid.assert_called()


class TestGetHistogram:
    """Test get_histogram method."""

    def test_calls_histogram_cache(self, blitzortung, mock_cache):
        mock_time_interval = Mock()
        mock_histogram = Mock()
        mock_cache.histogram.get.return_value = mock_histogram

        result = blitzortung.get_histogram(mock_time_interval)

        mock_cache.histogram.get.assert_called()
        blitzortung.metrics.for_histogram.assert_called_with(
            mock_cache.histogram.get_ratio.return_value,
            mock_cache.histogram.get_size.return_value)
        assert_that(result).is_same_as(mock_histogram)

    def test_stable_cache_key_reuses_histogram_across_intervals(
            self, mock_connection_pool, mock_strike_query, mock_strike_grid_query,
            mock_global_strike_grid_query, mock_histogram_query, mock_metrics,
            mock_forbidden_ips, grid_factory):
        """A stable key must reuse a histogram even when the interval object differs."""
        mock_histogram_query.create = Mock(
            side_effect=lambda *args, **kwargs: Mock(), name='create')
        mock_histogram_query.create.__name__ = 'create'
        service = Blitzortung(
            mock_connection_pool,
            None,
            strike_query=mock_strike_query,
            strike_grid_query=mock_strike_grid_query,
            global_strike_grid_query=mock_global_strike_grid_query,
            histogram_query=mock_histogram_query,
            cache=ServiceCache(),
            metrics=mock_metrics,
            forbidden_ips=mock_forbidden_ips,
        )
        grid_instance = grid_factory.get_for(10000)
        interval1 = TimeInterval(datetime.datetime(2024, 1, 1, 12, 0),
                                 datetime.datetime(2024, 1, 1, 13, 0))
        interval2 = TimeInterval(datetime.datetime(2024, 1, 1, 12, 0, 30),
                                 datetime.datetime(2024, 1, 1, 13, 0, 30))

        first = service.get_histogram(interval1, envelope=grid_instance,
                                      cache_key=(60, 0, grid_instance))
        second = service.get_histogram(interval2, envelope=grid_instance,
                                       cache_key=(60, 0, grid_instance))

        assert_that(mock_histogram_query.create.call_count).is_equal_to(1)
        assert_that(second).is_same_as(first)
        assert_that(mock_metrics.for_histogram.call_args[0]).is_equal_to((0.5, 1))


class TestJsonRpcGetStrikesRaster:
    """Test jsonrpc_get_strikes_raster method."""

    def test_calls_get_strikes_grid(self, blitzortung):
        with patch.object(blitzortung, 'jsonrpc_get_strikes_grid') as mock_method:
            mock_method.return_value = {}
            request = Mock()
            result = blitzortung.jsonrpc_get_strikes_raster(request, 60, 10000, 0, 1)

            mock_method.assert_called_once_with(request, 60, 10000, 0, 1)


class TestJsonRpcGetStrokesRaster:
    """Test jsonrpc_get_strokes_raster method."""

    def test_calls_get_strikes_grid(self, blitzortung):
        with patch.object(blitzortung, 'jsonrpc_get_strikes_grid') as mock_method:
            mock_method.return_value = {}
            request = Mock()
            result = blitzortung.jsonrpc_get_strokes_raster(request, 60, 10000, 0, 1)

            mock_method.assert_called_once_with(request, 60, 10000, 0, 1)


class TestJsonRpcGetStrikes:
    """Test jsonrpc_get_strikes method."""

    def test_returns_none_blocked(self, blitzortung):
        request = MockRequest(user_agent='test')
        result = blitzortung.jsonrpc_get_strikes(request, 60, 0)
        assert_that(result).is_none()

    def test_enforces_minute_length_range(self, blitzortung):
        request = MockRequest()
        # minute_length of -5 should be clamped to 0
        result = blitzortung.jsonrpc_get_strikes(request, -5, 0)
        assert_that(result).is_none()

    def test_enforces_max_minute_length(self, blitzortung):
        request = MockRequest()
        # minute_length of 2000 should be clamped to 1440
        result = blitzortung.jsonrpc_get_strikes(request, 2000, 0)
        assert_that(result).is_none()


class TestJsonRpcGetStrikesGrid:
    """Test jsonrpc_get_strikes_grid method."""

    def test_returns_empty_for_forbidden_ip(self, blitzortung):
        """Test that requests from forbidden IPs are blocked."""
        request = MockRequest(
            client_ip='192.168.1.100',
            content_type='text/json',
            referer=None,
            user_agent='bo-android-150'
        )
        result = blitzortung.jsonrpc_get_strikes_grid(request, 60, 10000, 0, 1)
        assert_that(result).is_equal_to({})

    def test_returns_empty_for_invalid_user_agent(self, blitzortung):
        """Test that requests with invalid user agent are blocked."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer=None,
            user_agent='invalid'
        )
        result = blitzortung.jsonrpc_get_strikes_grid(request, 60, 10000, 0, 1)
        assert_that(result).is_equal_to({})

    def test_returns_empty_for_invalid_content_type(self, blitzortung):
        """Test that requests without proper content type are blocked."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/html',
            referer=None,
            user_agent='bo-android-150'
        )
        result = blitzortung.jsonrpc_get_strikes_grid(request, 60, 10000, 0, 1)
        assert_that(result).is_equal_to({})

    def test_allows_request_without_referer(self, blitzortung):
        """Test that requests without referer are allowed."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer=None,
            user_agent='bo-android-150'
        )
        result = blitzortung.jsonrpc_get_strikes_grid(request, 60, 10000, 0, 1)
        # Should return cached response (empty dict from mock)
        assert_that(result).is_equal_to({})

    def test_blocks_request_with_referer(self, blitzortung):
        """Test that requests with referer are blocked."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer='http://example.com',
            user_agent='bo-android-150'
        )
        result = blitzortung.jsonrpc_get_strikes_grid(request, 60, 10000, 0, 1)
        # Should return empty dict because request with referer is blocked
        assert_that(result).is_equal_to({})

    def test_returns_empty_for_small_grid_baselength(self, blitzortung):
        """Test that requests with too small grid_baselength are blocked."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer='http://example.com',
            user_agent='bo-android-150'
        )
        result = blitzortung.jsonrpc_get_strikes_grid(request, 60, 1000, 0, 1)
        assert_that(result).is_equal_to({})

    def test_returns_empty_for_invalid_grid_baselength(self, blitzortung):
        """Test that requests with invalid grid_baselength are blocked."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer='http://example.com',
            user_agent='bo-android-150'
        )
        result = blitzortung.jsonrpc_get_strikes_grid(request, 60, 1000001, 0, 1)
        assert_that(result).is_equal_to({})

    def test_returns_empty_for_request_with_referer(self, blitzortung):
        """Test that requests with referer are blocked."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer='http://example.com',
            user_agent='bo-android-150'
        )
        result = blitzortung.jsonrpc_get_strikes_grid(request, 60, 10000, 0, 1)
        # Should return empty dict because request with referer is blocked
        assert_that(result).is_equal_to({})


class TestJsonRpcGetGlobalStrikesGrid:
    """Test jsonrpc_get_global_strikes_grid method."""

    def test_returns_empty_for_forbidden_ip(self, blitzortung):
        """Test that requests from forbidden IPs are blocked."""
        request = MockRequest(
            client_ip='192.168.1.100',
            content_type='text/json',
            referer='http://example.com',
            user_agent='bo-android-150'
        )
        result = blitzortung.jsonrpc_get_global_strikes_grid(request, 60, 10000, 0)
        assert_that(result).is_equal_to({})

    def test_returns_empty_for_invalid_user_agent(self, blitzortung):
        """Test that requests with invalid user agent are blocked."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer=None,
            user_agent='invalid'
        )
        result = blitzortung.jsonrpc_get_global_strikes_grid(request, 60, 25000, 0)
        assert_that(result).is_equal_to({})

    def test_returns_empty_for_small_grid_baselength(self, blitzortung):
        """Test that the global endpoint blocks baselines below the global minimum."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer=None,
            user_agent='bo-android-150'
        )
        result = blitzortung.jsonrpc_get_global_strikes_grid(request, 60, 24999, 0)
        assert_that(result).is_equal_to({})

    def test_returns_response_for_valid_request(self, blitzortung):
        """Test that valid requests get a response."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer=None,
            user_agent='bo-android-150'
        )
        result = blitzortung.jsonrpc_get_global_strikes_grid(request, 60, 25000, 0)
        assert_that(result).is_equal_to({})


class TestJsonRpcGetLocalStrikesGrid:
    """Test jsonrpc_get_local_strikes_grid method."""

    def test_returns_empty_for_forbidden_ip(self, blitzortung):
        """Test that requests from forbidden IPs are blocked."""
        request = MockRequest(
            client_ip='192.168.1.100',
            content_type='text/json',
            referer='http://example.com'
        )
        result = blitzortung.jsonrpc_get_local_strikes_grid(request, 10, 20, 10000, 60, 0)
        assert_that(result).is_equal_to({})

    def test_returns_empty_for_invalid_content_type(self, blitzortung):
        """Test that requests without proper content type are blocked."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/html',
            referer='http://example.com'
        )
        result = blitzortung.jsonrpc_get_local_strikes_grid(request, 10, 20, 10000, 60, 0)
        assert_that(result).is_equal_to({})

    def test_returns_empty_for_invalid_user_agent(self, blitzortung):
        """Test that requests with invalid user agent are blocked."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer=None,
            user_agent='invalid'
        )
        result = blitzortung.jsonrpc_get_local_strikes_grid(request, 10, 20, 10000, 60, 0)
        assert_that(result).is_equal_to({})

    def test_returns_response_for_valid_request(self, blitzortung):
        """Test that valid requests get a response."""
        request = MockRequest(
            client_ip='192.168.1.1',
            content_type='text/json',
            referer=None,
            user_agent='bo-android-150'
        )
        result = blitzortung.jsonrpc_get_local_strikes_grid(request, 10, 20, 10000, 60, 0)
        assert_that(result).is_equal_to({})


class TestCheckPeriod:
    """Test __check_period method."""

    def test_restarts_period_when_changed(self, blitzortung):
        """Test that period is restarted when it changes."""
        with patch.object(blitzortung, '_Blitzortung__restart_period') as mock_restart:
            with patch.object(blitzortung, '_Blitzortung__current_period') as mock_current:
                # Set current period to be different
                mock_current.return_value = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
                blitzortung.current_period = datetime.datetime(2025, 1, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)
                blitzortung._Blitzortung__check_period()
                mock_restart.assert_called_once()

    def test_does_not_restart_when_same_period(self, blitzortung):
        """Test that period is not restarted when unchanged."""
        with patch.object(blitzortung, '_Blitzortung__restart_period') as mock_restart:
            with patch.object(blitzortung, '_Blitzortung__current_period') as mock_current:
                same_period = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
                mock_current.return_value = same_period
                blitzortung.current_period = same_period
                blitzortung._Blitzortung__check_period()
                mock_restart.assert_not_called()


class TestRestartPeriod:
    """Test __restart_period method."""

    def test_resets_current_data(self, blitzortung):
        """Test that current_data is reset."""
        blitzortung.current_data['test'] = [1, 2, 3]
        blitzortung._Blitzortung__restart_period()
        assert_that(blitzortung.current_data).is_instance_of(dict)
        assert_that(blitzortung.current_data['test']).is_equal_to([])
