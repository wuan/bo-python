# -*- coding: utf8 -*-

"""

   Copyright 2025 Andreas Würl

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

import pytest
import pytest_twisted
from assertpy import assert_that
from mock import Mock, patch

import blitzortung.service.db


class TestLoggingDetector:

    def test_start_reconnecting_logs_error(self):
        detector = blitzortung.service.db.LoggingDetector()
        mock_failure = Mock()
        mock_failure.value = Exception("Connection lost")

        with patch('blitzortung.service.db.log.msg') as mock_log:
            with patch.object(detector.__class__.__bases__[0], 'startReconnecting', return_value=None):
                detector.startReconnecting(mock_failure)

                mock_log.assert_called_once()
                assert_that(str(mock_log.call_args[0][0])).contains("database connection is down")
                assert_that(str(mock_log.call_args[0][0])).contains("Connection lost")

    def test_reconnect_logs_message(self):
        detector = blitzortung.service.db.LoggingDetector()

        with patch('blitzortung.service.db.log.msg') as mock_log:
            with patch.object(detector.__class__.__bases__[0], 'reconnect', return_value=None):
                detector.reconnect()

                mock_log.assert_called_once()
                assert_that(str(mock_log.call_args[0][0])).contains("reconnecting")

    def test_connection_recovered_logs_message(self):
        detector = blitzortung.service.db.LoggingDetector()

        with patch('blitzortung.service.db.log.msg') as mock_log:
            with patch.object(detector.__class__.__bases__[0], 'connectionRecovered', return_value=None):
                detector.connectionRecovered()

                mock_log.assert_called_once()
                assert_that(str(mock_log.call_args[0][0])).contains("connection recovered")


@pytest.fixture
def config(connection_string: str):
    with patch('blitzortung.config.config') as mock_config:
        mock_config.return_value.get_db_connection_string.return_value = connection_string
        mock_config.return_value.get_db_connection_count.return_value = 3
        yield mock_config


class FakeSemaphore:
    """Immediately runs the scheduled callable, simulating a free token."""

    def run(self, func, *args, **kwargs):
        return func(*args, **kwargs)


class TestDictConnectionPoolInstrumentation:
    """Test that query pool wait time is measured and reported."""

    @staticmethod
    def _pool():
        pool = blitzortung.service.db.DictConnectionPool(None, 'dummy')
        pool._semaphore = FakeSemaphore()
        return pool

    def test_observe_wait_without_observer_is_noop(self):
        pool = self._pool()
        pool.observe_wait(0.5)
        assert_that(pool.wait_observer).is_none()

    def test_run_query_without_observer_executes_query(self):
        pool = self._pool()
        executed = []
        pool._runQuery = lambda *args, **kwargs: executed.append((args, kwargs))

        pool.runQuery('select 1', ())

        assert_that(executed).is_equal_to([(('select 1', ()), {})])

    def test_run_query_reports_wait_to_observer(self):
        pool = self._pool()
        waits = []
        pool.wait_observer = waits.append
        executed = []
        pool._runQuery = lambda *args, **kwargs: executed.append((args, kwargs))

        pool.runQuery('select 1', ())

        assert_that(waits).is_length(1)
        assert_that(waits[0]).is_greater_than_or_equal_to(0)
        assert_that(executed).is_equal_to([(('select 1', ()), {})])


@pytest_twisted.inlineCallbacks
def test_database(config, db_strikes):
    deferred_pool = blitzortung.service.db.create_connection_pool()
    async_connection_pool = yield deferred_pool

    query = async_connection_pool.runQuery("select count(*) from strikes;".encode())

    result = yield query

    print("ready", result)

    assert result[0]['count'] == 0
