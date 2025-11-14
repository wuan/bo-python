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

        with patch('builtins.print') as mock_print:
            with patch.object(detector.__class__.__bases__[0], 'startReconnecting', return_value=None):
                detector.startReconnecting(mock_failure)

                mock_print.assert_called_once()
                assert_that(str(mock_print.call_args[0][0])).contains("database connection is down")
                assert_that(str(mock_print.call_args[0][0])).contains("Connection lost")

    def test_reconnect_logs_message(self):
        detector = blitzortung.service.db.LoggingDetector()

        with patch('builtins.print') as mock_print:
            with patch.object(detector.__class__.__bases__[0], 'reconnect', return_value=None):
                detector.reconnect()

                mock_print.assert_called_once()
                assert_that(str(mock_print.call_args[0][0])).contains("reconnecting")

    def test_connection_recovered_logs_message(self):
        detector = blitzortung.service.db.LoggingDetector()

        with patch('builtins.print') as mock_print:
            with patch.object(detector.__class__.__bases__[0], 'connectionRecovered', return_value=None):
                detector.connectionRecovered()

                mock_print.assert_called_once()
                assert_that(str(mock_print.call_args[0][0])).contains("connection recovered")


@pytest.fixture
def config(connection_string: str):
    with patch('blitzortung.config.config') as mock_config:
        mock_config.return_value.get_db_connection_string.return_value = connection_string
        yield mock_config


@pytest_twisted.inlineCallbacks
def test_database(config, db_strikes):
    deferred_pool = blitzortung.service.db.create_connection_pool()
    async_connection_pool = yield deferred_pool

    query = async_connection_pool.runQuery("select count(*) from strikes;".encode())

    result = yield query

    print("ready", result)

    assert result[0]['count'] == 0
