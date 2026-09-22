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

from mock import Mock, patch

import blitzortung.db
import blitzortung.db.table


class TestDbModule:

    def test_cleanup_closes_connection_pool(self):
        connection_pool = Mock()
        blitzortung.db.DbModule.cleanup(connection_pool)
        connection_pool.closeall.assert_called_once()


class TestConnectionPoolProvider:

    @patch('blitzortung.db.atexit.register')
    @patch('blitzortung.db.psycopg2.pool.ThreadedConnectionPool')
    def test_provide_psycopg2_connection_pool(self, pool_class, register):
        config = Mock()
        config.get_db_connection_string.return_value = 'dbname=test'
        db_module = blitzortung.db.DbModule()

        connection_pool = db_module.provide_psycopg2_connection_pool(config)

        pool_class.assert_called_once_with(4, 50, 'dbname=test')
        register.assert_called_once_with(db_module.cleanup, connection_pool)
        assert connection_pool is pool_class.return_value


class TestHelperFunctions:

    @patch('blitzortung.INJECTOR')
    def test_strike(self, mock_injector):
        mock_strike_table = Mock(spec=blitzortung.db.table.Strike)
        mock_injector.get.return_value = mock_strike_table

        result = blitzortung.db.strike()

        mock_injector.get.assert_called_once_with(blitzortung.db.table.Strike)
        assert result == mock_strike_table
