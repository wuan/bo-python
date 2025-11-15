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

import psycopg2
import psycopg2.extras
from twisted.internet.defer import Deferred
from twisted.python import log
from txpostgres import reconnection
from txpostgres.txpostgres import Connection, ConnectionPool

import blitzortung.config
from blitzortung.db.query import SelectQuery


def connection_factory(*args, **kwargs):
    """Create a psycopg2 connection with DictConnection factory."""
    kwargs['connection_factory'] = psycopg2.extras.DictConnection
    return psycopg2.connect(*args, **kwargs)


class LoggingDetector(reconnection.DeadConnectionDetector):
    """Database connection detector that logs reconnection events."""

    def startReconnecting(self, f):
        print('[*] database connection is down (error: %r)' % f.value)
        return reconnection.DeadConnectionDetector.startReconnecting(self, f)

    def reconnect(self):
        print('[*] reconnecting...')
        return reconnection.DeadConnectionDetector.reconnect(self)

    def connectionRecovered(self):
        print('[*] connection recovered')
        return reconnection.DeadConnectionDetector.connectionRecovered(self)


class DictConnection(Connection):
    """Database connection using DictConnection factory with logging detector."""
    connectionFactory = staticmethod(connection_factory)

    def __init__(self, reactor=None, cooperator=None, detector=None):
        if not detector:
            detector = LoggingDetector()
        super(DictConnection, self).__init__(reactor, cooperator, detector)


class DictConnectionPool(ConnectionPool):
    """Connection pool using DictConnection instances."""
    connectionFactory = DictConnection

    def __init__(self, _ignored, *connargs, **connkw):
        super(DictConnectionPool, self).__init__(_ignored, *connargs, **connkw)


def create_connection_pool() -> Deferred:
    """Create and start the database connection pool."""
    config = blitzortung.config.config()
    db_connection_string = config.get_db_connection_string()

    connection_pool = DictConnectionPool(None, db_connection_string)

    d = connection_pool.start()
    d.addErrback(log.err)

    return d


def execute(connection, query: SelectQuery):
    return connection.runQuery(str(query), query.get_parameters())
