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

import time
from collections.abc import Callable

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
        log.msg('[*] database connection is down (error: %r)' % f.value)
        return reconnection.DeadConnectionDetector.startReconnecting(self, f)

    def reconnect(self):
        log.msg('[*] reconnecting...')
        return reconnection.DeadConnectionDetector.reconnect(self)

    def connectionRecovered(self):
        log.msg('[*] connection recovered')
        return reconnection.DeadConnectionDetector.connectionRecovered(self)


class DictConnection(Connection):
    """Database connection using DictConnection factory with logging detector."""
    connectionFactory = staticmethod(connection_factory)

    def __init__(self, reactor=None, cooperator=None, detector=None):
        if not detector:
            detector = LoggingDetector()
        super(DictConnection, self).__init__(reactor, cooperator, detector)


class DictConnectionPool(ConnectionPool):
    """Connection pool using DictConnection instances.

    When a ``wait_observer`` callable is attached, every query reports how
    long it waited for a free pooled connection, which makes pool exhaustion
    visible in the service metrics.
    """
    connectionFactory = DictConnection

    def __init__(self, _ignored, *connargs, **connkw):
        super(DictConnectionPool, self).__init__(_ignored, *connargs, **connkw)
        self.wait_observer: Callable[[float], None] | None = None

    def observe_wait(self, wait_seconds: float) -> None:
        """Forward a measured pool wait to the observer, if one is attached."""
        if self.wait_observer is not None:
            self.wait_observer(wait_seconds)

    def runQuery(self, *args, **kwargs):
        if self.wait_observer is None:
            return super().runQuery(*args, **kwargs)

        started = time.monotonic()
        return self._semaphore.run(self._run_query_observed, started, *args, **kwargs)

    def _run_query_observed(self, started, *args, **kwargs):
        self.observe_wait(time.monotonic() - started)
        return self._runQuery(*args, **kwargs)


def create_connection_pool() -> Deferred:
    """Create and start the database connection pool."""
    config = blitzortung.config.config()
    db_connection_string = config.get_db_connection_string()

    connection_pool = DictConnectionPool(None, db_connection_string,
                                         min=config.get_db_connection_count())

    d: Deferred = connection_pool.start()
    d.addErrback(log.err)

    return d


def execute(connection, query: SelectQuery):
    return connection.runQuery(str(query), query.get_parameters())
