# -*- coding: utf8 -*-

"""

   Copyright 2014-2016 Andreas Würl

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

import atexit

import psycopg2
import psycopg2.pool
import psycopg2.extras
import psycopg2.extensions
from injector import Module, singleton, inject, provider

from . import compat  # Register psycopg2cffi compatibility

from .. import config

from . import query, query_builder, mapper, table


class DbModule(Module):
    @staticmethod
    def cleanup(connection_pool):
        connection_pool.closeall()

    @singleton
    @provider
    @inject
    def provide_psycopg2_connection_pool(self, config: config.Config) -> psycopg2.pool.ThreadedConnectionPool:
        connection_pool = psycopg2.pool.ThreadedConnectionPool(4, 50, config.get_db_connection_string())
        atexit.register(self.cleanup, connection_pool)
        return connection_pool


def strike():
    import blitzortung

    return blitzortung.INJECTOR.get(table.Strike)


def strike_cluster():
    import blitzortung

    return blitzortung.INJECTOR.get(table.StrikeCluster)  # type: ignore[attr-defined]


def station():
    import blitzortung

    return blitzortung.INJECTOR.get(table.Station)  # type: ignore[attr-defined]


def station_offline():
    import blitzortung

    return blitzortung.INJECTOR.get(table.StationOffline)  # type: ignore[attr-defined]


def location():
    import blitzortung

    return blitzortung.INJECTOR.get(table.Location)  # type: ignore[attr-defined]


def servicelog_total():
    import blitzortung

    return blitzortung.INJECTOR.get(table.ServiceLogTotal)  # type: ignore[attr-defined]


def servicelog_country():
    import blitzortung

    return blitzortung.INJECTOR.get(table.ServiceLogCountry)  # type: ignore[attr-defined]


def servicelog_version():
    import blitzortung

    return blitzortung.INJECTOR.get(table.ServiceLogVersion)  # type: ignore[attr-defined]


def servicelog_parameters():
    import blitzortung

    return blitzortung.INJECTOR.get(table.ServiceLogParameters)  # type: ignore[attr-defined]
