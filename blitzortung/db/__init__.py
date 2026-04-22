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

import psycopg
import psycopg.rows
import psycopg_pool
from injector import Module, singleton, inject, provider

from .. import config

from . import query, query_builder, mapper, table


class DbModule(Module):
    @staticmethod
    def cleanup(connection_pool):
        connection_pool.closeall()

    @singleton
    @provider
    @inject
    def provide_psycopg_connection_pool(self, config: config.Config) -> psycopg_pool.ConnectionPool:
        connection_pool = psycopg_pool.ConnectionPool(
            config.get_db_connection_string(),
            min_connections=4,
            max_connections=50
        )
        atexit.register(self.cleanup, connection_pool)
        return connection_pool


def strike():
    from .. import INJECTOR

    return INJECTOR.get(table.Strike)


def strike_cluster():
    from .. import INJECTOR

    return INJECTOR.get(table.StrikeCluster)


def station():
    from .. import INJECTOR

    return INJECTOR.get(table.Station)


def station_offline():
    from .. import INJECTOR

    return INJECTOR.get(table.StationOffline)


def location():
    from .. import INJECTOR

    return INJECTOR.get(table.Location)


def servicelog_total():
    from .. import INJECTOR

    return INJECTOR.get(table.ServiceLogTotal)


def servicelog_country():
    from .. import INJECTOR

    return INJECTOR.get(table.ServiceLogCountry)


def servicelog_version():
    from .. import INJECTOR

    return INJECTOR.get(table.ServiceLogVersion)


def servicelog_parameters():
    from .. import INJECTOR

    return INJECTOR.get(table.ServiceLogParameters)
