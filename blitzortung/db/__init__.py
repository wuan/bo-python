# -*- coding: utf8 -*-

"""
Copyright (C) 2012-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

from injector import Module, provides, singleton, inject
import atexit


def create_psycopg2_dummy():
    class Dummy(object):
        pass

    dummy = Dummy()
    dummy.pool = Dummy()
    dummy.pool.ThreadedConnectionPool = Dummy
    return dummy


try:
    import psycopg2
    import psycopg2.pool
    import psycopg2.extras
    import psycopg2.extensions
except ImportError:
    psycopg2 = create_psycopg2_dummy()

from .. import config

from . import query, query_builder, mapper, table


class DbModule(Module):
    @staticmethod
    def cleanup(connection_pool):
        connection_pool.closeall()

    @singleton
    @provides(psycopg2.pool.ThreadedConnectionPool)
    @inject(config=config.Config)
    def provide_psycopg2_connection_pool(self, config):
        connection_pool = psycopg2.pool.ThreadedConnectionPool(4, 50, config.get_db_connection_string())
        atexit.register(self.cleanup, connection_pool)
        return connection_pool


def strike():
    from blitzortung import INJECTOR

    return INJECTOR.get(table.Strike)


def strike_cluster():
    from blitzortung import INJECTOR

    return INJECTOR.get(table.StrikeCluster)


def station():
    from blitzortung import INJECTOR

    return INJECTOR.get(table.Station)


def station_offline():
    from blitzortung import INJECTOR

    return INJECTOR.get(table.StationOffline)


def location():
    from blitzortung import INJECTOR

    return INJECTOR.get(table.Location)


def servicelog_total():
    from blitzortung import INJECTOR

    return INJECTOR.get(table.ServiceLogTotal)


def servicelog_country():
    from blitzortung import INJECTOR

    return INJECTOR.get(table.ServiceLogCountry)


def servicelog_version():
    from blitzortung import INJECTOR

    return INJECTOR.get(table.ServiceLogVersion)
