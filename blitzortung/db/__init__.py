# -*- coding: utf8 -*-

""" classes for database access """

from injector import Module, provides, singleton, inject
import atexit

try:
    import psycopg2
    import psycopg2.pool
    import psycopg2.extras
    import psycopg2.extensions
except ImportError:
    psycopg2 = None

import blitzortung
import table


class DbModule(Module):
    @staticmethod
    def cleanup(connection_pool):
        connection_pool.closeall()

    @singleton
    @provides(psycopg2.pool.ThreadedConnectionPool)
    @inject(config=blitzortung.config.Config)
    def provide_psycopg2_connection_pool(self, config):
        connection_pool = psycopg2.pool.ThreadedConnectionPool(4, 50, config.get_db_connection_string())
        atexit.register(self.cleanup, connection_pool)
        return connection_pool


def stroke():
    from blitzortung import INJECTOR

    return INJECTOR.get(blitzortung.db.table.Stroke)


def station():
    from blitzortung import INJECTOR

    return INJECTOR.get(blitzortung.db.table.Station)


def station_offline():
    from blitzortung import INJECTOR

    return INJECTOR.get(blitzortung.db.table.StationOffline)


def location():
    from blitzortung import INJECTOR

    return INJECTOR.get(blitzortung.db.table.Location)


def servicelog():
    from blitzortung import INJECTOR

    return INJECTOR.get(blitzortung.db.table.ServiceLog)
