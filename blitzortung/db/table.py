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
from abc import ABCMeta, abstractmethod
import datetime
import logging
from typing import Optional

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.pool
from injector import inject

from blitzortung.db.grid_result import build_grid_result

from . import mapper
from . import query_builder
from .query import TimeInterval
from .. import data
from .. import geom
from ..logger import get_logger_name


class Base(metaclass=ABCMeta):
    """
    abstract base class for database access objects

    creation of database

    as user postgres:

    createuser -i -D -R -S -W -E -P blitzortung
    createdb -E utf8 -O blitzortung blitzortung
    createlang plpgsql blitzortung
    psql -f /usr/share/postgresql/10/contrib/postgis-2.4/postgis.sql -d blitzortung
    psql -f /usr/share/postgresql/12/contrib/postgis-3.0/postgis.sql -d blitzortung
    psql -f /usr/share/postgresql/10/contrib/postgis-2.4/spatial_ref_sys.sql -d blitzortung

    psql blitzortung

    GRANT SELECT ON spatial_ref_sys TO blitzortung;
    GRANT SELECT ON geometry_columns TO blitzortung;
    GRANT INSERT, DELETE ON geometry_columns TO blitzortung;
    CREATE EXTENSION "btree_gist";

    """

    default_timezone = datetime.timezone.utc

    def __init__(self, db_connection_pool):

        self.logger = logging.getLogger(get_logger_name(self.__class__))
        self.db_connection_pool = db_connection_pool

        self.schema_name = ""
        self.table_name = ""

        while True:
            self.conn = self.db_connection_pool.getconn()
            self._cancel_if_active(self.conn)
            try:
                self.conn.reset()
            except psycopg2.OperationalError:
                self.logger.warning("reconnect to db")
                self.db_connection_pool.putconn(self.conn, close=True)
                continue
            break

        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, self.conn)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, self.conn)
        self.conn.set_client_encoding('UTF8')

        self.srid = geom.Geometry.default_srid
        self.tz = None
        self.set_timezone(Base.default_timezone)

        cur = None
        try:
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except psycopg2.DatabaseError as error:
            self.logger.error(error)

            if self.conn:
                try:
                    self.conn.close()
                except NameError:
                    pass
        finally:
            if cur:
                cur.close()

    @staticmethod
    def _cancel_if_active(conn):
        """Cancel a query still running on a borrowed connection.

        ``cancel`` opens a second server connection, so it is only worth doing
        when the connection is actually executing a query.  Idle connections
        are simply reset.
        """
        if conn.info.transaction_status == psycopg2.extensions.TRANSACTION_STATUS_ACTIVE:
            conn.cancel()

    def close(self):
        self.db_connection_pool.putconn(self.conn)

    def is_connected(self):
        if self.conn:
            return not self.conn.closed
        else:
            return False

    @property
    def full_table_name(self):
        if self.schema_name:
            return '"' + self.schema_name + '"."' + self.table_name + '"'
        else:
            return self.table_name

    def get_srid(self):
        return self.srid

    def set_srid(self, srid):
        self.srid = srid

    def get_timezone(self):
        return self.tz

    def set_timezone(self, tz):
        self.tz = tz
        with self.conn.cursor() as cur:
            cur.execute("SET TIME ZONE %s", (str(self.tz),))

    def fix_timezone(self, timestamp):
        return timestamp.astimezone(self.tz) if timestamp else None

    def from_bare_utc_to_timezone(self, utc_time):
        return utc_time.replace(tzinfo=datetime.timezone.utc).astimezone(self.tz)

    @staticmethod
    def from_timezone_to_bare_utc(time_with_tz):
        return time_with_tz.astimezone(datetime.timezone.utc).replace(tzinfo=None)

    def commit(self):
        """ commit pending database transaction """
        self.conn.commit()

    def rollback(self):
        """ rollback pending database transaction """
        self.conn.rollback()

    @abstractmethod
    def insert(self, *args):
        pass

    @abstractmethod
    def select(self, **kwargs):
        pass

    def execute(self, sql_statement, parameters=None, factory_method=None, **factory_method_args):
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(sql_statement, parameters)
            if factory_method:
                method = factory_method(cursor, **factory_method_args)
                return method

    def execute_single(self, sql_statement, parameters=None, factory_method=None, **factory_method_args):
        def single_cursor_factory(cursor):
            if cursor.rowcount == 1:
                return factory_method(cursor.fetchone(), **factory_method_args)

        return self.execute(sql_statement, parameters, single_cursor_factory)

    def execute_many(self, sql_statement, parameters=None, factory_method=None, **factory_method_args):
        factory_method = factory_method or (lambda values, **_: values)
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(sql_statement, parameters)
            for value in cursor:
                yield factory_method(value, **factory_method_args)


class Strike(Base):
    """
    strike db access class

    database table creation (as db user blitzortung, database blitzortung):

    > psql -h localhost -U blitzortung -W blitzortung

    CREATE TABLE strikes (id bigserial, "timestamp" timestamptz, nanoseconds SMALLINT, geog GEOGRAPHY(Point),
        PRIMARY KEY(id));
    ALTER TABLE strikes ADD COLUMN altitude SMALLINT;
    ALTER TABLE strikes ADD COLUMN region SMALLINT;
    ALTER TABLE strikes ADD COLUMN amplitude REAL;
    ALTER TABLE strikes ADD COLUMN error2d SMALLINT;
    ALTER TABLE strikes ADD COLUMN stationcount SMALLINT;

    CREATE INDEX strikes_timestamp ON strikes USING btree("timestamp");
    CREATE INDEX strikes_timestamp_geog ON strikes USING gist("timestamp", geog);
    CREATE INDEX strikes_region_timestamp ON strikes USING btree(region, "timestamp");
    CREATE INDEX strikes_id_timestamp ON strikes USING btree(id, "timestamp");
    CREATE INDEX strikes_geog ON strikes USING gist(geog);
    CREATE INDEX strikes_id_timestamp_geog ON strikes USING gist(id, "timestamp", geog);
    CREATE INDEX strikes_region_timestamp_nanoseconds ON strikes USING btree(region, "timestamp", nanoseconds);

    empty the table with the following commands:

    DELETE FROM strikes;
    ALTER SEQUENCE strikes_id_seq RESTART 1;

    """

    table_name = 'strikes'

    @inject
    def __init__(self, db_connection_pool: psycopg2.pool.ThreadedConnectionPool, query_builder_: query_builder.Strike,
                 strike_mapper: mapper.Strike):
        super().__init__(db_connection_pool)

        self.table_name = Strike.table_name
        self.query_builder = query_builder_
        self.strike_mapper = strike_mapper

    def insert(self, strike, region=1):
        sql = 'INSERT INTO ' + self.full_table_name + \
              ' ("timestamp", nanoseconds, geog, altitude, region, amplitude, error2d, stationcount) ' + \
              'VALUES (%(timestamp)s, %(nanoseconds)s, ST_MakePoint(%(longitude)s, %(latitude)s), ' + \
              '%(altitude)s, %(region)s, %(amplitude)s, %(error2d)s, %(stationcount)s)'

        parameters = {
            'timestamp': strike.timestamp.datetime,
            'nanoseconds': strike.timestamp.nanosecond,
            'longitude': strike.x,
            'latitude': strike.y,
            'altitude': strike.altitude,
            'region': region,
            'amplitude': strike.amplitude,
            'error2d': strike.lateral_error,
            'stationcount': strike.station_count
        }

        self.execute(sql, parameters)

    def insert_many(self, strikes, region=None):
        """Insert multiple strikes using a single round trip per page.

        ``region`` may pin every inserted strike to the same region.  When it
        is ``None`` the region stored on each strike is used, falling back to
        region 1.  Returns the number of inserted strikes.
        """
        sql = 'INSERT INTO ' + self.full_table_name + \
              ' ("timestamp", nanoseconds, geog, altitude, region, amplitude, error2d, stationcount) ' + \
              'VALUES %s'
        template = '(%s, %s, ST_MakePoint(%s, %s), %s, %s, %s, %s, %s)'

        values = [
            (
                strike.timestamp.datetime,
                strike.timestamp.nanosecond,
                strike.x,
                strike.y,
                strike.altitude,
                region if region is not None else (strike.region if strike.region is not None else 1),
                strike.amplitude,
                strike.lateral_error,
                strike.station_count
            )
            for strike in strikes
        ]

        if not values:
            return 0

        with self.conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, sql, values, template=template)

        return len(values)

    def get_latest_time(self, region=None):
        sql = 'SELECT "timestamp", nanoseconds FROM ' + self.full_table_name + \
              (' WHERE region=%(region)s' if region else '') + \
              ' ORDER BY "timestamp" DESC, nanoseconds DESC LIMIT 1'

        def prepare_result(result):
            return data.Timestamp(self.fix_timezone(result['timestamp']), result['nanoseconds'])

        parameters = {'region': region}
        return self.execute_single(sql, parameters, prepare_result)

    def select(self, **kwargs):
        """ build up query """

        query_ = self.query_builder.select_query(self.full_table_name, self.srid, **kwargs)

        return self.execute_many(str(query_), query_.get_parameters(), self.strike_mapper.create_object,
                                 timezone=self.tz)

    @staticmethod
    def _create_strike_key(result):
        timestamp = data.Timestamp(result['timestamp'], result['nanoseconds'])
        return (timestamp.value, round(result['x'], 4), round(result['y'], 4), result['error2d'])

    def select_strike_keys(self, **kwargs):
        """Select only the fields needed to identify a strike.

        Returns an iterator over ``(timestamp, x, y, lateral_error)`` tuples,
        avoiding the cost of building full :class:`~blitzortung.data.Strike`
        objects (used by the URL updater for de-duplication).
        """

        query = self.query_builder.select_key_query(self.full_table_name, self.srid, **kwargs)

        return self.execute_many(str(query), query.get_parameters(), self._create_strike_key)

    def select_grid(self, grid, count_threshold=0, **kwargs):
        """ build up raster query """

        query = self.query_builder.grid_query(self.full_table_name, grid, count_threshold, **kwargs)
        data = self.execute_many(str(query), query.get_parameters())

        grid_result = build_grid_result(data, grid.x_bin_count, grid.y_bin_count, kwargs['time_interval'].end)

        return grid_result

    def select_global_grid(self, grid, count_threshold=0, **kwargs):
        """ build up raster query """

        query = self.query_builder.global_grid_query(self.full_table_name, grid, count_threshold, **kwargs)

        data = self.execute_many(str(query), query.get_parameters())

        grid_result = build_grid_result(data, grid.x_bin_count, grid.y_bin_count, kwargs['time_interval'].end)

        return grid_result

    def select_histogram(self, time_interval: TimeInterval, binsize: int = 5, region: Optional[int] = None,
                         envelope=None):

        query = self.query_builder.histogram_query( self.full_table_name, time_interval, binsize, region, envelope )

        minutes = time_interval.minutes()

        def prepare_result(cursor):
            value_count = minutes // binsize

            result = [0] * value_count

            for bin_data in cursor:
                result[bin_data[0] + value_count - 1] = bin_data[1]

            return result

        return self.execute(str(query), query.get_parameters(), prepare_result)
