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
import datetime
import logging

import math
from injector import inject

from . import mapper
from . import query
from . import query_builder
from .. import data
from .. import geom
from ..logger import get_logger_name

try:
    import psycopg2
    import psycopg2.pool
    import psycopg2.extras
    import psycopg2.extensions
except ImportError as e:
    from . import create_psycopg2_dummy

    psycopg2 = create_psycopg2_dummy()

from abc import ABCMeta, abstractmethod


class Base:
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
    __metaclass__ = ABCMeta

    default_timezone = datetime.timezone.utc

    def __init__(self, db_connection_pool):

        self.logger = logging.getLogger(get_logger_name(self.__class__))
        self.db_connection_pool = db_connection_pool

        self.schema_name = ""
        self.table_name = ""

        while True:
            self.conn = self.db_connection_pool.getconn()
            self.conn.cancel()
            try:
                self.conn.reset()
            except psycopg2.OperationalError:
                print("reconnect to db")
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

    def __del__(self):
        try:
            if not self.conn.closed:
                self.db_connection_pool.putconn(self.conn)
        except (psycopg2.pool.PoolError, AttributeError):
            pass

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
            cur.execute('SET TIME ZONE \'%s\'' % str(self.tz))

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
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(sql_statement, parameters)
            if factory_method:
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
    CREATE INDEX strikes_region_timestamp_nanoseconds ON strikes USING btree(region, "timestamp", nanoseconds);
    CREATE INDEX strikes_id_timestamp ON strikes USING btree(id, "timestamp");
    CREATE INDEX strikes_geog ON strikes USING gist(geog);
    CREATE INDEX strikes_timestamp_geog ON strikes USING gist("timestamp", geog);
    CREATE INDEX strikes_id_timestamp_geog ON strikes USING gist(id, "timestamp", geog);

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

    def select_grid(self, grid, count_threshold, **kwargs):
        """ build up raster query """

        query = self.query_builder.grid_query(self.table_name, grid, count_threshold, **kwargs)

        def prepare_results(cursor, _):
            raster_data = data.GridData(grid)

            for result in cursor:
                raster_data.set(result['rx'], result['ry'],
                                geom.GridElement(result['count'], result['timestamp']))

            return raster_data

        return self.execute_base(str(query), query.get_parameters(), prepare_results)

    def select_histogram(self, minutes, minute_offset=0, binsize=5, region=None, envelope=None):

        query = self.query_builder.histogram_query(
            self.full_table_name,
            minutes, minute_offset, binsize,
            region, envelope
        )

        def prepare_result(cursor, _):
            value_count = minutes / binsize

            result = [0] * value_count

            for bin_data in cursor:
                result[bin_data[0] + value_count - 1] = bin_data[1]

            return result

        return self.execute(str(query), query.get_parameters(), prepare_result)


class Station(Base):
    """

    database table creation (as db user blitzortung, database blitzortung):

    CREATE TABLE stations (id bigserial, number int, "user" int, geog GEOGRAPHY(Point), PRIMARY KEY(id));
    ALTER TABLE stations ADD COLUMN region SMALLINT;
    ALTER TABLE stations ADD COLUMN name CHARACTER VARYING;
    ALTER TABLE stations ADD COLUMN country CHARACTER VARYING;
    ALTER TABLE stations ADD COLUMN "timestamp" TIMESTAMPTZ;

    CREATE INDEX stations_timestamp ON stations USING btree("timestamp");
    CREATE INDEX stations_number_timestamp ON stations USING btree(number, "timestamp");
    CREATE INDEX stations_geog ON stations USING gist(geog);

    empty the table with the following commands:

    DELETE FROM stations;
    ALTER SEQUENCE stations_id_seq RESTART 1;
    """

    @inject
    def __init__(self, db_connection_pool: psycopg2.pool.ThreadedConnectionPool, station_mapper: mapper.Station):
        super().__init__(db_connection_pool)

        self.table_name = 'stations'
        self.station_mapper = station_mapper

    def insert(self, station, region=1):
        self.execute('INSERT INTO ' + self.full_table_name +
                     ' (number, "user", "name", country, "timestamp", geog, region) ' +
                     'VALUES (%s, %s, %s, %s, %s, ST_MakePoint(%s, %s), %s)',
                     (station.number, station.user, station.name,
                      station.country, station.timestamp.datetime, station.x, station.y, region))

    def select(self, timestamp=None, region=None):
        sql = ''' select
             o.begin, s.number, s.user, s.name, s.country, s.geog
        from stations as s
        inner join
           (select b. region, b.number, max(b."timestamp") as "timestamp"
            from stations as b
        group by region, number
        order by region, number) as c
        on s.region = c.region and s.number = c.number and s."timestamp" = c."timestamp"
        left join stations_offline as o
        on o.number = s.number and o.region = s.region and o."end" is null'''

        if region:
            sql += ''' where s.region = %(region)s'''

        sql += ''' order by s.number'''

        return self.execute_many(sql, {'region': region}, self.station_mapper.create_object)


class StationOffline(Base):
    """

    database table creation (as db user blitzortung, database blitzortung):

    CREATE TABLE stations_offline (id bigserial, number int, PRIMARY KEY(id));
    ALTER TABLE stations_offline ADD COLUMN region SMALLINT;
    ALTER TABLE stations_offline ADD COLUMN begin TIMESTAMPTZ;
    ALTER TABLE stations_offline ADD COLUMN "end" TIMESTAMPTZ;

    CREATE INDEX stations_offline_begin ON stations_offline USING btree(begin);
    CREATE INDEX stations_offline_end ON stations_offline USING btree("end");
    CREATE INDEX stations_offline_end_number ON stations_offline USING btree("end", number);
    CREATE INDEX stations_offline_begin_end ON stations_offline USING btree(begin, "end");

    empty the table with the following commands:

    DELETE FROM stations_offline;
    ALTER SEQUENCE stations_offline_id_seq RESTART 1;
    """

    @inject
    def __init__(self, db_connection_pool: psycopg2.pool.ThreadedConnectionPool,
                 station_offline_mapper: mapper.StationOffline):
        super().__init__(db_connection_pool)

        self.table_name = 'stations_offline'
        self.station_offline_mapper = station_offline_mapper

    def insert(self, station_offline, region=1):
        self.execute('INSERT INTO ' + self.full_table_name +
                     ' (number, region, begin, "end") ' +
                     'VALUES (%s, %s, %s, %s)',
                     (station_offline.number, region, station_offline.begin.datetime,
                      station_offline.end.datetime if station_offline.end else None))

    def update(self, station_offline, region=1):
        self.execute('UPDATE ' + self.full_table_name + ' SET "end"=%s WHERE id=%s and region=%s',
                     (station_offline.end, station_offline.id, region))

    def select(self, timestamp=None, region=1):
        sql = '''select id, number, region, begin, "end"
            from stations_offline where "end" is null and region=%s order by number;'''

        return self.execute_many(sql, (region,), self.station_offline_mapper.create_object)


class Location(Base):
    """
    geonames db access class

    CREATE SCHEMA geo;

    CREATE TABLE geo.geonames (id bigserial, "name" character varying, geog Geography(Point), PRIMARY KEY(id));

    ALTER TABLE geo.geonames ADD COLUMN "class" INTEGER;
    ALTER TABLE geo.geonames ADD COLUMN feature_class CHARACTER(1);
    ALTER TABLE geo.geonames ADD COLUMN feature_code VARCHAR;
    ALTER TABLE geo.geonames ADD COLUMN country_code VARCHAR;
    ALTER TABLE geo.geonames ADD COLUMN admin_code_1 VARCHAR;
    ALTER TABLE geo.geonames ADD COLUMN admin_code_2 VARCHAR;
    ALTER TABLE geo.geonames ADD COLUMN population INTEGER;
    ALTER TABLE geo.geonames ADD COLUMN elevation SMALLINT;

    CREATE INDEX geonames_geog ON geo.geonames USING gist(geog);

    """

    @inject
    def __init__(self, db_connection_pool: psycopg2.pool.ThreadedConnectionPool):
        super().__init__(db_connection_pool)
        self.schema_name = 'geo'
        self.table_name = 'geonames'
        self.center = None
        self.min_population = None
        self.limit = None
        self.max_distance = None

    def delete_all(self):
        self.execute('DELETE FROM ' + self.full_table_name)

    def insert(self, line):
        fields = line.strip().split('\t')
        name = fields[1]
        y = float(fields[4])
        x = float(fields[5])
        feature_class = fields[6]
        feature_code = fields[7]
        country_code = fields[8]
        admin_code_1 = fields[10]
        admin_code_2 = fields[11]
        population = int(fields[14])
        if fields[15] != '':
            elevation = int(fields[15])
        else:
            elevation = -1

        name = name.replace("'", "''")

        classification = self.determine_size_class(population)

        if classification is not None:
            self.execute('INSERT INTO ' + self.full_table_name +
                         '(geog, name, class, feature_class, feature_code, country_code, admin_code_1, admin_code_2, ' +
                         'population, elevation)' +
                         'VALUES(ST_GeomFromText(\'POINT(%s %s)\', 4326), %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                         (x, y, name, classification, feature_class, feature_code, country_code, admin_code_1,
                          admin_code_2, population, elevation))

    @staticmethod
    def determine_size_class(n):
        if n < 1:
            return None
        base = math.floor(math.log(n) / math.log(10)) - 1
        relative = n / math.pow(10, base)
        order = min(2, math.floor(relative / 25))
        if base < 0:
            base = 0
        return min(15, base * 3 + order)

    def create_object_instance(self, result):
        pass

    def select(self, *args):
        self.center = None
        self.min_population = 1000
        self.max_distance = 10000
        self.limit = 10

        for arg in args:
            if arg:
                if isinstance(arg, query.Center):
                    self.center = arg
                elif isinstance(arg, query.Limit):
                    self.limit = arg

        if self.is_connected():
            query_string = '''SELECT
                name,
                country_code,
                admin_code_1,
                admin_code_2,
                feature_class,
                feature_code,
                elevation,
                ST_Transform(geog::geometry, %(srid)s) AS geog,
                population,
                ST_Distance_Sphere(geog::geometry, c.center) AS distance,
                ST_Azimuth(geog::geometry, c.center) AS azimuth
            FROM
                (SELECT ST_SetSRID(ST_MakePoint(%(center_x)s, %(center_y)s), %(srid)s) as center ) as c,''' + \
                           self.full_table_name + '''
            WHERE
                feature_class='P'
                AND population >= %(min_population)s
                AND ST_Transform(geog::geometry, %(srid)s) && ST_Expand(c.center, %(max_distance)s)
            ORDER BY distance
            LIMIT %(limit)s'''

            params = {
                'srid': self.srid,
                'center_x': self.center.get_point().x,
                'center_y': self.center.get_point().y,
                'min_population': self.min_population,
                'max_distance': self.max_distance,
                'limit': self.limit
            }

            def build_results(cursor, _):
                locations = tuple(
                    {
                        'name': result['name'],
                        'distance': result['distance'],
                        'azimuth': result['azimuth']
                    } for result in cursor
                )

                return locations

            return self.execute_many(query_string, params, build_results)


class ServiceLogBase(Base):
    """

    Base class for servicelog tables
    """

    def get_latest_time(self):
        sql = 'SELECT "timestamp" FROM ' + self.full_table_name + \
              ' ORDER BY "timestamp" DESC LIMIT 1'

        def prepare_result(cursor, _):
            if cursor.rowcount == 1:
                result = cursor.fetchone()
                return data.Timestamp(self.fix_timezone(result['timestamp']))
            else:
                return None

        return self.execute(sql, factory_method=prepare_result)

    def select(self, args):
        pass

    def create_object_instance(self, result):
        pass


class ServiceLogTotal(ServiceLogBase):
    """
        CREATE TABLE servicelog_total ("timestamp" TIMESTAMPTZ, count INT);

        CREATE INDEX servicelog_total_timestamp ON servicelog_total USING btree("timestamp");
    """

    @inject
    def __init__(self, db_connection_pool: psycopg2.pool.ThreadedConnectionPool):
        super().__init__(db_connection_pool)

        self.table_name = 'servicelog_total'

    def insert(self, timestamp, count):
        sql = 'INSERT INTO ' + self.full_table_name + ' ' + \
              '("timestamp", count)' + \
              'VALUES (%(timestamp)s, %(count)s);'

        parameters = {
            'timestamp': timestamp,
            'count': count
        }

        self.execute(sql, parameters)


class ServiceLogCountry(ServiceLogBase):
    """
        CREATE TABLE servicelog_country ("timestamp" TIMESTAMPTZ, country_code CHARACTER VARYING, "count" INT);

        CREATE INDEX servicelog_country_timestamp ON servicelog_country USING btree("timestamp");
    """

    @inject
    def __init__(self, db_connection_pool: psycopg2.pool.ThreadedConnectionPool):
        super().__init__(db_connection_pool)

        self.table_name = 'servicelog_country'

    def insert(self, timestamp, country_code, count):
        sql = 'INSERT INTO ' + self.full_table_name + ' ' + \
              '("timestamp", country_code, "count")' + \
              'VALUES (%(timestamp)s, %(country_code)s, %(count)s);'

        parameters = {
            'timestamp': timestamp,
            'country_code': country_code,
            'count': count
        }

        self.execute(sql, parameters)


class ServiceLogVersion(ServiceLogBase):
    """
        CREATE TABLE servicelog_version ("timestamp" TIMESTAMPTZ, version CHARACTER VARYING, "count" INT);

        CREATE INDEX servicelog_version_timestamp ON servicelog_version USING btree("timestamp");
    """

    @inject
    def __init__(self, db_connection_pool: psycopg2.pool.ThreadedConnectionPool):
        super().__init__(db_connection_pool)

        self.table_name = 'servicelog_version'

    def insert(self, timestamp, version, count):
        sql = 'INSERT INTO ' + self.full_table_name + ' ' + \
              '("timestamp", version, "count")' + \
              'VALUES (%(timestamp)s, %(version)s, %(count)s);'

        parameters = {
            'timestamp': timestamp,
            'version': version,
            'count': count
        }

        self.execute(sql, parameters)


class ServiceLogParameters(ServiceLogBase):
    """
        CREATE TABLE servicelog_parameters ("timestamp" TIMESTAMPTZ, region INT, minute_length INT, minute_offset INT, grid_baselength INT, count_threshold INT, "count" INT);

        CREATE INDEX servicelog_parameters_timestamp ON servicelog_parameters USING btree("timestamp");
    """

    @inject
    def __init__(self, db_connection_pool: psycopg2.pool.ThreadedConnectionPool):
        super().__init__(db_connection_pool)

        self.table_name = 'servicelog_parameters'

    def insert(self, timestamp, region, minute_length, minute_offset, grid_baselength, count_threshold, count):
        sql = 'INSERT INTO ' + self.full_table_name + ' ' + \
              '("timestamp", region, minute_length, minute_offset, grid_baselength, count_threshold, "count")' + \
              'VALUES (%(timestamp)s, %(region)s, %(minute_length)s, %(minute_offset)s, %(grid_baselength)s, %(count_threshold)s, %(count)s);'

        parameters = {
            'timestamp': timestamp,
            'region': region,
            'minute_length': minute_length,
            'minute_offset': minute_offset,
            'grid_baselength': grid_baselength,
            'count_threshold': count_threshold,
            'count': count
        }

        self.execute(sql, parameters)
