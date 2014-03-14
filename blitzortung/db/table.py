import math

from injector import inject
import pytz
import shapely.wkb
import shapely.geometry
import shapely.geometry.base
import pandas as pd

import blitzortung.geom
from blitzortung.db.query import Limit, Center, Query, RasterQuery


try:
    import psycopg2
    import psycopg2.pool
    import psycopg2.extras
    import psycopg2.extensions
except ImportError:
    psycopg2 = None

from abc import ABCMeta, abstractmethod


class Base(object):
    """
    abstract base class for database access objects

    creation of database

    as user postgres:

    createuser -i -D -R -S -W -E -P blitzortung
    createdb -E utf8 -O blitzortung blitzortung
    createlang plpgsql blitzortung
    psql -f /usr/share/postgresql/9.1/contrib/postgis-1.5/postgis.sql -d blitzortung
    psql -f /usr/share/postgresql/9.1/contrib/postgis-1.5/spatial_ref_sys.sql -d blitzortung
    (< pg 9.0)
    psql -f /usr/share/postgresql/8.4/contrib/btree_gist.sql blitzortung


    psql blitzortung

    GRANT SELECT ON spatial_ref_sys TO blitzortung;
    GRANT SELECT ON geometry_columns TO blitzortung;
    GRANT INSERT, DELETE ON geometry_columns TO blitzortung;
    (>= pg 9.0)
    CREATE EXTENSION "btree_gist";

    """
    __metaclass__ = ABCMeta

    DefaultTimezone = pytz.UTC

    def __init__(self, db_connection_pool):

        self.db_connection_pool = db_connection_pool

        self.schema_name = None
        self.table_name = None

        while True:
            self.conn = self.db_connection_pool.getconn()
            self.conn.cancel()
            try:
                self.conn.reset()
            except psycopg2.OperationalError:
                print "reconnect to db"
                self.db_connection_pool.putconn(self.conn, close=True)
                continue
            break
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, self.conn)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, self.conn)
        self.conn.set_client_encoding('UTF8')

        self.srid = blitzortung.geom.Geometry.DefaultSrid
        self.tz = None
        self.set_timezone(Base.DefaultTimezone)

        cur = None
        try:
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except psycopg2.DatabaseError, e:
            print e

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
            self.db_connection_pool.putconn(self.conn)
        except (psycopg2.pool.PoolError, AttributeError):
            pass

    def is_connected(self):
        if self.conn:
            return not self.conn.closed
        else:
            return False

    def set_table_name(self, table_name):
        self.table_name = table_name

    def get_table_name(self):
        return self.table_name

    def get_full_table_name(self):
        if self.get_schema_name():
            return '"' + self.get_schema_name() + '"."' + self.get_table_name() + '"'
        else:
            return self.get_table_name()

    def set_schema_name(self, schema_name):
        self.schema_name = schema_name

    def get_schema_name(self):
        return self.schema_name

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
        return utc_time.replace(tzinfo=pytz.UTC).astimezone(self.tz)

    @staticmethod
    def from_timezone_to_bare_utc(time_with_tz):
        return time_with_tz.astimezone(pytz.UTC).replace(tzinfo=None)

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
    def select(self, *args):
        pass

    @abstractmethod
    def create_object_instance(self, result):
        pass

    def create_results(self, cursor, _):
        return [self.create_object_instance(value) for value in cursor.fetchall()]

    def execute(self, sql_statement, parameters=None, build_result=None):
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(sql_statement, parameters)
            if build_result:
                return build_result(cursor, self.create_object_instance)


class Stroke(Base):
    """
    stroke db access class

    database table creation (as db user blitzortung, database blitzortung):

    CREATE TABLE strokes (id bigserial, "timestamp" timestamptz, nanoseconds SMALLINT, geog GEOGRAPHY(Point),
        PRIMARY KEY(id));
    ALTER TABLE strokes ADD COLUMN altitude SMALLINT;
    ALTER TABLE strokes ADD COLUMN region SMALLINT;
    ALTER TABLE strokes ADD COLUMN amplitude REAL;
    ALTER TABLE strokes ADD COLUMN error2d SMALLINT;
    ALTER TABLE strokes ADD COLUMN stationcount SMALLINT;

    CREATE INDEX strokes_timestamp ON strokes USING btree("timestamp");
    CREATE INDEX strokes_region_timestamp ON strokes USING btree(region, "timestamp");
    CREATE INDEX strokes_id_timestamp ON strokes USING btree(id, "timestamp");
    CREATE INDEX strokes_geog ON strokes USING gist(geog);
    CREATE INDEX strokes_timestamp_geog ON strokes USING gist("timestamp", geog);
    CREATE INDEX strokes_id_timestamp_geog ON strokes USING gist(id, "timestamp", geog);

    empty the table with the following commands:

    DELETE FROM strokes;
    ALTER SEQUENCE strokes_id_seq RESTART 1;

    """

    @inject(db_connection_pool=psycopg2.pool.ThreadedConnectionPool, stroke_builder=blitzortung.builder.Stroke)
    def __init__(self, db_connection_pool, stroke_builder):
        super(Stroke, self).__init__(db_connection_pool)

        self.set_table_name('strokes')
        self.stroke_builder = stroke_builder

    def insert(self, stroke, region=1):
        sql = 'INSERT INTO ' + self.get_full_table_name() + \
              ' ("timestamp", nanoseconds, geog, altitude, region, amplitude, error2d, stationcount) ' + \
              'VALUES (%(timestamp)s, %(nanoseconds)s, ST_MakePoint(%(longitude)s, %(latitude)s), ' + \
              '%(altitude)s, %(region)s, %(amplitude)s, %(error2d)s, %(stationcount)s)'

        parameters = {
            'timestamp': stroke.get_timestamp(),
            'nanoseconds': stroke.get_timestamp().nanosecond,
            'longitude': stroke.get_x(),
            'latitude': stroke.get_y(),
            'altitude': stroke.get_altitude(),
            'region': region,
            'amplitude': stroke.get_amplitude(),
            'error2d': stroke.get_lateral_error(),
            'stationcount': stroke.get_station_count()
        }

        self.execute(sql, parameters)

    def get_latest_time(self, region=1):
        sql = 'SELECT "timestamp", nanoseconds FROM ' + self.get_full_table_name() + \
              ' WHERE region=%(region)s' + \
              ' ORDER BY "timestamp" DESC, nanoseconds DESC LIMIT 1'

        def prepare_result(cursor, _):
            if cursor.rowcount == 1:
                result = cursor.fetchone()
                total_nanoseconds = pd.Timestamp(self.fix_timezone(result['timestamp'])).value + result['nanoseconds']
                return pd.Timestamp(total_nanoseconds, tz=self.tz)
            else:
                return None

        return self.execute(sql, {'region': region}, prepare_result)

    def create_object_instance(self, result):
        self.stroke_builder.set_id(result['id'])
        self.stroke_builder.set_timestamp(self.fix_timezone(result['timestamp']), result['nanoseconds'])
        stroke_location = shapely.wkb.loads(result['geog'].decode('hex'))
        self.stroke_builder.set_x(stroke_location.x)
        self.stroke_builder.set_y(stroke_location.y)
        self.stroke_builder.set_altitude(result['altitude'])
        self.stroke_builder.set_amplitude(result['amplitude'])
        self.stroke_builder.set_station_count(result['stationcount'])
        self.stroke_builder.set_lateral_error(result['error2d'])

        return self.stroke_builder.build()

    def select_query(self, args, query=None):
        """ build up query object for select statement """

        if not query:
            query = Query()

        query.set_table_name(self.get_full_table_name())
        query.set_columns(['id', '"timestamp"', 'nanoseconds', 'ST_Transform(geog::geometry, %(srid)s) AS geog',
                           'altitude', 'amplitude', 'error2d', 'stationcount'])
        query.add_parameters({'srid': self.srid})

        query.parse_args(args)
        return query

    def select(self, *args):
        """ build up query """

        query = self.select_query(args)

        return self.select_execute(query)

    def select_raster(self, raster, *args):
        """ build up raster query """

        query = self.select_query(args, RasterQuery(raster))

        return self.select_execute(query)

    def select_histogram(self, minutes, minute_offset=0, binsize=5, region=None, envelope=None):

        query = Query()
        query.set_table_name(self.get_full_table_name())
        query.add_column("-extract(epoch from clock_timestamp() + interval '%(offset)s minutes'"
                         " - \"timestamp\")::int/60/%(binsize)s as interval")
        query.add_column("count(*)")
        query.add_condition("\"timestamp\" >= (select clock_timestamp() + interval '%(offset)s minutes'"
                            " - interval '%(minutes)s minutes')")
        query.add_condition("\"timestamp\" < (select clock_timestamp() + interval '%(offset)s minutes') ")

        if region:
            query.add_condition("region = %(region)s")

        if envelope and envelope.get_env().is_valid:
            query.add_condition('ST_SetSRID(CAST(%(envelope)s AS geometry), %(envelope_srid)s) && geog',
                                {'envelope': shapely.wkb.dumps(envelope.get_env()).encode('hex'),
                                 'envelope_srid': envelope.get_srid()})

        query.add_group_by("interval")
        query.add_order("interval")
        query.add_parameters({'minutes': minutes, 'offset': minute_offset, 'binsize': binsize})

        def prepare_result(cursor, _):
            value_count = minutes / binsize

            result = [0] * value_count

            raw_result = cursor.fetchall()
            for bin_data in raw_result:
                result[bin_data[0] + value_count - 1] = bin_data[1]

            return result

        return self.execute(str(query), query.get_parameters(), prepare_result)

    def select_execute(self, query):
        return self.execute(str(query), query.get_parameters(), query.get_results)


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

    @inject(db_connection_pool=psycopg2.pool.ThreadedConnectionPool, station_builder=blitzortung.builder.Station)
    def __init__(self, db_connection_pool, station_builder):
        super(Station, self).__init__(db_connection_pool)

        self.set_table_name('stations')
        self.station_builder = station_builder

    def insert(self, station, region=1):
        self.execute('INSERT INTO ' + self.get_full_table_name() +
                     ' (number, "user", "name", country, "timestamp", geog, region) ' +
                     'VALUES (%s, %s, %s, %s, %s, ST_MakePoint(%s, %s), %s)',
                     (station.get_number(), station.get_user(), station.get_name(),
                      station.get_country(), station.get_timestamp(), station.get_x(), station.get_y(), region))

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

        return self.execute(sql, {'region': region}, self.create_results)

    def create_object_instance(self, result):
        self.station_builder.set_number(result['number'])
        self.station_builder.set_user(result['user'])
        self.station_builder.set_name(result['name'])
        self.station_builder.set_country(result['country'])
        location = shapely.wkb.loads(result['geog'].decode('hex'))
        self.station_builder.set_x(location.x)
        self.station_builder.set_y(location.y)
        self.station_builder.set_timestamp(self.fix_timezone(result['begin']))

        return self.station_builder.build()


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

    @inject(db_connection_pool=psycopg2.pool.ThreadedConnectionPool,
            station_offline_builder=blitzortung.builder.StationOffline)
    def __init__(self, db_connection_pool, station_offline_builder):
        super(StationOffline, self).__init__(db_connection_pool)

        self.set_table_name('stations_offline')
        self.station_offline_builder = station_offline_builder

    def insert(self, station_offline, region=1):
        self.execute('INSERT INTO ' + self.get_full_table_name() +
                     ' (number, region, begin, "end") ' +
                     'VALUES (%s, %s, %s, %s)',
                     (station_offline.get_number(), region, station_offline.get_begin(), station_offline.get_end()))

    def update(self, station_offline, region=1):
        self.execute('UPDATE ' + self.get_full_table_name() + ' SET "end"=%s WHERE id=%s and region=%s',
                     (station_offline.get_end(), station_offline.get_id(), region))

    def select(self, timestamp=None, region=1):
        sql = '''select id, number, region, begin, "end"
            from stations_offline where "end" is null and region=%s order by number;'''

        return self.execute(sql, (region,), self.create_results)

    def create_object_instance(self, result):
        self.station_offline_builder.set_id(result['id'])
        self.station_offline_builder.set_number(result['number'])
        self.station_offline_builder.set_begin(result['begin'])
        self.station_offline_builder.set_end(result['end'])

        return self.station_offline_builder.build()


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

    @inject(db_connection_pool=psycopg2.pool.ThreadedConnectionPool)
    def __init__(self, db_connection_pool):
        super(Location, self).__init__(db_connection_pool)
        self.set_schema_name('geo')
        self.set_table_name('geonames')
        self.center = None
        self.min_population = None
        self.limit = None
        self.max_distance = None

    def delete_all(self):
        self.execute('DELETE FROM ' + self.get_full_table_name())

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
            self.execute('INSERT INTO ' + self.get_full_table_name() +
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
                if isinstance(arg, Center):
                    self.center = arg
                elif isinstance(arg, Limit):
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
                           self.get_full_table_name() + '''
            WHERE
                feature_class='P'
                AND population >= %(min_population)s
                AND ST_Transform(geog::geometry, %(srid)s) && ST_Expand(c.center, %(max_distance)s)
            ORDER BY distance
            LIMIT %(limit)s'''

            params = {
                'srid': self.get_srid(),
                'center_x': self.center.get_point().x,
                'center_y': self.center.get_point().y,
                'min_population': self.min_population,
                'max_distance': self.max_distance,
                'limit': self.limit
            }

            def build_results(cursor, _):
                locations = []
                if cursor.rowcount > 0:
                    for result in cursor.fetchall():
                        location = {'name': result['name'], 'distance': result['distance'],
                                    'azimuth': result['azimuth']}
                        locations.append(location)

                return locations

            return self.execute(query_string, params, build_results)


class ServiceLog(Base):
    """
        CREATE TABLE servicelog (id BIGSERIAL, "timestamp" TIMESTAMPTZ, geog GEOGRAPHY(Point), version INT,
            address INET, city CHARACTER VARYING, country CHARACTER VARYING, PRIMARY KEY(id));

            CREATE INDEX servicelog_timestamp ON servicelog USING btree("timestamp");
    """

    @inject(db_connection_pool=psycopg2.pool.ThreadedConnectionPool)
    def __init__(self, db_connection_pool):
        super(ServiceLog, self).__init__(db_connection_pool)

        self.set_table_name('servicelog')

    def insert(self, timestamp, ip_address, version, city_name, country_name, longitude, latitude):
        sql = 'INSERT INTO ' + self.get_full_table_name() + ' ' + \
              '("timestamp", geog, address, version, city, country)' + \
              'VALUES (%(timestamp)s, ST_MakePoint(%(longitude)s, %(latitude)s), %(ip_address)s, %(version)s, ' + \
              '%(city_name)s, %(country_name)s);'

        parameters = {
            'timestamp': timestamp,
            'ip_address': ip_address,
            'version': version,
            'longitude': longitude,
            'latitude': latitude,
            'city_name': city_name,
            'country_name': country_name
        }

        self.execute(sql, parameters)

    def get_latest_time(self, region=1):
        sql = 'SELECT "timestamp" FROM ' + self.get_full_table_name() + \
              ' ORDER BY "timestamp" DESC LIMIT 1'

        def prepare_result(cursor, _):
            if cursor.rowcount == 1:
                result = cursor.fetchone()
                return pd.Timestamp(self.fix_timezone(result['timestamp']))
            else:
                return None

        return self.execute(sql, build_result=prepare_result)

    def select(self, args):
        pass

    def create_object_instance(self, result):
        pass

