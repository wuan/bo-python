# -*- coding: utf8 -*-

''' classes for database access '''

import math
from injector import Module, provides, inject, singleton

import datetime
import pytz
import shapely.wkb
import shapely.geometry
import pandas as pd

try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.extensions
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
except ImportError:
    pass

from abc import ABCMeta, abstractmethod

import blitzortung

class BaseInterval(object):

    def __init__(self, start = None, end = None):
        self.start = start
        self.end = end

    def get_start(self):
        return self.start

    def get_end(self):
        return self.end

    def __str__(self):
        return '[' + str(self.start) + ' - ' + str(self.end) + ']'

class IdInterval(BaseInterval):

    def __init__(self, start = None, end = None):
        if start and not isinstance(start, int):
            raise ValueError("start should be an integer value")
        if end and not isinstance(end, int):
            raise ValueError("end should be an integer value")        

        super(IdInterval, self).__init__(start, end)

class TimeInterval(BaseInterval):

    def __init__(self, start = None, end = None):
        if start and not isinstance(start, datetime.datetime):
            raise ValueError("start should be a datetime value")
        if end and not isinstance(end, datetime.datetime):
            raise ValueError("end should be a datetime value")        

        super(TimeInterval, self).__init__(start, end)


class Query(object):
    '''
    simple class for building of complex queries
    '''

    def __init__(self):
        self.sql = ''
        self.conditions = []
        self.parameters = {}
        self.table_name = None
        self.columns = None
        self.limit = None
        self.order = []

    def set_table_name(self, table_name):
        self.table_name = table_name

    def set_columns(self, columns):
        self.columns = columns

    def add_order(self, order):
        self.order.append(order)

    def set_limit(self, limit):
        if self.limit != None:
            raise RuntimeError("overriding Query.limit")
        self.limit = limit

    def add_condition(self, condition, parameters = None):
        self.conditions.append(condition)
        if parameters != None:
            self.parameters.update(parameters)

    def add_parameters(self, parameters):
        self.parameters.update(parameters)

    def __str__(self):
        sql = 'SELECT '

        if self.columns:
            for index, column in enumerate(self.columns):
                if index != 0:
                    sql += ', '
                sql += column
            sql += ' '

        sql += 'FROM ' + self.table_name + ' '

        for index, condition in enumerate(self.conditions):
            if index == 0:
                sql += 'WHERE '
            else:
                sql += 'AND '
            sql += condition + ' '

        if len(self.order) > 0:
            sql += 'ORDER BY '
            for index, order in enumerate(self.order):
                if index != 0:
                    sql += ', '
                sql += order.get_column()
                if order.is_desc():
                    sql += ' DESC'
            sql += ' '

        if self.limit:
            sql += 'LIMIT ' + str(self.limit.get_number()) + ' '

        return sql.strip()

    def get_parameters(self):
        return self.parameters

    def parse_args(self, args):
        for arg in args:
            if arg:
                if isinstance(arg, TimeInterval):

                    if arg.get_start() != None:
                        self.add_condition('timestamp >= %(starttime)s', {'starttime': arg.get_start()})
#self.add_condition('timestamp >= :starttime', {'starttime': arg.get_start().astimezone(pytz.UTC).replace(tzinfo=None)})

                    if arg.get_end() != None:
                        self.add_condition('timestamp < %(endtime)s', {'endtime': arg.get_end()})
#self.add_condition('timestamp < :endtime', {'endtime': arg.get_end().astimezone(pytz.UTC).replace(tzinfo=None)})

                elif isinstance(arg, IdInterval):

                    if arg.get_start() != None:
                        self.add_condition('id >= %(startid)s', {'startid': arg.get_start()})

                    if arg.get_end() != None:
                        self.add_condition('id < %(endid)s', {'endid': arg.get_end()})

                elif isinstance(arg, shapely.geometry.base.BaseGeometry):

                    if arg.is_valid:

                        self.add_condition('ST_SetSRID(CAST(%(envelope)s AS geometry), %(srid)s) && geog', {'envelope': shapely.wkb.dumps(arg.envelope).encode('hex')})
#self.add_condition('ST_SetSRID(CAST(:envelope AS geometry), :srid) && Transform(geog, :srid)', {'envelope': shapely.wkb.dumps(arg.envelope).encode('hex')})

                        if not arg.equals(arg.envelope):
                            self.add_condition('Intersects(ST_SetSRID(CAST(%(geometry)s AS geometry), %(srid)s), st_transform(geog, %(srid)s))', {'geometry': shapely.wkb.dumps(arg).encode('hex')})
#self.add_condition('Intersects(ST_SetSRID(CAST(:geometry AS geometry), :srid), Transform(geog, :srid))', {'geometry': shapely.wkb.dumps(arg).encode('hex')})

                    else:
                        raise ValueError("invalid geometry in db.Stroke.select()")

                elif isinstance(arg, Order):
                    self.add_order(arg)

                elif isinstance(arg, Limit):
                    self.set_limit(arg)

                else:
                    print 'WARNING: ' + __name__ + ' unhandled object ' + str(type(arg))

    def get_results(self, cur, db):

        resulting_strokes = []
        if cur.rowcount > 0:
            for result in cur.fetchall():
                resulting_strokes.append(db.create(result))

        return resulting_strokes

class RasterQuery(Query):

    def __init__(self, raster):
        super(RasterQuery, self).__init__()

        self.raster = raster

        env = self.raster.get_env()

        if env.is_valid:
            self.add_condition('ST_SetSRID(CAST(%(envelope)s AS geometry), %(srid)s) && geog', {'envelope': shapely.wkb.dumps(env).encode('hex')})
#self.add_condition('ST_SetSRID(CAST(:envelope AS geometry), :srid) && Transform(geog, :srid)', {'envelope': shapely.wkb.dumps(env).encode('hex')})
        else:
            raise ValueError("invalid Raster geometry in db.Stroke.select()")

    def __str__(self):
        sql = 'SELECT '

        sql += 'TRUNC((ST_X(ST_TRANSFORM(geog, %(srid)s)) - ' + str(self.raster.get_x_min()) + ') /' + str(self.raster.get_x_div()) + ') AS rx, '
        sql += 'TRUNC((ST_Y(ST_TRANSFORM(geog, %(srid)s)) - ' + str(self.raster.get_y_min()) + ') /' + str(self.raster.get_y_div()) + ') AS ry, '
        sql += 'count(*) AS count, max(timestamp) as timestamp FROM ('

        sql += Query.__str__(self)

        sql += ') AS ' + self.table_name + ' GROUP BY rx, ry'

        return sql

    def get_results(self, cur, db):

        if cur.rowcount > 0:
            for result in cur.fetchall():
                self.raster.set(result['rx'], result['ry'], blitzortung.geom.RasterElement(result['count'], result['timestamp']))
        return self.raster

class Order(object):
    '''
    definition for query search order
    '''

    def __init__(self, column, desc = False):
        self.column = column
        self.desc = desc

    def get_column(self):
        return self.column

    def is_desc(self):
        return self.desc


class Limit(object):
    '''
    definition of query result limit
    '''

    def __init__(self, limit):
        self.limit = limit

    def get_number(self):
        return self.limit


class Center(object):
    '''
    definition of query center point
    '''

    def __init__(self, center):
        self.center = center

    def get_point(self):
        return self.center

class Connection(Module):
    
#    @singleton
    @provides(psycopg2._psycopg.connection)
    @inject(config=blitzortung.config.Config)
    def provide_psycopg2_connection(self, config):
        return psycopg2.connect(config.get_db_connection_string())


class Base(object):
    '''
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

    '''
    __metaclass__ = ABCMeta

    DefaultTimezone = pytz.UTC

    def __init__(self, db_connection):
        self.schema_name = None
        self.table_name = None
        self.conn = db_connection
	self.conn.set_client_encoding('UTF8')

        self.set_srid(blitzortung.geom.Geometry.DefaultSrid)
        self.set_timezone(Base.DefaultTimezone)

        try:
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except psycopg2.DatabaseError, e:
            print e

            if self.conn != None:
                try:
                    self.conn.close()
                except NameError:
                    pass
	finally:
	    cur.close()

        #if not self.has_table('geometry_columns'):
        #    self.execute('SELECT InitSpatialMetadata()')
        #    self.execute("INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ref_sys_name, proj4text) VALUES (4326, 'epsg', 4326, 'WGS 84', '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')")

#    def has_table(self, table_name):
#        result = self.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'" % table_name)
#        return result.fetchone() != None

    def is_connected(self):
        if self.conn != None:
            return not self.conn.closed
        else:
            return False

    def set_table_name(self, table_name):
        self.table_name = table_name

    def get_table_name(self):
        return self.table_name

    def get_full_table_name(self):
        if self.get_schema_name() != None:
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
	cur = self.conn.cursor()
	cur.execute('SET TIME ZONE \'%s\'' %(str(self.tz)))

    def fix_timezone(self, timestamp):
        return timestamp.astimezone(self.tz) if timestamp else None

    def from_bare_utc_to_timezone(self, utc_time):
        return utc_time.replace(tzinfo=pytz.UTC).astimezone(self.tz)

    def from_timezone_to_bare_utc(self, time_with_tz):
        return time_with_tz.astimezone(pytz.UTC).replace(tzinfo=None)

    def commit(self):
        ''' commit pending database transaction '''
        self.conn.commit()

    def rollback(self):
        ''' rollback pending database transaction '''
        self.conn.rollback()

    @abstractmethod
    def insert(self, object_to_insert):
        pass

    @abstractmethod
    def select(self, args):
        pass

    def execute(self, sql_string, parameters=None):
	cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute(sql_string, parameters)
	return cur

@singleton
class Stroke(Base):
    '''
    stroke db access class

    database table creation (as db user blitzortung, database blitzortung): 

    CREATE TABLE strokes (id bigserial, timestamp timestamptz, nanoseconds SMALLINT, geog GEOGRAPHY(Point), PRIMARY KEY(id));
    ALTER TABLE strokes ADD COLUMN region SMALLINT;
    ALTER TABLE strokes ADD COLUMN amplitude REAL;
    ALTER TABLE strokes ADD COLUMN error2d SMALLINT;
    ALTER TABLE strokes ADD COLUMN type SMALLINT;
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

    '''
    
    @inject(db_connection=psycopg2._psycopg.connection)
    def __init__(self, db_connection):
        super(Stroke, self).__init__(db_connection)

        self.set_table_name('strokes')

#        if not self.has_table(self.get_table_name()):
#            self.execute("CREATE TABLE strokes (id INTEGER PRIMARY KEY, timestamp timestamp, nanoseconds INTEGER)")
#            self.execute("SELECT AddGeometryColumn('strokes','geog',4326,'POINT',2)")
#            self.execute("ALTER TABLE strokes ADD COLUMN amplitude REAL")
#            self.execute("ALTER TABLE strokes ADD COLUMN error2d INTEGER")
#            self.execute("ALTER TABLE strokes ADD COLUMN type INTEGER")
#            self.execute("ALTER TABLE strokes ADD COLUMN stationcount INTEGER")
#            self.execute("ALTER TABLE strokes ADD COLUMN detected BOOLEAN")

    def insert(self, stroke, region=1):
        sql = 'INSERT INTO ' + self.get_full_table_name() + \
            ' ("timestamp", nanoseconds, geog, region, amplitude, error2d, type, stationcount) ' + \
            'VALUES (%(timestamp)s, %(nanoseconds)s, ST_MakePoint(%(longitude)s, %(latitude)s), %(region)s, %(amplitude)s, %(error2d)s, %(type)s, %(stationcount)s)'

        parameters = {
            'timestamp': stroke.get_timestamp(),
            'nanoseconds': stroke.get_timestamp().nanosecond,
            'longitude': stroke.get_x(),
            'latitude': stroke.get_y(),
            'region': region,
            'amplitude': stroke.get_amplitude(),
            'error2d': stroke.get_lateral_error(),
            'type': stroke.get_type(),
            'stationcount': stroke.get_station_count()
        }

        self.execute(sql, parameters)

    def get_latest_time(self, region=1):
        sql = 'SELECT timestamp FROM ' + self.get_full_table_name() + \
            ' WHERE region=%(region)s' + \
            ' ORDER BY timestamp DESC LIMIT 1'
        cur = self.execute(sql, {'region': region})
        if cur.rowcount == 1:
            result = cur.fetchone()
            return pd.Timestamp(self.fix_timezone(result['timestamp']))
        else:
            return None

    def create(self, result):
        stroke_builder = blitzortung.builder.Stroke()

        stroke_builder.set_id(result['id'])
        stroke_builder.set_timestamp(self.fix_timezone(result['timestamp']), result['nanoseconds'])
        location = shapely.wkb.loads(result['geog'].decode('hex'))
        stroke_builder.set_x(location.x)
        stroke_builder.set_y(location.y)
        stroke_builder.set_amplitude(result['amplitude'])
        stroke_builder.set_type(result['type'])
        stroke_builder.set_station_count(result['stationcount'])
        stroke_builder.set_lateral_error(result['error2d'])

        return stroke_builder.build()

    def select_query(self, args, query = None):
        ' build up query object for select statement '

        if not query:
            query = Query()

        query.set_table_name(self.get_full_table_name())
        query.set_columns(['id', '"timestamp"', 'nanoseconds', 'st_transform(geog::geometry, %(srid)s) AS geog', 'amplitude', 'type', 'error2d', 'stationcount'])
        query.add_parameters({'srid': self.srid})

#query.add_condition('geog IS NOT NULL')
        query.parse_args(args)
        return query

    def select(self, *args):
        ' build up query '

        query = self.select_query(args)

        return self.select_execute(query)

    def select_raster(self, raster, *args):
        ' build up query '

        query = self.select_query(args, RasterQuery(raster))

        return self.select_execute(query)

    def select_histogram(self, minutes, region=1, binsize=5):
        query = "select -extract(epoch from clock_timestamp() - timestamp)::int/60/%(binsize)s as interval, count(*) from strokes where timestamp > clock_timestamp() - interval '%(minutes)s minutes' and region = %(region)s group by interval order by interval;"

        cur = self.execute(query, {'minutes': minutes, 'binsize':binsize, 'region':region})

        value_count = minutes/binsize

        result = [0] * value_count

        raw_result = cur.fetchall()
        for bin_data in raw_result:
	    result[bin_data[0] + value_count - 1] = bin_data[1]

        return result

    def select_execute(self, query):
        cur = self.execute(str(query), query.get_parameters())

        return query.get_results(cur, self)
    
def stroke():
    from __init__ import INJECTOR
    return INJECTOR.get(Stroke)

@singleton
class Station(Base):
    '''

    database table creation (as db user blitzortung, database blitzortung): 

    CREATE TABLE stations (id bigserial, number int, geog GEOGRAPHY(Point), PRIMARY KEY(id));
    ALTER TABLE stations ADD COLUMN short_name CHARACTER VARYING;
    ALTER TABLE stations ADD COLUMN name CHARACTER VARYING;
    ALTER TABLE stations ADD COLUMN location_name CHARACTER VARYING;
    ALTER TABLE stations ADD COLUMN country CHARACTER VARYING;
    ALTER TABLE stations ADD COLUMN timestamp TIMESTAMPTZ;

    CREATE INDEX stations_timestamp ON stations USING btree("timestamp");
    CREATE INDEX stations_number_timestamp ON stations USING btree(number, "timestamp");
    CREATE INDEX stations_geog ON stations USING gist(geog);

    empty the table with the following commands:

    DELETE FROM stations;
    ALTER SEQUENCE stations_id_seq RESTART 1;
    '''

    @inject(db_connection=psycopg2._psycopg.connection)
    def __init__(self, db_connection):
        super(Station, self).__init__(db_connection)

        self.set_table_name('stations')

    def insert(self, station):
        self.execute('INSERT INTO ' + self.get_full_table_name() + \
                     ' (number, short_name, "name", location_name, country, timestamp, geog) ' + \
                     'VALUES (%s, %s, %s, %s, %s, %s, ST_MakePoint(%s, %s))',
                     (station.get_number(), station.get_short_name(), station.get_name(), station.get_location_name(), station.get_country(), station.get_timestamp(), station.get_x(), station.get_y()))

    def select(self, timestamp=None):
        sql = ''' select
        o.begin, s.number, s.short_name, s.name, s.location_name, s.country, s.geog
	from stations as s
	inner join 
	   (select b.number, max(b.timestamp) as timestamp
	    from stations as b
	    group by number
            order by number) as c
	on s.number = c.number and s.timestamp = c.timestamp
	left join stations_offline as o
	on o.number = s.number and o."end" is null
	order by s.number'''
        cur = self.execute(sql)

        resulting_stations = []
        if cur.rowcount > 0:
            for result in cur.fetchall():
                resulting_stations.append(self.create(result))

        return resulting_stations    

    def create(self, result):
        station_builder = blitzortung.builder.Station()

        station_builder.set_number(result['number'])
        station_builder.set_short_name(result['short_name'])
        station_builder.set_name(result['name'])
        station_builder.set_location_name(result['location_name'])
        station_builder.set_country(result['country'])
        location = shapely.wkb.loads(result['geog'].decode('hex'))
        station_builder.set_x(location.x)
        station_builder.set_y(location.y)
        station_builder.set_timestamp(self.fix_timezone(result['begin']))

        return station_builder.build()  

def station():
    from __init__ import INJECTOR
    return INJECTOR.get(Station)

@singleton
class StationOffline(Base):
    '''

    database table creation (as db user blitzortung, database blitzortung): 

    CREATE TABLE stations_offline (id bigserial, number int, PRIMARY KEY(id));
    ALTER TABLE stations_offline ADD COLUMN begin TIMESTAMPTZ;
    ALTER TABLE stations_offline ADD COLUMN "end" TIMESTAMPTZ;

    CREATE INDEX stations_offline_begin ON stations_offline USING btree(begin);
    CREATE INDEX stations_offline_end ON stations_offline USING btree("end");
    CREATE INDEX stations_offline_end_number ON stations_offline USING btree("end", number);
    CREATE INDEX stations_offline_begin_end ON stations_offline USING btree(begin, "end");

    empty the table with the following commands:

    DELETE FROM stations_offline;
    ALTER SEQUENCE stations_offline_id_seq RESTART 1;
    '''

    @inject(db_connection=psycopg2._psycopg.connection)
    def __init__(self, db_connection):
        super(StationOffline, self).__init__(db_connection)

        self.set_table_name('stations_offline')

    def insert(self, station_offline):
        self.execute('INSERT INTO ' + self.get_full_table_name() + \
                     ' (number, begin, "end") ' + \
                     'VALUES (%s, %s, %s)',
                     (station_offline.get_number(), station_offline.get_begin(), station_offline.get_end()))

    def update(self, station_offline):
        self.execute('UPDATE ' + self.get_full_table_name() + ' SET "end"=%s WHERE id=%s',
                     (station_offline.get_end(), station_offline.get_id()))

    def select(self, timestamp=None):
        sql = '''select id, number, begin, "end" from stations_offline where "end" is null order by number;'''
        cur = self.execute(sql)

        resulting_stations = []
        if cur.rowcount > 0:
            for result in cur.fetchall():
                resulting_stations.append(self.create(result))

        return resulting_stations    

    def create(self, result):
        stationOfflineBuilder = blitzortung.builder.StationOffline()

        stationOfflineBuilder.set_id(result['id'])
        stationOfflineBuilder.set_number(result['number'])
        stationOfflineBuilder.set_begin(result['begin'])
        stationOfflineBuilder.set_end(result['end'])

        return stationOfflineBuilder.build() 
    
def station_offline():
    from __init__ import INJECTOR
    return INJECTOR.get(StationOffline)  

@singleton
class Location(Base):
    '''
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

    '''
    
    @inject(db_connection=psycopg2._psycopg.connection)
    def __init__(self, db_connection):
        super(Location, self).__init__(db_connection)
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
        admin_code_3 = fields[12]
        admin_code_4 = fields[13]
        population = int(fields[14])
        if fields[15] != '':
            elevation = int(fields[15])
        else:
            elevation = -1

        name = name.replace("'", "''")

        classification = self.size_class(population)

        if classification is not None:
            self.execute('INSERT INTO ' + self.get_full_table_name() + '''
	(geog, name, class, feature_class, feature_code, country_code, admin_code_1, admin_code_2, population, elevation)
      VALUES(
	ST_GeomFromText('POINT(%s %s)', 4326), %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                                                                                     (x, y, name, classification, feature_class, feature_code, country_code, admin_code_1, admin_code_2, population, elevation))

    def size_class(self, n):
        if n < 1:
            return None
        base = math.floor(math.log(n)/math.log(10)) - 1
        relative = n / math.pow(10, base)
        order = min(2, math.floor(relative/25))
        if base < 0:
            base = 0
        return min(15, base * 3 + order)

    def select(self, *args):
        self.center = None
        self.min_population = 1000
        self.max_distance = 10000
        self.limit = 10

        for arg in args:
            if arg != None:
                if isinstance(arg, Center):
                    self.center = arg
                elif isinstance(arg, Limit):
                    self.limit = arg

        if self.is_connected():
            queryString = '''SELECT
	  name,
	  country_code,
	  admin_code_1,
	  admin_code_2,
	  feature_class,
	  feature_code,
	  elevation,
	  ST_Transform(geog, %(srid)s) AS geog,
	  population,
	  ST_Distance_Sphere(geog, c.center) AS distance,
	  ST_Azimuth(geog, c.center) AS azimuth
	FROM
	  (SELECT ST_SetSRID(ST_MakePoint(%(center_x)s, %(center_y)s), %(srid)s) as center ) as c,''' + \
                                                                                                      self.get_full_table_name() + '''
	WHERE
	  feature_class='P'
	  AND population >= %(min_population)s
	  AND ST_Transform(geog, %(srid)s) && st_expand(c.center, %(max_distance)s) order by distance limit %(limit)s'''

            params = {
                'srid': self.get_srid(),
                'center_x': self.center.get_point().x,
                'center_y': self.center.get_point().y,
                'min_population': self.min_population,
                'max_distance': self.max_distance,
                'limit': self.limit
            }

            cur = self.execute(queryString, params)

            locations = []
            if cur.rowcount > 0:
                for result in cur.fetchall():
                    location = {}
                    location['name'] = result['name']
                    location['distance'] = result['distance']
                    location['azimuth'] = result['azimuth']
                    locations.append(location)

            return locations
        
def location():
    from __init__ import INJECTOR
    return INJECTOR.get(Location)        
