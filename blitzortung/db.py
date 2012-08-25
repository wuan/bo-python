# -*- coding: utf8 -*-

import math

import datetime
import pytz
import shapely.wkb
import shapely.geometry
import pandas as pd

try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.extensions
except ImportError:
    pass

import builder
import geom

from abc import ABCMeta, abstractmethod

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

                        self.add_condition('SetSRID(CAST(%(envelope)s AS geometry), %(srid)s) && st_transform(the_geom, %(srid)s)', {'envelope': shapely.wkb.dumps(arg.envelope).encode('hex')})
#self.add_condition('SetSRID(CAST(:envelope AS geometry), :srid) && Transform(the_geom, :srid)', {'envelope': shapely.wkb.dumps(arg.envelope).encode('hex')})

                        if not arg.equals(arg.envelope):
                            self.add_condition('Intersects(SetSRID(CAST(%(geometry)s AS geometry), %(srid)s), st_transform(the_geom, %(srid)s))', {'geometry': shapely.wkb.dumps(arg).encode('hex')})
#self.add_condition('Intersects(SetSRID(CAST(:geometry AS geometry), :srid), Transform(the_geom, :srid))', {'geometry': shapely.wkb.dumps(arg).encode('hex')})

                    else:
                        raise ValueError("invalid geometry in db.Stroke.select()")

                elif isinstance(arg, Order):
                    self.add_order(arg)

                elif isinstance(arg, Limit):
                    self.set_limit(arg)

                else:
                    print 'WARNING: ' + __name__ + ' unhandled object ' + str(type(arg))

    def get_results(self, db):

        resulting_strokes = []
        if db.cur.rowcount > 0:
            for result in db.cur.fetchall():
                resulting_strokes.append(db.create(result))

        return resulting_strokes

class RasterQuery(Query):

    def __init__(self, raster):
        super(RasterQuery, self).__init__()

        self.raster = raster

        env = self.raster.getEnv()

        if env.is_valid:
            self.add_condition('SetSRID(CAST(%(envelope)s AS geometry), %(srid)s) && st_transform(the_geom, %(srid)s)', {'envelope': shapely.wkb.dumps(env).encode('hex')})
#self.add_condition('SetSRID(CAST(:envelope AS geometry), :srid) && Transform(the_geom, :srid)', {'envelope': shapely.wkb.dumps(env).encode('hex')})
        else:
            raise ValueError("invalid Raster geometry in db.Stroke.select()")

    def __str__(self):
        sql = 'SELECT '

        sql += 'TRUNC((ST_X(ST_TRANSFORM(the_geom, %(srid)s)) - ' + str(self.raster.getXMin()) + ') /' + str(self.raster.getXDiv()) + ') AS rx, '
        sql += 'TRUNC((ST_Y(ST_TRANSFORM(the_geom, %(srid)s)) - ' + str(self.raster.getYMin()) + ') /' + str(self.raster.getYDiv()) + ') AS ry, '
        sql += 'count(*) AS count, max(timestamp) as timestamp FROM ('

        sql += Query.__str__(self)

        sql += ') AS ' + self.table_name + ' GROUP BY rx, ry'

        return sql

    def get_results(self, db):

        if db.cur.rowcount > 0:
            for result in db.cur.fetchall():
                self.raster.set(result['rx'], result['ry'], geom.RasterElement(result['count'], result['timestamp']))
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


class Base(object):
    '''
    abstract base class for database access objects

    creation of database 

    as user postgres:

    createuser -i -D -R -S -W blitzortung
    createdb -E utf8 -O blitzortung blitzortung
    createlang plpgsql blitzortung
    psql -f /usr/share/postgresql/8.4/contrib/postgis-1.5/postgis.sql -d blitzortung
    psql -f /usr/share/postgresql/8.4/contrib/postgis-1.5/spatial_ref_sys.sql -d blitzortung  
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

    def __init__(self):
        '''
        create PostgreSQL db access object
        '''

        connection = "host='localhost' dbname='blitzortung' user='blitzortung' password='blitzortung'"
        self.schema_name = None
        self.table_name = None
        self.cur = None
        self.conn = None

        self.srid = geom.Geometry.DefaultSrid
        self.tz = Base.DefaultTimezone

#            self.db_file = "/tmp/blitzortung.sqlite"
#            self.conn = db.connect(self.db_file, detect_types=db.PARSE_DECLTYPES)
#            self.conn.enable_load_extension(True)
#            self.conn.execute('SELECT load_extension("libspatialite.so");')
#            self.conn.row_factory = db.Row
        try:
            self.conn = psycopg2.connect(connection)
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, self.cur)
        except psycopg2.DatabaseError, e:
            print e

            if self.cur != None:
                try:
                    self.cur.close()
                except NameError:
                    pass

            if self.conn != None:
                try:
                    self.conn.close()
                except NameError:
                    pass

        #if not self.has_table('geometry_columns'):
        #    self.cur.execute('SELECT InitSpatialMetadata()')
        #    self.cur.execute("INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ref_sys_name, proj4text) VALUES (4326, 'epsg', 4326, 'WGS 84', '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')")

#    def has_table(self, table_name):
#        result = self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'" % table_name)
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

    def update_timezone(self, timestamp):
        if timestamp:
	    return timestamp.replace(tzinfo=pytz.UTC).astimezone(self.tz)

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


class Stroke(Base):
    '''
    stroke db access class

    database table creation (as db user blitzortung, database blitzortung): 

    CREATE TABLE strokes (id bigserial, timestamp timestamptz, nanoseconds SMALLINT, PRIMARY KEY(id));
    SELECT AddGeometryColumn('public','strokes','the_geom','4326','POINT',2);

    ALTER TABLE strokes ADD COLUMN region SMALLINT;
    ALTER TABLE strokes ADD COLUMN amplitude REAL;
    ALTER TABLE strokes ADD COLUMN error2d SMALLINT;
    ALTER TABLE strokes ADD COLUMN type SMALLINT;
    ALTER TABLE strokes ADD COLUMN stationcount SMALLINT;

    CREATE INDEX strokes_timestamp ON strokes USING btree("timestamp");
    CREATE INDEX strokes_region_timestamp ON strokes USING btree(region, "timestamp");
    CREATE INDEX strokes_id_timestamp ON strokes USING btree(id, "timestamp");
    CREATE INDEX strokes_geom ON strokes USING gist(the_geom);
    CREATE INDEX strokes_timestamp_geom ON strokes USING gist("timestamp", the_geom);
    CREATE INDEX strokes_id_timestamp_geom ON strokes USING gist(id, "timestamp", the_geom);

    empty the table with the following commands:

    DELETE FROM strokes;
    ALTER SEQUENCE strokes_id_seq RESTART 1;

    '''

    def __init__(self):
        super(Stroke, self).__init__()

        self.set_table_name('strokes')

        self.update_timezone()

#        if not self.has_table(self.get_table_name()):
#            self.cur.execute("CREATE TABLE strokes (id INTEGER PRIMARY KEY, timestamp timestamp, nanoseconds INTEGER)")
#            self.cur.execute("SELECT AddGeometryColumn('strokes','the_geom',4326,'POINT',2)")
#            self.cur.execute("ALTER TABLE strokes ADD COLUMN amplitude REAL")
#            self.cur.execute("ALTER TABLE strokes ADD COLUMN error2d INTEGER")
#            self.cur.execute("ALTER TABLE strokes ADD COLUMN type INTEGER")
#            self.cur.execute("ALTER TABLE strokes ADD COLUMN stationcount INTEGER")
#            self.cur.execute("ALTER TABLE strokes ADD COLUMN detected BOOLEAN")

    def insert(self, stroke, region=1):
        sql = 'INSERT INTO ' + self.get_full_table_name() + \
            ' ("timestamp", nanoseconds, the_geom, region, amplitude, error2d, type, stationcount) ' + \
            'VALUES (%(timestamp)s, %(nanoseconds)s, st_setsrid(makepoint(%(longitude)s, %(latitude)s), 4326), %(region)s, %(amplitude)s, %(error2d)s, %(type)s, %(stationcount)s)'

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

        self.cur.execute(sql, parameters)

    def get_latest_time(self, region=1):
        sql = 'SELECT timestamp FROM ' + self.get_full_table_name() + \
            ' WHERE region=%(region)s' + \
            ' ORDER BY timestamp DESC LIMIT 1'
        self.cur.execute(sql, {'region': region})
        if self.cur.rowcount == 1:
            result = self.cur.fetchone()
            return pd.Timestamp(self.update_timezone(result['timestamp']))
        else:
            return None

    def create(self, result):
        stroke_builder = builder.Stroke()

        stroke_builder.set_id(result['id'])
        stroke_builder.set_timestamp(self.update_timezone(result['timestamp']), result['nanoseconds'])
        location = shapely.wkb.loads(result['the_geom'].decode('hex'))
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
        query.set_columns(['id', '"timestamp"', 'nanoseconds', 'st_transform(the_geom, %(srid)s) AS the_geom', 'amplitude', 'type', 'error2d', 'stationcount'])
        query.add_parameters({'srid': self.srid})

#query.add_condition('the_geom IS NOT NULL')
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

    def select_execute(self, query):
        self.cur.execute(str(query), query.get_parameters())

        return query.get_results(self)

    def update_timezone(self):
        self.cur.execute('SET TIME ZONE \'%s\'' %(str(self.tz)))

class Station(Base):
    '''

    database table creation (as db user blitzortung, database blitzortung): 

    CREATE TABLE stations (id bigserial, number int, PRIMARY KEY(id));
    SELECT AddGeometryColumn('public','stations','the_geom','4326','POINT',2);

    ALTER TABLE stations ADD COLUMN short_name CHARACTER VARYING;
    ALTER TABLE stations ADD COLUMN name CHARACTER VARYING;
    ALTER TABLE stations ADD COLUMN location_name CHARACTER VARYING;
    ALTER TABLE stations ADD COLUMN country CHARACTER VARYING;
    ALTER TABLE stations ADD COLUMN timestamp TIMESTAMPTZ;

    CREATE INDEX stations_timestamp ON stations USING btree("timestamp");
    CREATE INDEX stations_number_timestamp ON stations USING btree(number, "timestamp");
    CREATE INDEX stations_geom ON stations USING gist(the_geom);

    empty the table with the following commands:

    DELETE FROM stations;
    ALTER SEQUENCE stations_id_seq RESTART 1;
    '''

    def __init__(self):
        super(Station, self).__init__()

        self.set_table_name('stations')

    def insert(self, station):
        self.cur.execute('INSERT INTO ' + self.get_full_table_name() + \
                         ' (number, short_name, "name", location_name, country, timestamp, the_geom) ' + \
                         'VALUES (%s, %s, %s, %s, %s, %s, st_setsrid(makepoint(%s, %s), 4326))',
                         (station.get_number(), station.get_short_name(), station.get_name(), station.get_location_name(), station.get_country(), station.get_timestamp(), station.get_x(), station.get_y()))

    def select(self, timestamp=None):
        ' set timezone for query '
        self.cur.execute('SET TIME ZONE \'%s\'' %(str(self.tz)))

        sql = ''' select
        o.begin, s.number, s.short_name, s.name, s.location_name, s.country, s.the_geom
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
        self.cur.execute(sql)

        resulting_stations = []
        if self.cur.rowcount > 0:
            for result in self.cur.fetchall():
                resulting_stations.append(self.create(result))

        return resulting_stations    

    def create(self, result):
        station_builder = builder.Station()

        station_builder.set_number(result['number'])
        station_builder.set_short_name(result['short_name'])
        station_builder.set_name(result['name'])
        station_builder.set_location_name(result['location_name'])
        station_builder.set_country(result['country'])
        location = shapely.wkb.loads(result['the_geom'].decode('hex'))
        station_builder.set_x(location.x)
        station_builder.set_y(location.y)
        station_builder.set_timestamp(self.update_timezone(result['begin']))

        return station_builder.build()  

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

    def __init__(self):
        super(StationOffline, self).__init__()

        self.set_table_name('stations_offline')

    def insert(self, station_offline):
        self.cur.execute('INSERT INTO ' + self.get_full_table_name() + \
                         ' (number, begin, "end") ' + \
                         'VALUES (%s, %s, %s)',
                         (station_offline.get_number(), station_offline.get_begin(), station_offline.get_end()))

    def update(self, station_offline):
        self.cur.execute('UPDATE ' + self.get_full_table_name() + ' SET "end"=%s WHERE id=%s',
                         (station_offline.get_end(), station_offline.get_id()))

    def select(self, timestamp=None):
        ' set timezone for query '
        self.cur.execute('SET TIME ZONE \'%s\'' %(str(self.tz)))

        sql = '''select id, number, begin, "end" from stations_offline where "end" is null order by number;'''
        self.cur.execute(sql)

        resulting_stations = []
        if self.cur.rowcount > 0:
            for result in self.cur.fetchall():
                resulting_stations.append(self.create(result))

        return resulting_stations    

    def create(self, result):
        stationOfflineBuilder = builder.StationOffline()

        stationOfflineBuilder.set_id(result['id'])
        stationOfflineBuilder.set_number(result['number'])
        stationOfflineBuilder.set_begin(result['begin'])
        stationOfflineBuilder.set_end(result['end'])

        return stationOfflineBuilder.build()  

class Location(Base):
    '''
    geonames db access class

    CREATE SCHEMA geo;

    CREATE TABLE geo.geonames (id bigserial, "name" character varying, PRIMARY KEY(id));
    SELECT AddGeometryColumn('geo','geonames','the_geom','4326','POINT',2);

    ALTER TABLE geo.geonames ADD COLUMN "class" INTEGER;
    ALTER TABLE geo.geonames ADD COLUMN feature_class CHARACTER(1);
    ALTER TABLE geo.geonames ADD COLUMN feature_code VARCHAR;
    ALTER TABLE geo.geonames ADD COLUMN country_code VARCHAR;
    ALTER TABLE geo.geonames ADD COLUMN admin_code_1 VARCHAR;
    ALTER TABLE geo.geonames ADD COLUMN admin_code_2 VARCHAR;
    ALTER TABLE geo.geonames ADD COLUMN population INTEGER;
    ALTER TABLE geo.geonames ADD COLUMN elevation SMALLINT;

    CREATE INDEX geonames_geom ON geo.geonames USING gist(the_geom);

    '''

    def __init__(self):
        super(Location, self).__init__()
        self.set_schema_name('geo')
        self.set_table_name('geonames')
        self.center = None
        self.min_population = None
        self.limit = None
        self.max_distance = None

    def delete_all(self):
        self.cur.execute('DELETE FROM ' + self.get_full_table_name())

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
            self.cur.execute('INSERT INTO ' + self.get_full_table_name() + '''
	(the_geom, name, class, feature_class, feature_code, country_code, admin_code_1, admin_code_2, population, elevation)
      VALUES(
	GeomFromText('POINT(%s %s)', 4326), %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
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
	  st_transform(the_geom, %(srid)s) AS the_geom,
	  population,
	  distance_sphere(the_geom, c.center) AS distance,
	  st_azimuth(the_geom, c.center) AS azimuth
	FROM
	  (SELECT SetSRID(MakePoint(%(center_x)s, %(center_y)s), %(srid)s) as center ) as c,''' + \
                                                                                                self.get_full_table_name() + '''
	WHERE
	  feature_class='P'
	  AND population >= %(min_population)s
	  AND st_transform(the_geom, %(srid)s) && st_expand(c.center, %(max_distance)s) order by distance limit %(limit)s'''

            params = {
                'srid': self.get_srid(),
                'center_x': self.center.get_point().x,
                'center_y': self.center.get_point().y,
                'min_population': self.min_population,
                'max_distance': self.max_distance,
                'limit': self.limit
            }

            self.cur.execute(queryString, params)

            locations = []
            if self.cur.rowcount > 0:
                for result in self.cur.fetchall():
                    location = {}
                    location['name'] = result['name']
                    location['distance'] = result['distance']
                    location['azimuth'] = result['azimuth']
                    locations.append(location)

            return locations
