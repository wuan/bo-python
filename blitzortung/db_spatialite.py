# -*- coding: utf8 -*-

'''

@author: Andreas Würl

'''

import os

import pytz
import shapely.geometry

from pysqlite2 import dbapi2 as db

import data
import geom

from abc import ABCMeta, abstractmethod

class TimeInterval:

  def __init__(self, start = None, end = None):
    self.start = start
    self.end = end

  def get_start(self):
    return self.start

  def get_end(self):
    return self.end

  def __str__(self):
    return '[' + str(self.start) + ' - ' + str(self.end) + ']'

class Query:
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
        self.raster = None
        self.order = []

    def set_table_name(self, table_name):
        self.table_name = table_name

    def set_columns(self, columns):
        self.columns = columns

    def add_order(self, order):
        self.order.append(order)

    def set_limit(self, limit):
        if self.limit != None:
            raise Error("overriding Query.limit")
        self.limit = limit

    def add_condition(self, condition, parameters = None):
        self.conditions.append(condition)
        if parameters != None:
            self.parameters.update(parameters)

    def add_parameters(self, parameters):
        self.parameters.update(parameters)

    def __str__(self):
        sql = 'SELECT '

        if self.raster:
            sql += 'TRUNC((ST_X(Transform(the_geom, :srid)) - ' + str(self.raster.getXMin()) + ') /' + str(self.raster.getXDiv()) + ') AS rx, '
            sql += 'TRUNC((ST_Y(Transform(the_geom, :srid)) - ' + str(self.raster.getYMin()) + ') /' + str(self.raster.getYDiv()) + ') AS ry, '
            sql += 'count(*) AS count FROM ( SELECT '

        if self.columns != None:
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
                sql += order.get_column() + ' '
                if order.is_desc():
                    sql += 'DESC '

        if self.limit != None:
            sql += 'LIMIT ' + str(self.limit.get_number()) + ' '

        if self.raster:
            sql += ') AS ' + self.table_name + ' GROUP BY rx, ry'

        return sql

    def get_parameters(self):
        return self.parameters

    def parse_args(self, args):
        for arg in args:
            if arg != None:
                if isinstance(arg, TimeInterval):

                    if arg.get_start() != None:
                        self.add_condition('timestamp >= :starttime', {'starttime': arg.get_start().astimezone(pytz.UTC).replace(tzinfo=None)})

                    if arg.get_end() != None:
                        self.add_condition('timestamp < :endtime', {'endtime': arg.get_end().astimezone(pytz.UTC).replace(tzinfo=None)})

                elif isinstance(arg, shapely.geometry.base.BaseGeometry):

                    if arg.is_valid:

                        self.add_condition('SetSRID(CAST(:envelope AS geometry), :srid) && Transform(the_geom, :srid)', {'envelope': shapely.wkb.dumps(arg.envelope).encode('hex')})

                        if not arg.equals(arg.envelope):
                            self.add_condition('Intersects(SetSRID(CAST(:geometry AS geometry), :srid), Transform(the_geom, :srid))', {'geometry': shapely.wkb.dumps(arg).encode('hex')})

                    else:
                        raise Error("invalid geometry in db.Stroke.select()")

                elif isinstance(arg, geom.Raster):
                    self.raster = arg
                    env = self.raster.getEnv()

                    if env.is_valid:
                        self.add_condition('SetSRID(CAST(:envelope AS geometry), :srid) && Transform(the_geom, :srid)', {'envelope': shapely.wkb.dumps(env).encode('hex')})
                    else:
                        raise Error("invalid Raster geometry in db.Stroke.select()")

                elif isinstance(arg, Order):
                    self.add_order(arg)

                elif isinstance(arg, Limit):
                    self.setLimit(arg)

                else:
                    print 'WARNING: ' + __name__ + ' unhandled object ' + str(type(arg))

    def get_results(self, db):

        if self.raster == None:
            strokes = []
	    for result in db.cur.fetchall():
		strokes.append(db.create(result))
            return strokes
        else:
            if db.cur.rowcount > 0:
                for result in db.cur.fetchall():
                    self.raster.set(result['rx'], result['ry'], result['count'])
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

    '''
    __metaclass__ = ABCMeta

    DefaultTimezone = pytz.UTC

    def __init__(self):
        '''
        create sqlite db access object
        '''

        self.db_file = "/tmp/blitzortung.sqlite"
        self.schema_name = None
        self.cur = None
        self.conn = None

        self.srid = geom.Geometry.DefaultSrid
        self.tz = Base.DefaultTimezone

        try:
            self.conn = db.connect(self.db_file, detect_types=db.PARSE_DECLTYPES)
            self.conn.enable_load_extension(True)
            self.conn.execute('SELECT load_extension("libspatialite.so");')
  	    self.conn.row_factory = db.Row
            self.cur = self.conn.cursor()
        except Exception, e:
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

        if not self.has_table('geometry_columns'):
	    self.cur.execute('SELECT InitSpatialMetadata()')
	    self.cur.execute("INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ref_sys_name, proj4text) VALUES (4326, 'epsg', 4326, 'WGS 84', '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')");

    def has_table(self, table_name):
      result = self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'" % table_name)
      return result.fetchone() != None

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

    def getNextId(self):
        return nextid

    @abstractmethod
    def insert(self, object):
        pass

    @abstractmethod
    def select(self, args):
        pass


class Stroke(Base):
    '''
    stroke db access class

    empty the table with the following commands:

    DELETE FROM strokes;
    ALTER SEQUENCE strokes_id_seq RESTART 1;

    '''

    def __init__(self):
        Base.__init__(self)
        self.set_table_name('strokes')

	if not self.has_table(self.get_table_name()):
    	    self.cur.execute("CREATE TABLE strokes (id INTEGER PRIMARY KEY, timestamp timestamp, nanoseconds INTEGER)")
	    self.cur.execute("SELECT AddGeometryColumn('strokes','the_geom',4326,'POINT',2)")
	    self.cur.execute("ALTER TABLE strokes ADD COLUMN amplitude REAL")
	    self.cur.execute("ALTER TABLE strokes ADD COLUMN error2d INTEGER")
   	    self.cur.execute("ALTER TABLE strokes ADD COLUMN type INTEGER")
            self.cur.execute("ALTER TABLE strokes ADD COLUMN stationcount INTEGER")
            self.cur.execute("ALTER TABLE strokes ADD COLUMN detected BOOLEAN")
# create index strokes_timestamp on strokes (timestamp);
# create index strokes_id on strokes (id);
#    CREATE INDEX strokes_timestamp ON strokes USING btree("timestamp");
#    CREATE INDEX strokes_geom ON strokes USING gist(the_geom);
#    CREATE INDEX strokes_timestamp_geom ON strokes USING gist("timestamp", the_geom);
#    CREATE INDEX strokes_id_timestamp_geom ON strokes USING gist(id, "timestamp", the_geom);

    def insert(self, stroke):
        sql = 'INSERT INTO ' + self.get_full_table_name() + \
            ' (timestamp, nanoseconds, the_geom, amplitude, error2d, type, stationcount, detected) ' + \
            'VALUES (:timestamp, :nanoseconds, makepoint(:longitude, :latitude, 4326), :amplitude, :error2d, :type, :stationcount, :detected)'

	parameters = {
           'timestamp': self.from_timezone_to_bare_utc(stroke.get_time()),
	   'nanoseconds': stroke.get_nanoseconds(),
	   'longitude': stroke.x,
	   'latitude': stroke.y,
	   'amplitude': stroke.getAmplitude(),
	   'error2d': stroke.getError2D(),
	   'type': stroke.getType(),
	   'stationcount': stroke.getStationCount(),
	   'detected': stroke.isDetectedByUser(),
	}
        self.cur.execute(sql, parameters)

    def get_latest_time(self):
        sql = 'SELECT timestamp FROM ' + self.get_full_table_name() + \
            ' ORDER BY timestamp DESC LIMIT 1'
        self.cur.execute(sql)
        result = self.cur.fetchone()
	if result:
            return self.from_bare_utc_to_timezone(result['timestamp'])
        else:
            return None

    def create(self, result):
        stroke = data.Stroke()
        stroke.set_time(self.from_bare_utc_to_timezone(result['timestamp']))
	stroke.set_nanoseconds(result['nanoseconds'])
        stroke.set_location(shapely.wkb.loads(str(result['the_geom'])))
        stroke.setAmplitude(result['amplitude'])
        stroke.setType(result['type'])
        stroke.setStationCount(result['stationcount'])
        stroke.setError2D(result['error2d'])
#        stroke.setDetectedByUser(result['detected'])

        return stroke

    def select_query(self, args):
        ' build up query object for select statement '
        query = Query()
        query.set_table_name(self.get_full_table_name())
        query.set_columns(['timestamp', 'nanoseconds', 'st_asbinary(Transform(the_geom, :srid)) AS the_geom', 'amplitude', 'type', 'error2d', 'stationcount', 'detected'])
        query.add_parameters({'srid': self.srid})

        query.parse_args(args)
        return query

    def select(self, *args):

        ' build up query '
        query = self.select_query(args)

        ' perform query '
        self.cur.execute(str(query), query.get_parameters())

        ' collect and return data '   
        return query.get_results(self)

class Location(Base):
    '''
    geonames db access class

    CREATE SCHEMA gis;

    CREATE TABLE gis.geonames (id INTEGER PRIMARY KEY, "name" character varying);
    SELECT AddGeometryColumn('gis','geonames','the_geom','4326','POINT',2);

    ALTER TABLE gis.geonames ADD COLUMN "class" INTEGER;
    ALTER TABLE gis.geonames ADD COLUMN feature CHARACTER(1);
    ALTER TABLE gis.geonames ADD COLUMN subfeature VARCHAR;
    ALTER TABLE gis.geonames ADD COLUMN country VARCHAR;
    ALTER TABLE gis.geonames ADD COLUMN admin1 VARCHAR;
    ALTER TABLE gis.geonames ADD COLUMN admin2 VARCHAR;
    ALTER TABLE gis.geonames ADD COLUMN population INTEGER;
    ALTER TABLE gis.geonames ADD COLUMN elevation INTEGER;

    GRANT SELECT ON TABLE gis.geonames TO bogroup_ro;
    '''

    def __init__(self):
        Base.__init__(self)
        self.set_schema_name('geo')
        self.set_table_name('geonames')

    def deleteAll(self):
        self.cur.execute('DELETE FROM ' + self.get_full_table_name())

    def insert(self, line):

        ' split line and parse resulting fields'
        fields = line.strip().split('\t')
        name = fields[1]
        latitude = float(fields[4])
        longitude = float(fields[5])
        feature = fields[6]
        subfeature = fields[7]
        country = fields[8]
        country2 = fields[9]
        admin1 = fields[10]
        admin2 = fields[11]
        admin3 = fields[12]
        admin4 = fields[13]
        population = int(fields[14])
        if fields[15] != '':
            elevation = int(fields[15])
        else:
            elevation = -1

        name = name.replace("'", "''")

        sizeclass = self.size_class(population)

        self.cur.execute('INSERT INTO ' + self.get_full_table_name() + '''
          (the_geom, name, class, feature, subfeature, country, admin1, admin2, population, elevation)
        VALUES(
          GeomFromText('POINT(%f %f)', 4326), '%s', %d, '%s', '%s', '%s', '%s', '%s', %d, %d)'''
                         % (longitude, latitude, name, classification, feature, subfeature, country, admin1, admin2, population, elevation))

    def size_class(self, n):
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
                    ' center point information given '
                    self.center = arg
                elif isinstance(arg, Limit):
                    ' limit information given '
                    self.limit = arg

        if self.is_connected():
            queryString = '''SELECT
        name,
        country,
        admin1,
        admin2,
        feature,
        subfeature,
        elevation,
        Transform(the_geom, :srid) AS the_geom,
        population,
        distance_sphere(the_geom, c.center) AS distance,
        st_azimuth(the_geom, c.center)
      FROM
        (SELECT SetSRID(MakePoint(:center_x, :center_y), :srid) as center ) as c,
        :table_name
      WHERE
        feature='P'
        AND population >= :min_population
        AND Transform(the_geom, :srid) && st_expand(c.center, :max_distance) order by distance limit :limit''';

            params = {}
            params['srid'] = self.get_srid()
            params['table_name'] = self.get_full_table_name()
            params['center_x'] = self.center.get_point().x
            params['center_y'] = self.center.get_point().y
            params['min_population'] = self.min_population
            params['max_distance'] = self.max_distance
            params['limit'] = self.limit

            self.cur.execute(queryString % params)

  	    locations = []
            if self.cur.rowcount > 0:
                for result in self.cur.fetchall():
                    print result
            return None

