# -*- coding: utf8 -*-

'''

@author: Andreas Würl

'''

import os

import math

import pytz
import shapely.wkb

import psycopg2
import psycopg2.extras
import psycopg2.extensions

import GeoTypes

import data
import geom

GeoTypes.initialisePsycopgTypes(psycopg_module=psycopg2, psycopg_extensions_module=psycopg2.extensions)

from abc import ABCMeta, abstractmethod

class IdInterval:

  def __init__(self, start = None, end = None):
    self.start = start
    self.end = end

  def get_start(self):
    return self.start

  def get_end(self):
    return self.end

  def __str__(self):
    return '[' + str(self.start) + ' - ' + str(self.end) + ']'

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
	sql += order.get_column() + ' '
	if order.is_desc():
	  sql += 'DESC '

    if self.limit:
      sql += 'LIMIT ' + str(self.limit.get_number()) + ' '

    return sql

  def get_parameters(self):
      return self.parameters

  def parse_args(self, args):
    for arg in args:
      if arg:
	if isinstance(arg, TimeInterval):

	  if arg.get_start() != None:
	    self.add_condition('timestamp >= %(starttime)s', {'starttime': arg.get_start()})

	  if arg.get_end() != None:
	    self.add_condition('timestamp < %(endtime)s', {'endtime': arg.get_end()})

	elif isinstance(arg, IdInterval):

	  if arg.get_start() != None:
	    self.add_condition('id >= %(startid)s', {'startid': arg.get_start()})

	  if arg.get_end() != None:
	    self.add_condition('id < %(endid)s', {'endid': arg.get_end()})

	elif isinstance(arg, shapely.geometry.base.BaseGeometry):

	  if arg.is_valid:

	    self.add_condition('SetSRID(CAST(%(envelope)s AS geometry), %(srid)s) && st_transform(the_geom, %(srid)s)', {'envelope': shapely.wkb.dumps(arg.envelope).encode('hex')})

	    if not arg.equals(arg.envelope):
	      self.add_condition('Intersects(SetSRID(CAST(%(geometry)s AS geometry), %(srid)s), st_transform(the_geom, %(srid)s))', {'geometry': shapely.wkb.dumps(arg).encode('hex')})

	  else:
	      raise Error("invalid geometry in db.Stroke.select()")

	elif isinstance(arg, Order):
	    self.add_order(arg)

	elif isinstance(arg, Limit):
	    self.setLimit(arg)

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
    else:
      raise Error("invalid Raster geometry in db.Stroke.select()")

  def __str__(self):
    sql = 'SELECT '

    sql += 'TRUNC((ST_X(ST_TRANSFORM(the_geom, %(srid)s)) - ' + str(self.raster.getXMin()) + ') /' + str(self.raster.getXDiv()) + ') AS rx, '
    sql += 'TRUNC((ST_Y(ST_TRANSFORM(the_geom, %(srid)s)) - ' + str(self.raster.getYMin()) + ') /' + str(self.raster.getYDiv()) + ') AS ry, '
    sql += 'count(*) AS count FROM ('

    sql += Query.__str__(self)

    sql += ') AS ' + self.table_name + ' GROUP BY rx, ry'

    return sql

  def get_results(self, db):

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

    creation of database 

    psql as user postgres:

    CREATE USER blitzortung PASSWORD 'blitzortung' INHERIT;

    createdb -T postgistemplate -E utf8 -O blitzortung blitzortung

    psql blitzortung

    GRANT SELECT ON spatial_ref_sys TO blitzortung;
    GRANT SELECT ON geometry_columns TO blitzortung;
    GRANT INSERT, DELETE ON geometry_columns TO blitzortung;

    '''
    __metaclass__ = ABCMeta

    DefaultTimezone = pytz.UTC

    def __init__(self):
        '''
        create PostgreSQL db access object
        '''

        connection = "host='localhost' dbname='blitzortung' user='blitzortung' password='blitzortung'"
        self.schema_name = None
        self.cur = None
        self.conn = None

        self.srid = geom.Geometry.DefaultSrid
        self.tz = Base.DefaultTimezone

        try:
            self.conn = psycopg2.connect(connection)
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
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

    def commit(self):
        ''' commit pending database transaction '''
        self.conn.commit()

    def rollback(self):
        ''' rollback pending database transaction '''
        self.conn.rollback()

    @abstractmethod
    def insert(self, object):
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
    GRANT SELECT ON TABLE strokes TO bogroup_ro;

    ALTER TABLE strokes ADD COLUMN amplitude REAL;
    ALTER TABLE strokes ADD COLUMN error2d SMALLINT;
    ALTER TABLE strokes ADD COLUMN type SMALLINT;
    ALTER TABLE strokes ADD COLUMN stationcount SMALLINT;

    CREATE INDEX strokes_timestamp ON strokes USING btree("timestamp");
    CREATE INDEX strokes_geom ON strokes USING gist(the_geom);
    CREATE INDEX strokes_timestamp_geom ON strokes USING gist("timestamp", the_geom);
    CREATE INDEX strokes_id_timestamp_geom ON strokes USING gist(id, "timestamp", the_geom);

    empty the table with the following commands:

    DELETE FROM strokes;
    ALTER SEQUENCE strokes_id_seq RESTART 1;

    '''

    def __init__(self):
        super(Stroke,self).__init__()

        self.set_table_name('strokes')

    def insert(self, stroke):
        sql = 'INSERT INTO ' + self.get_full_table_name() + \
            ' ("timestamp", nanoseconds, the_geom, amplitude, error2d, type, stationcount) ' + \
            'VALUES (\'%s\', %d, st_setsrid(makepoint(%f, %f), 4326), %f, %d, %d, %d)' \
            %(stroke.get_time(), stroke.get_nanoseconds(), stroke.get_location().x, stroke.get_location().y, stroke.get_amplitude(), stroke.get_lateral_error(), stroke.get_type(), stroke.get_station_count())
        self.cur.execute(sql)

    def get_latest_time(self):
        sql = 'SELECT timestamp FROM ' + self.get_full_table_name() + \
            ' ORDER BY timestamp DESC LIMIT 1'
        self.cur.execute(sql)
        if self.cur.rowcount == 1:
            result = self.cur.fetchone()
            return result['timestamp']
        else:
            return None

    def create(self, result):
        stroke = data.Stroke()

	stroke.set_id(result['id'])
        stroke.set_time(result['timestamp'])
	stroke.set_nanoseconds(result['nanoseconds'])
        stroke.set_location(shapely.wkb.loads(result['the_geom'].decode('hex')))
        stroke.set_amplitude(result['amplitude'])
        stroke.set_type(result['type'])
        stroke.set_station_count(result['stationcount'])
        stroke.set_lateral_error(result['error2d'])

        return stroke

    def select_query(self, args, query = Query()):
        ' build up query object for select statement '
        query.set_table_name(self.get_full_table_name())
        query.set_columns(['id', '"timestamp"', 'nanoseconds', 'st_transform(the_geom, %i) AS the_geom' % self.srid, 'amplitude', 'type', 'error2d', 'stationcount'])
        query.add_parameters({'srid': self.srid})

        query.add_condition('the_geom IS NOT NULL')
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
        ' set timezone for query '
        self.cur.execute('SET TIME ZONE \'%s\'' %(str(self.tz)))

        ' perform query '
        self.cur.execute(str(query), query.get_parameters())

        ' collect and return data '   
        return query.get_results(self)

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
    super(Location,self).__init__()
    self.set_schema_name('geo')
    self.set_table_name('geonames')

  def delete_all(self):
    self.cur.execute('DELETE FROM ' + self.get_full_table_name())

  def insert(self, line):
    fields = line.strip().split('\t')
    name = fields[1]
    latitude = float(fields[4])
    longitude = float(fields[5])
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
	GeomFromText('POINT(%f %f)', 4326), '%s', %d, '%s', '%s', '%s', '%s', '%s', %d, %d)'''
		       % (longitude, latitude, name, classification, feature_class, feature_code, country_code, admin_code_1, admin_code_2, population, elevation))

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
	  ' center point information given '
	  self.center = arg
	elif isinstance(arg, Limit):
	  ' limit information given '
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
	  (SELECT SetSRID(MakePoint(%(center_x)s, %(center_y)s), %(srid)s) as center ) as c,
	  %(table_name)s
	WHERE
	  feature_class='P'
	  AND population >= %(min_population)s
	  AND st_transform(the_geom, %(srid)s) && st_expand(c.center, %(max_distance)s) order by distance limit %(limit)s''';

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
	  location = {}
	  location['name'] = result['name']
	  location['distance'] = result['distance']
	  location['azimuth'] = result['azimuth']
	  locations.append(location)

      return locations

