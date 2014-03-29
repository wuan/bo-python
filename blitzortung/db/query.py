# -*- coding: utf8 -*-
import datetime
import shapely.geometry.base
import shapely.wkb

import blitzortung


class BaseInterval(object):
    """
    Basic interval range
    """

    def __init__(self, start=None, end=None):
        self.start = start
        self.end = end

    def get_start(self):
        return self.start

    def get_end(self):
        return self.end

    def __str__(self):
        return '[' + (str(self.start) if self.start else '') + ' : ' + (str(self.end) if self.end else '') + ']'


class IdInterval(BaseInterval):
    """
    Interval of Id
    """

    def __init__(self, start=None, end=None):
        if start and not isinstance(start, int):
            raise ValueError("start should be an integer value")
        if end and not isinstance(end, int):
            raise ValueError("end should be an integer value")

        super(IdInterval, self).__init__(start, end)


class TimeInterval(BaseInterval):
    """
    Time interval
    """

    def __init__(self, start=None, end=None):
        if start and not isinstance(start, datetime.datetime):
            raise ValueError("start should be a datetime value")
        if end and not isinstance(end, datetime.datetime):
            raise ValueError("end should be a datetime value")

        super(TimeInterval, self).__init__(start, end)


class Query(object):
    """
    simple class for building of complex queries
    """

    def __init__(self):
        self.sql = ''
        self.conditions = []
        self.groups = []
        self.parameters = {}
        self.table_name = None
        self.columns = []
        self.limit = None
        self.order = []

    def set_table_name(self, table_name):
        self.table_name = table_name

    def set_columns(self, columns):
        self.columns = columns

    def add_column(self, column):
        self.columns.append(column)

    def add_group_by(self, group_by):
        self.groups.append(group_by)

    def add_order(self, order):
        self.order.append(order if isinstance(order, Order) else Order(order))

    def set_limit(self, limit):
        if self.limit:
            raise RuntimeError("overriding Query.limit")
        self.limit = limit

    def add_condition(self, condition, parameters=None):
        self.conditions.append(condition)
        if parameters:
            self.parameters.update(parameters)

    def add_parameters(self, parameters):
        self.parameters.update(parameters)

    def __str__(self):
        sql = 'SELECT '

        if self.columns:
            sql += ', '.join(self.columns) + ' '

        sql += 'FROM ' + self.table_name + ' '

        if self.conditions:
            sql += 'WHERE ' + ' AND '.join(self.conditions) + ' '

        if self.groups:
            sql += 'GROUP BY ' + ', '.join(self.groups) + ' '

        if self.order:
            build_order_query = lambda order: order.get_column() + (' DESC' if order.is_desc() else '')
            order_query_elements = map(build_order_query, self.order)
            sql += 'ORDER BY ' + ', '.join(order_query_elements) + ' '

        if self.limit:
            sql += 'LIMIT ' + str(self.limit.get_number()) + ' '

        return sql.strip()

    def get_parameters(self):
        return self.parameters

    def parse_args(self, args):
        for arg in args:
            if arg:
                if isinstance(arg, TimeInterval):

                    if arg.get_start():
                        self.add_condition('"timestamp" >= %(start_time)s', {'start_time': arg.get_start()})

                    if arg.get_end():
                        self.add_condition('"timestamp" < %(end_time)s', {'end_time': arg.get_end()})

                elif isinstance(arg, IdInterval):

                    if arg.get_start():
                        self.add_condition('id >= %(start_id)s', {'start_id': arg.get_start()})

                    if arg.get_end():
                        self.add_condition('id < %(end_id)s', {'end_id': arg.get_end()})

                elif isinstance(arg, shapely.geometry.base.BaseGeometry):

                    if arg.is_valid:

                        self.add_condition('ST_SetSRID(CAST(%(envelope)s AS geometry), %(srid)s) && geog',
                                           {'envelope': shapely.wkb.dumps(arg.envelope).encode('hex')})

                        if not arg.equals(arg.envelope):
                            self.add_condition(
                                'Intersects(ST_SetSRID(CAST(%(geometry)s AS geometry), %(srid)s), ' +
                                'ST_Transform(geog::geometry, %(srid)s))',
                                {'geometry': shapely.wkb.dumps(arg).encode('hex')})

                    else:
                        raise ValueError("invalid geometry in db.Stroke.select()")

                elif isinstance(arg, Order):
                    self.add_order(arg)

                elif isinstance(arg, Limit):
                    self.set_limit(arg)

                else:
                    print('WARNING: ' + __name__ + ' unhandled condition ' + str(type(arg)))

    def get_results(self, cursor, object_creator):

        resulting_strokes = []
        if cursor.rowcount > 0:
            for result in cursor.fetchall():
                resulting_strokes.append(object_creator(result))

        return resulting_strokes


class RasterQuery(Query):
    def __init__(self, raster):
        super(RasterQuery, self).__init__()

        self.raster = raster

        env = self.raster.get_env()

        if env.is_valid:
            self.add_condition('ST_SetSRID(CAST(%(envelope)s AS geometry), %(envelope_srid)s) && geog',
                               {'envelope': shapely.wkb.dumps(env).encode('hex'), 'envelope_srid': raster.get_srid()})
        else:
            raise ValueError("invalid Raster geometry in db.Stroke.select()")

    def __str__(self):
        sql = 'SELECT '

        sql += 'TRUNC((ST_X(ST_TRANSFORM(geog, %(srid)s)) - ' + str(self.raster.get_x_min()) + ') /' + str(
            self.raster.get_x_div()) + ') AS rx, '
        sql += 'TRUNC((ST_Y(ST_TRANSFORM(geog, %(srid)s)) - ' + str(self.raster.get_y_min()) + ') /' + str(
            self.raster.get_y_div()) + ') AS ry, '
        sql += 'count(*) AS count, max("timestamp") as "timestamp" FROM ('
        sql += Query.__str__(self)
        sql += ') AS ' + self.table_name + ' GROUP BY rx, ry'

        return sql

    def get_results(self, cursor, _):

        self.raster.clear()

        if cursor.rowcount > 0:
            for result in cursor.fetchall():
                self.raster.set(result['rx'], result['ry'],
                                blitzortung.geom.RasterElement(result['count'], result['timestamp']))
        return self.raster


class Order(object):
    """
    definition for query search order
    """

    def __init__(self, column, desc=False):
        self.column = column
        self.desc = desc

    def get_column(self):
        return self.column

    def is_desc(self):
        return self.desc


class Limit(object):
    """
    definition of query result limit
    """

    def __init__(self, limit):
        self.limit = limit

    def get_number(self):
        return self.limit


class Center(object):
    """
    definition of query center point
    """

    def __init__(self, center):
        self.center = center

    def get_point(self):
        return self.center