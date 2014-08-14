# -*- coding: utf8 -*-

from __future__ import print_function

import datetime
import shapely.geometry.base
import shapely.wkb
import psycopg2


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
        self.conditions = []
        self.groups = []
        self.parameters = {}
        self.limit = None
        self.order = []

    def add_group_by(self, group_by):
        self.groups.append(group_by)
        return self

    def add_order(self, order):
        self.order.append(order if isinstance(order, Order) else Order(order))
        return self

    def set_limit(self, limit):
        if self.limit:
            raise RuntimeError("overriding Query.limit")
        self.limit = limit
        return self

    def add_condition(self, condition, parameters=None):
        self.conditions.append(condition)
        if parameters:
            self.parameters.update(parameters)
        return self

    def add_parameters(self, parameters):
        self.parameters.update(parameters)
        return self

    def __str__(self):
        sql = ""

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
                        self.add_condition('ST_GeomFromWKB(%(envelope)s, %(srid)s) && geog',
                                           {'envelope': psycopg2.Binary(shapely.wkb.dumps(arg.envelope))})

                        if not arg.equals(arg.envelope):
                            self.add_condition(
                                'ST_Intersects(ST_GeomFromWKB(%(geometry)s, %(srid)s), ' +
                                'ST_Transform(geog::geometry, %(srid)s))',
                                {'geometry': psycopg2.Binary(shapely.wkb.dumps(arg))})

                    else:
                        raise ValueError("invalid geometry in db.Strike.select()")

                elif isinstance(arg, Order):
                    self.add_order(arg)

                elif isinstance(arg, Limit):
                    self.set_limit(arg)

                else:
                    print('WARNING: ' + __name__ + ' unhandled condition ' + str(type(arg)))
        return self


class SelectQuery(Query):
    def __init__(self):
        super(SelectQuery, self).__init__()
        self.table_name = ""
        self.columns = []

    def set_table_name(self, table_name):
        self.table_name = table_name
        return self

    def set_columns(self, *columns):
        self.columns = columns
        return self

    def add_column(self, column):
        self.columns.append(column)
        return self

    def __str__(self):
        sql = 'SELECT '

        if self.columns:
            sql += ', '.join(self.columns) + ' '

        sql += 'FROM ' + self.table_name + ' '

        sql += super(SelectQuery, self).__str__()

        return sql.strip()


class GridQuery(SelectQuery):
    def __init__(self, raster):
        super(GridQuery, self).__init__()

        self.raster = raster

        self.add_parameters({
            'srid': raster.get_srid(),
            'xmin': raster.get_x_min(),
            'xdiv': raster.get_x_div(),
            'ymin': raster.get_y_min(),
            'ydiv': raster.get_y_div(),
        })

        self.set_columns(
            'TRUNC((ST_X(ST_Transform(geog::geometry, %(srid)s)) - %(xmin)s) / %(xdiv)s) AS rx',
            'TRUNC((ST_Y(ST_Transform(geog::geometry, %(srid)s)) - %(ymin)s) / %(ydiv)s) AS ry',
            'count(*) AS count',
            'max("timestamp") as "timestamp"'
        )

        env = self.raster.get_env()

        if env.is_valid:
            self.add_condition('ST_GeomFromWKB(%(envelope)s, %(envelope_srid)s) && geog',
                               {'envelope': psycopg2.Binary(shapely.wkb.dumps(env)),
                                'envelope_srid': raster.get_srid()})
        else:
            raise ValueError("invalid Raster geometry in db.Strike.select()")

    def __str__(self):
        sql = super(GridQuery, self).__str__()

        sql += ' GROUP BY rx, ry'

        return sql


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

