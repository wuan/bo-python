# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

from __future__ import print_function

import datetime
import shapely.geometry.base
import shapely.wkb

try:
    import psycopg2
except ImportError:
    psycopg2 = None


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

    def get_duration(self):
        return self.end - self.start

    def contains(self, timestamp):
        return self.start <= timestamp < self.end


class Query(object):
    """
    simple class for building of complex queries
    """

    def __init__(self):
        self.conditions = []
        self.groups = []
        self.groups_having = []
        self.parameters = {}
        self.limit = None
        self.order = []
        self.default_conditions = {
            'time_interval': self.add_time_interval,
            'id_interval': self.add_id_interval,
            'geometry': self.add_geometry,
            'order': self._set_order,
            'limit': self.set_limit
        }

    def add_group_by(self, group_by):
        self.groups.append(group_by)
        return self

    def _set_order(self, order):
        order_items = order if type(order) is list else [order]
        return self.set_order(*order_items)

    def set_order(self, *order_items):
        if len(self.order) > 0:
            raise RuntimeError("overriding Query.limit")

        self.order = [(order_item if isinstance(order_item, Order) else Order(order_item)) for order_item in
                      order_items]
        return self

    def set_limit(self, limit):
        if self.limit:
            raise RuntimeError("overriding Query.limit")
        self.limit = limit
        return self

    def add_condition(self, condition, **parameters):
        self.conditions.append(condition)
        self.add_parameters(**parameters)
        return self

    def add_group_having(self, condition, **parameters):
        self.groups_having.append(condition)
        self.add_parameters(**parameters)
        return self

    def add_parameters(self, **parameters):
        self.parameters.update(parameters)
        return self

    def __str__(self):
        sql = ""

        if self.conditions:
            sql += 'WHERE ' + ' AND '.join(self.conditions) + ' '

        if self.groups:
            sql += 'GROUP BY ' + ', '.join(self.groups) + ' '

            if self.groups_having:
                sql += ' AND '.join(self.groups_having)

        if self.order:
            build_order_query = lambda order: order.get_column() + (' DESC' if order.is_desc() else '')
            order_query_elements = map(build_order_query, self.order)
            sql += 'ORDER BY ' + ', '.join(order_query_elements) + ' '

        if self.limit:
            sql += 'LIMIT ' + str(self.limit) + ' '

        return sql.strip()

    def get_parameters(self):
        return self.parameters

    def set_default_conditions(self, **kwargs):
        for keyword, value in kwargs.items():
            if keyword in self.default_conditions and value:
                self.default_conditions[keyword](value)
        return self

    def add_time_interval(self, time_interval):
        if time_interval.get_start():
            self.add_condition('"timestamp" >= %(start_time)s', start_time=time_interval.get_start())

        if time_interval.get_end():
            self.add_condition('"timestamp" < %(end_time)s', end_time=time_interval.get_end())

    def add_id_interval(self, id_interval):
        if id_interval.get_start():
            self.add_condition('id >= %(start_id)s', start_id=id_interval.get_start())

        if id_interval.get_end():
            self.add_condition('id < %(end_id)s', end_id=id_interval.get_end())

    def add_geometry(self, geometry):
        if geometry.is_valid:
            self.add_condition('ST_GeomFromWKB(%(envelope)s, %(srid)s) && geog',
                               envelope=psycopg2.Binary(shapely.wkb.dumps(geometry.envelope)))

            if not geometry.equals(geometry.envelope):
                self.add_condition(
                    'ST_Intersects(ST_GeomFromWKB(%(geometry)s, %(srid)s), ' +
                    'ST_Transform(geog::geometry, %(srid)s))',
                    geometry=psycopg2.Binary(shapely.wkb.dumps(geometry)))

        else:
            raise ValueError("invalid geometry in db.Strike.select()")


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
    def __init__(self, raster, count_threshold=0):
        super(GridQuery, self).__init__()

        self.raster = raster

        self.add_parameters(
            srid=raster.get_srid(),
            xmin=raster.get_x_min(),
            xdiv=raster.get_x_div(),
            ymin=raster.get_y_min(),
            ydiv=raster.get_y_div(),
        )

        self.set_columns(
            'TRUNC((ST_X(ST_Transform(geog::geometry, %(srid)s)) - %(xmin)s) / %(xdiv)s)::integer AS rx',
            'TRUNC((ST_Y(ST_Transform(geog::geometry, %(srid)s)) - %(ymin)s) / %(ydiv)s)::integer AS ry',
            'count(*) AS strike_count',
            'max("timestamp") as "timestamp"'
        )

        env = self.raster.get_env()

        if env.is_valid:
            self.add_condition('ST_GeomFromWKB(%(envelope)s, %(envelope_srid)s) && geog',
                               envelope=psycopg2.Binary(shapely.wkb.dumps(env)),
                               envelope_srid=raster.get_srid())
        else:
            raise ValueError("invalid Raster geometry in db.query.GridQuery.__init__()")

        if count_threshold > 0:
            self.add_group_having("strike_count > %(count_threshold)s", count_threshold=count_threshold)

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


class Center(object):
    """
    definition of query center point
    """

    def __init__(self, center):
        self.center = center

    def get_point(self):
        return self.center
