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

import shapely.geometry.base
import shapely.wkb

import psycopg2

class BaseInterval:
    """
    Basic interval range
    """

    __slots__ = ['__start', '__end']

    def __init__(self, start=None, end=None):
        self.__start = start
        self.__end = end

    @property
    def start(self):
        return self.__start

    @property
    def end(self):
        return self.__end

    def __str__(self):
        return '[' + (str(self.start) if self.start else '') + ' : ' + (str(self.end) if self.end else '') + ']'


class IdInterval(BaseInterval):
    """
    Interval of Id
    """

    __slots__ = []

    def __init__(self, start=None, end=None):
        if start and not isinstance(start, int):
            raise ValueError("start should be an integer value")
        if end and not isinstance(end, int):
            raise ValueError("end should be an integer value")

        super().__init__(start, end)


class TimeInterval(BaseInterval):
    """
    Time interval
    """

    __slots__ = []

    def __init__(self, start=None, end=None):
        if start and not isinstance(start, datetime.datetime):
            raise ValueError("start should be a datetime value")
        if end and not isinstance(end, datetime.datetime):
            raise ValueError("end should be a datetime value")

        super().__init__(start, end)

    @property
    def duration(self):
        return self.end - self.start

    def contains(self, timestamp):
        return self.start <= timestamp < self.end

    def minutes(self) -> int:
        if self.start and self.end:
            return int((self.end - self.start).total_seconds() // 60)
        else:
            raise ValueError("incomplete time interval")


class Query:
    """
    simple class for building of complex queries
    """

    __slots__ = ['conditions', 'groups', 'groups_having', 'parameters', 'limit', 'order', 'default_conditions']

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
                sql += 'HAVING ' + ' AND '.join(self.groups_having) + ' '

        if self.order:
            def build_order_query(order): return order.get_column() + (' DESC' if order.is_desc() else '')

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
        if time_interval.start:
            self.add_condition('"timestamp" >= %(start_time)s', start_time=time_interval.start)

        if time_interval.end:
            self.add_condition('"timestamp" < %(end_time)s', end_time=time_interval.end)

    def add_id_interval(self, id_interval):
        if id_interval.start:
            self.add_condition('id >= %(start_id)s', start_id=id_interval.start)

        if id_interval.end:
            self.add_condition('id < %(end_id)s', end_id=id_interval.end)

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
    __slots__ = ['table_name', 'columns']

    def __init__(self):
        super().__init__()
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

        sql += super().__str__()

        return sql.strip()


class GridQuery(SelectQuery):
    __slots__ = ['grid']

    def __init__(self, grid, count_threshold=0):
        super().__init__()

        self.grid = grid

        self.add_parameters(
            srid=grid.srid,
            xmin=grid.x_min,
            xdiv=grid.x_div,
            ymin=grid.y_min,
            ydiv=grid.y_div,
        )

        self.set_columns(
            'TRUNC((ST_X(ST_Transform(geog::geometry, %(srid)s)) - %(xmin)s) / %(xdiv)s)::integer AS rx',
            'TRUNC((ST_Y(ST_Transform(geog::geometry, %(srid)s)) - %(ymin)s) / %(ydiv)s)::integer AS ry',
            'count(*) AS strike_count',
            'max("timestamp") as "timestamp"'
        )

        env = self.grid.env

        if env.is_valid:
            self.add_condition('ST_GeomFromWKB(%(envelope)s, %(envelope_srid)s) && geog',
                               envelope=psycopg2.Binary(shapely.wkb.dumps(env)),
                               envelope_srid=grid.srid)
        else:
            raise ValueError("invalid Raster geometry in db.query.GridQuery.__init__()")

        self.add_group_by('rx')
        self.add_group_by('ry')

        if count_threshold > 0:
            self.add_group_having("count(*) > %(count_threshold)s", count_threshold=count_threshold)


class GlobalGridQuery(SelectQuery):
    __slots__ = ['grid']

    def __init__(self, grid, count_threshold=0):
        super().__init__()

        self.grid = grid

        self.add_parameters(
            srid=grid.srid,
            xdiv=grid.x_div,
            ydiv=grid.y_div,
        )

        self.set_columns(
            'ROUND((ST_X(ST_Transform(geog::geometry, %(srid)s)) - %(xdiv)s * 0.5) / %(xdiv)s)::integer AS rx',
            'ROUND((ST_Y(ST_Transform(geog::geometry, %(srid)s)) - %(ydiv)s * 0.5) / %(ydiv)s)::integer AS ry',
            'count(*) AS strike_count',
            'max("timestamp") as "timestamp"'
        )

        self.add_group_by('rx')
        self.add_group_by('ry')

        if count_threshold > 0:
            self.add_group_having("count(*) > %(count_threshold)s", count_threshold=count_threshold)


class Order:
    """
    definition for query search order
    """

    __slots__ = ['column', 'desc']

    def __init__(self, column, desc=False):
        self.column = column
        self.desc = desc

    def get_column(self):
        return self.column

    def is_desc(self):
        return self.desc


class Center:
    """
    definition of query center point
    """

    __slots__ = ['point']

    def __init__(self, center):
        self.point = center

    def get_point(self):
        return self.point
