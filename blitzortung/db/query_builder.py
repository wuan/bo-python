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
from typing import Optional

import psycopg2

import shapely.wkb

from .query import SelectQuery, GridQuery, GlobalGridQuery, TimeInterval


class Strike:
    @staticmethod
    def select_query(table_name, srid, **kwargs):
        query = SelectQuery() \
            .set_table_name(table_name) \
            .set_columns('id', '"timestamp"', 'nanoseconds', 'ST_X(ST_Transform(geog::geometry, %(srid)s)) AS x',
                         'ST_Y(ST_Transform(geog::geometry, %(srid)s)) AS y', 'altitude', 'amplitude', 'error2d',
                         'stationcount') \
            .add_parameters(srid=srid) \
            .set_default_conditions(**kwargs)

        if 'region' in kwargs:
            query.add_condition("region = %(region)s", region=kwargs['region'])

        return query

    @staticmethod
    def grid_query(table_name, grid, count_threshold=0, **kwargs):
        return GridQuery(grid, count_threshold) \
            .set_table_name(table_name) \
            .set_default_conditions(**kwargs)

    @staticmethod
    def global_grid_query(table_name, grid, count_threshold=0, **kwargs):
        return GlobalGridQuery(grid, count_threshold) \
            .set_table_name(table_name) \
            .set_default_conditions(**kwargs)

    @staticmethod
    def histogram_query(table_name: str, time_interval: TimeInterval, binsize:int, region:Optional[int]=None, envelope=None) -> SelectQuery:

        query = SelectQuery() \
            .set_table_name(table_name) \
            .add_column("-extract( epoch from %(end_time)s - \"timestamp\")::int/60/%(binsize)s as interval") \
            .add_column("count(*)") \
            .add_group_by("interval") \
            .set_order("interval") \
            .set_default_conditions(time_interval=time_interval) \
            .add_parameters(binsize=binsize)

        if region:
            query.add_condition("region = %(region)s", region=region)

        if envelope and envelope.env.is_valid:
            query.add_condition('ST_SetSRID(CAST(%(envelope)s AS geometry), %(envelope_srid)s) && geog',
                                envelope=psycopg2.Binary(shapely.wkb.dumps(envelope.env)),
                                envelope_srid=envelope.srid)

        return query


class StrikeCluster:
    def select_query(self, table_name, srid, timestamp, interval_duration, interval_count=1, interval_offset=None):
        end_time = timestamp
        interval_offset = interval_duration if interval_offset is None or interval_offset.total_seconds() <= 0 \
            else interval_offset
        start_time = timestamp - interval_offset * (interval_count - 1) - interval_duration

        query = SelectQuery() \
            .set_table_name(table_name) \
            .add_column("id") \
            .add_column("\"timestamp\"") \
            .add_column("ST_Transform(geog::geometry, %(srid)s) as geom") \
            .add_column("strike_count") \
            .add_parameters(srid=srid) \
            .add_condition("\"timestamp\" in %(timestamps)s",
                           timestamps=tuple(str(timestamp) for timestamp in
                                            self.get_timestamps(start_time, end_time, interval_duration,
                                                                interval_offset))) \
            .add_condition("interval_seconds=%(interval_seconds)s",
                           interval_seconds=interval_duration.total_seconds())

        return query

    @staticmethod
    def get_timestamps(start_time, end_time, interval_duration, interval_offset):
        final_time = start_time + interval_duration
        current_time = end_time
        while current_time >= final_time:
            yield current_time
            current_time -= interval_offset
