# -*- coding: utf8 -*-

"""
Copyright (C) 2012-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""
import datetime

import psycopg2
import shapely.wkb
from .query import SelectQuery, GridQuery


class Strike(object):
    def select_query(self, table_name, srid, *args):
        return SelectQuery() \
            .set_table_name(table_name) \
            .set_columns('id', '"timestamp"', 'nanoseconds', 'ST_X(ST_Transform(geog::geometry, %(srid)s)) AS x',
                         'ST_Y(ST_Transform(geog::geometry, %(srid)s)) AS y', 'altitude', 'amplitude', 'error2d',
                         'stationcount') \
            .add_parameters({'srid': srid}) \
            .parse_args(args)

    def grid_query(self, table_name, grid, *args):
        return GridQuery(grid) \
            .set_table_name(table_name) \
            .parse_args(args)

    def histogram_query(self, table_name, minutes, minute_offset, binsize, region=None, envelope=None):

        query = SelectQuery() \
            .set_table_name(table_name) \
            .add_column("-extract(epoch from clock_timestamp() + interval '%(offset)s minutes'"
                        " - \"timestamp\")::int/60/%(binsize)s as interval") \
            .add_column("count(*)") \
            .add_condition("\"timestamp\" >= (select clock_timestamp() + interval '%(offset)s minutes'"
                           " - interval '%(minutes)s minutes')") \
            .add_condition("\"timestamp\" < (select clock_timestamp() + interval '%(offset)s minutes') ") \
            .add_group_by("interval") \
            .add_order("interval") \
            .add_parameters({'minutes': minutes, 'offset': minute_offset, 'binsize': binsize})

        if region:
            query.add_condition("region = %(region)s", {'region': region})

        if envelope and envelope.get_env().is_valid:
            query.add_condition('ST_SetSRID(CAST(%(envelope)s AS geometry), %(envelope_srid)s) && geog',
                                {'envelope': psycopg2.Binary(shapely.wkb.dumps(envelope.get_env())),
                                 'envelope_srid': envelope.get_srid()})

        return query


class StrikeCluster(object):
    def select_query(self, table_name, srid, timestamp, interval_duration, interval_count=1, interval_offset=None):
        end_time = timestamp
        interval_offset = interval_duration if interval_offset is None or interval_offset.total_seconds() <= 0 else interval_offset
        start_time = timestamp - interval_offset * (interval_count - 1) - interval_duration

        query = SelectQuery() \
            .set_table_name(table_name) \
            .add_column("id") \
            .add_column("\"timestamp\"") \
            .add_column("ST_Transform(geog::geometry, %(srid)s) as geom") \
            .add_column("strike_count") \
            .add_parameters({'srid': srid}) \
            .add_condition("\"timestamp\" in (%(timestamps)s)",
                           {'timestamps':
                                ",".join(str(timestamp) for timestamp in
                                         self.get_timestamps(start_time, end_time, interval_duration,
                                                             interval_offset))}) \
            .add_condition("interval_seconds=%(interval_seconds)s",
                           {'interval_seconds': interval_duration.total_seconds()})

        return query

    def get_timestamps(self, start_time, end_time, interval_duration, interval_offset):
        final_time = start_time + interval_duration
        current_time = end_time
        while current_time >= final_time:
            yield current_time
            current_time -= interval_offset
