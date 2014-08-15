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
