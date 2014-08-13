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
