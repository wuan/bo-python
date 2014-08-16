from injector import inject
import time
from twisted.internet.defer import gatherResults
from twisted.python import log

import blitzortung

from general import create_time_interval, TimingState


class StrikeGridState(TimingState):
    def __init__(self, statsd_client, grid_parameters, end_time):
        super(StrikeGridState, self).__init__(statsd_client)
        self.grid_parameters = grid_parameters
        self.end_time = end_time

    def get_grid_parameters(self):
        return self.grid_parameters

    def get_end_time(self):
        return self.end_time


class StrikeGridQuery(object):
    @inject(strike_query_builder=blitzortung.db.query_builder.Strike)
    def __init__(self, strike_query_builder):
        self.strike_query_builder = strike_query_builder

    def create(self, grid_parameters, minute_length, minute_offset, connection, statsd_client):
        time_interval = create_time_interval(minute_length, minute_offset)

        state = StrikeGridState(statsd_client, grid_parameters, time_interval.get_end())

        query = self.strike_query_builder.grid_query(blitzortung.db.table.Strike.TABLE_NAME, grid_parameters,
                                                     time_interval)
        print(str(query))
        grid_query = connection.runQuery(str(query), query.get_parameters())
        grid_query.addCallback(self.build_strikes_grid_result, state=state)
        grid_query.addErrback(log.err)
        return grid_query, state

    @staticmethod
    def build_strikes_grid_result(results, state):
        print("strikes_grid_query: %.03fs #%d %s" % (state.get_seconds(), len(results), state.get_grid_parameters()))
        state.log_timing('strikes_grid.query')

        reference_time = time.time()
        y_bin_count = state.get_grid_parameters().get_y_bin_count()
        end_time = state.get_end_time()
        strikes_grid_result = tuple(
            (
                result['rx'],
                y_bin_count - result['ry'] - 1,
                result['count'],
                -(end_time - result['timestamp']).seconds
            ) for result in results
        )
        print("strikes_grid_result: %.03fs" % state.get_seconds(reference_time))
        state.log_timing('strikes_grid.build_result', reference_time)

        return strikes_grid_result

    def combine_result(self, strike_grid_result, histogram_result, state):
        combined_result = gatherResults([strike_grid_result, histogram_result], consumeErrors=True)
        combined_result.addCallback(self.build_grid_response, state=state)
        combined_result.addErrback(log.err)

        return combined_result

    @staticmethod
    def build_grid_response(results, state):
        state.log_timing('strikes_grid.results')

        grid_data = results[0]
        histogram_data = results[1]

        state.log_gauge('strikes_grid.size', len(grid_data))
        state.log_incr('strikes_grid')

        grid_parameters = state.get_grid_parameters()
        end_time = state.get_end_time()
        response = {'r': grid_data, 'xd': round(grid_parameters.get_x_div(), 6),
                    'yd': round(grid_parameters.get_y_div(), 6),
                    'x0': round(grid_parameters.get_x_min(), 4), 'y1': round(grid_parameters.get_y_max(), 4),
                    'xc': grid_parameters.get_x_bin_count(),
                    'yc': grid_parameters.get_y_bin_count(), 't': end_time.strftime("%Y%m%dT%H:%M:%S"),
                    'h': histogram_data}
        print("strikes_grid_total: %.03fs" % state.get_seconds())
        state.log_timing('strikes_grid.total')
        return response

