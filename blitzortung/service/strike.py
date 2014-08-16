import time
from injector import inject
from twisted.internet.defer import gatherResults
from twisted.python import log

import blitzortung
from general import TimingState, create_time_interval


class StrikeState(TimingState):
    def __init__(self, statsd_client, end_time):
        super(StrikeState, self).__init__(statsd_client)
        self.end_time = end_time

    def get_end_time(self):
        return self.end_time


class StrikeQuery(object):
    @inject(strike_query_builder=blitzortung.db.query_builder.Strike, strike_mapper=blitzortung.db.mapper.Strike)
    def __init__(self, strike_query_builder, strike_mapper):
        self.strike_query_builder = strike_query_builder
        self.strike_mapper = strike_mapper

    def create(self, id_or_offset, minute_length, minute_offset, connection, statsd_client):
        time_interval = create_time_interval(minute_length, minute_offset)
        state = StrikeState(statsd_client, time_interval.get_end())

        id_interval = blitzortung.db.query.IdInterval(id_or_offset) if id_or_offset > 0 else None
        order = blitzortung.db.query.Order('id')
        query = self.strike_query_builder.select_query(blitzortung.db.table.Strike.TABLE_NAME,
                                                       blitzortung.geom.Geometry.DefaultSrid, time_interval,
                                                       id_interval, order)

        strikes_result = connection.runQuery(str(query), query.get_parameters())
        strikes_result.addCallback(self.strike_build_results, state=state)
        return strikes_result, state

    def strike_build_results(self, query_result, state):
        print("strikes_query: %.03fs #%d" % (state.get_seconds(), len(query_result)))
        state.log_timing('strikes.query')

        reference_time = time.time()
        end_time = state.get_end_time()
        strikes = tuple(
            (
                (end_time - strike.get_timestamp()).seconds,
                strike.get_x(),
                strike.get_y(),
                strike.get_altitude(),
                strike.get_lateral_error(),
                strike.get_amplitude(),
                strike.get_station_count()
            ) for strike in self.create_strikes(query_result))

        result = {'s': strikes}

        if strikes:
            result['next'] = query_result[-1][0] + 1

        print("strikes_build_result: %.03fs" % state.get_seconds(reference_time))
        state.log_timing('strikes.build_result', reference_time)
        return result

    def create_strikes(self, query_results):
        for result in query_results:
            yield self.strike_mapper.create_object(result)

    def combine_result(self, strikes_result, histogram_result, state):
        query = gatherResults([strikes_result, histogram_result], consumeErrors=True)
        query.addCallback(self.compile_strikes_result, state=state)
        query.addErrback(log.err)
        return query

    @staticmethod
    def compile_strikes_result(result, state):
        strikes_result = result[0]
        histogram_result = result[1]

        final_result = {
            't': state.get_end_time().strftime("%Y%m%dT%H:%M:%S"),
            'h': histogram_result
        }
        final_result.update(strikes_result)

        print("strikes_total: %.03fs" % state.get_seconds())
        state.log_timing('strikes.total')

        return final_result


