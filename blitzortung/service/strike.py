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

import time

from injector import inject
from twisted.internet.defer import gatherResults
from twisted.python import log

from .general import TimingState, create_time_interval
from .. import db, geom
from ..data import Timestamp
from ..db.query import TimeInterval


class StrikeState(TimingState):
    __slots__ = ['end_time']

    def __init__(self, statsd_client, end_time):
        super().__init__("strikes", statsd_client)
        self.end_time = end_time


class StrikeQuery:
    @inject
    def __init__(self, strike_query_builder: db.query_builder.Strike, strike_mapper: db.mapper.Strike):
        self.strike_query_builder = strike_query_builder
        self.strike_mapper = strike_mapper
        self.id_order = db.query.Order('id')

    def create(self, id_or_offset, time_interval: TimeInterval, connection, statsd_client):
        state = StrikeState(statsd_client, Timestamp(time_interval.end))

        id_interval = db.query.IdInterval(id_or_offset) if id_or_offset > 0 else None
        query = self.strike_query_builder.select_query(db.table.Strike.table_name, geom.Geometry.default_srid,
                                                       time_interval=time_interval, order=self.id_order,
                                                       id_interval=id_interval)

        strikes_result = connection.runQuery(str(query), query.get_parameters())
        strikes_result.addCallback(self.build_result, state=state)
        return strikes_result, state

    def build_result(self, query_result, state):
        state.add_info_text("query %.03fs #%d" % (state.get_seconds(), len(query_result)))
        state.log_timing('strikes.query')

        reference_time = time.time()
        end_time = state.end_time
        strikes = tuple(
            (
                (end_time - strike.timestamp).seconds,
                strike.x,
                strike.y,
                strike.altitude,
                strike.lateral_error,
                strike.amplitude,
                strike.station_count
            ) for strike in self.create_strikes(query_result))

        result = {'s': strikes}

        if strikes:
            result['next'] = query_result[-1][0] + 1

        state.add_info_text(", result %.03fs" % state.get_seconds(reference_time))
        state.log_timing('strikes.build_result', reference_time)
        return result

    def create_strikes(self, query_results):
        for result in query_results:
            yield self.strike_mapper.create_object(result)

    def combine_result(self, strikes_result, histogram_result, state):
        query = gatherResults([strikes_result, histogram_result], consumeErrors=True)
        query.addCallback(self.build_strikes_response, state=state)
        query.addErrback(log.err)
        return query

    @staticmethod
    def build_strikes_response(result, state):
        strikes_result = result[0]
        histogram_result = result[1]

        final_result = {
            't': state.end_time.strftime("%Y%m%dT%H:%M:%S"),
            'h': histogram_result
        }
        final_result.update(strikes_result)

        state.add_info_text(", total %.03fs" % state.get_seconds())
        state.log_timing('strikes.total')

        return final_result
