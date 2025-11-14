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
from dataclasses import dataclass
from typing import Optional

from injector import inject
from twisted.internet.defer import gatherResults
from twisted.python import log

from .db import execute
from .general import TimingState
from .. import db
from ..db.grid_result import build_grid_result
from ..db.query import TimeInterval
from ..geom import Grid


@dataclass(frozen=True)
class GridParameters:
    grid: Grid
    base_length: int
    region: Optional[int] = None
    count_threshold: int = 0


class StrikeGridState(TimingState):
    __slots__ = ['grid_parameters', 'time_interval']

    def __init__(self, statsd_client, grid_parameters: GridParameters, time_interval: TimeInterval):
        super().__init__("strikes_grid", statsd_client)
        self.grid_parameters = grid_parameters
        self.time_interval = time_interval


class StrikeGridQuery:
    @inject
    def __init__(self, strike_query_builder: db.query_builder.Strike):
        self.strike_query_builder = strike_query_builder

    def create(self, grid_parameters: GridParameters, time_interval: TimeInterval, connection_pool, statsd_client):
        state = StrikeGridState(statsd_client, grid_parameters, time_interval)

        query = self.strike_query_builder.grid_query(db.table.Strike.table_name, grid_parameters.grid,
                                                     time_interval=time_interval,
                                                     count_threshold=grid_parameters.count_threshold)

        result = execute(connection_pool, query)
        result.addCallback(self.build_result, state=state)
        result.addErrback(log.err)
        return result, state

    @staticmethod
    def build_result(results, state: StrikeGridState):
        state.add_info_text("grid query %.03fs #%d %s" % (state.get_seconds(), len(results), state.grid_parameters))
        state.log_timing('strikes_grid.query')

        reference_time = time.time()

        grid = state.grid_parameters.grid
        strikes_grid_result = build_grid_result(results, grid.x_bin_count, grid.y_bin_count, state.time_interval.end)

        state.add_info_text(", result %.03fs" % state.get_seconds(reference_time))
        state.log_timing('strikes_grid.build_result', reference_time)

        return strikes_grid_result

    def combine_result(self, strike_grid_result, histogram_result, state: StrikeGridState):
        combined_result = gatherResults([strike_grid_result, histogram_result], consumeErrors=True)
        combined_result.addCallback(self.build_grid_response, state=state)
        combined_result.addErrback(log.err)

        return combined_result

    @staticmethod
    def build_grid_response(results, state: StrikeGridState):
        state.log_timing('strikes_grid.results')

        grid_data = results[0]
        histogram_data = results[1]

        state.log_gauge('strikes_grid.size', len(grid_data) if grid_data else 0)
        state.log_gauge(f'strikes_grid.size.{state.grid_parameters.base_length}', len(grid_data))
        if state.grid_parameters.region is None:
            state.log_gauge('local_strikes_grid.size', len(grid_data) if grid_data else 0)
            state.log_gauge(f'local_strikes_grid.size.{state.grid_parameters.base_length}', len(grid_data))
        state.log_incr('strikes_grid')

        grid_parameters = state.grid_parameters.grid
        end_time = state.time_interval.end
        duration = state.time_interval.duration
        response = {'r': grid_data, 'xd': round(grid_parameters.x_div, 6),
                    'yd': round(grid_parameters.y_div, 6),
                    'x0': round(grid_parameters.x_min, 4),
                    'y1': round(grid_parameters.y_max + grid_parameters.y_div, 4),
                    'xc': grid_parameters.x_bin_count,
                    'yc': grid_parameters.y_bin_count,
                    't': end_time.strftime("%Y%m%dT%H:%M:%S"),
                    'dt': duration.seconds,
                    'h': histogram_data}
        state.add_info_text(", total %.03fs" % state.get_seconds())
        state.log_timing('strikes_grid.total')
        print("".join(state.info_text))

        return response


class GlobalStrikeGridQuery:
    @inject
    def __init__(self, strike_query_builder: db.query_builder.Strike):
        self.strike_query_builder = strike_query_builder

    def create(self, grid_parameters: GridParameters, time_interval: TimeInterval, connection_pool, statsd_client):
        state = StrikeGridState(statsd_client, grid_parameters, time_interval)

        query = self.strike_query_builder.global_grid_query(db.table.Strike.table_name, grid_parameters.grid,
                                                            time_interval=time_interval,
                                                            count_threshold=grid_parameters.count_threshold)

        result = execute(connection_pool, query)
        result.addCallback(self.build_result, state=state)
        result.addErrback(log.err)
        return result, state

    @staticmethod
    def build_result(results, state):
        state.add_info_text(
            "global grid query %.03fs #%d %s" % (state.get_seconds(), len(results), state.grid_parameters))
        state.log_timing('global_strikes_grid.query')

        reference_time = time.time()
        end_time = state.time_interval.end
        global_strikes_grid_result = tuple(
            (
                result['rx'],
                -result['ry'] - 1,
                result['strike_count'],
                -(end_time - result['timestamp']).seconds
            ) for result in results
        )
        state.add_info_text(", result %.03fs" % state.get_seconds(reference_time))
        state.log_timing('globaL_strikes_grid.build_result', reference_time)

        return global_strikes_grid_result

    def combine_result(self, strike_grid_result, histogram_result, state):
        combined_result = gatherResults([strike_grid_result, histogram_result], consumeErrors=True)
        combined_result.addCallback(self.build_grid_response, state=state)
        combined_result.addErrback(log.err)

        return combined_result

    @staticmethod
    def build_grid_response(results, state):
        state.log_timing('global_strikes_grid.results')

        grid_data = results[0]
        histogram_data = results[1]

        state.log_gauge('global_strikes_grid.size', len(grid_data))
        state.log_gauge(f'global_strikes_grid.size.{state.grid_parameters.base_length}', len(grid_data))
        state.log_incr('global_strikes_grid')

        grid_parameters = state.grid_parameters
        end_time = state.time_interval.end
        duration = state.time_interval.duration
        response = {'r': grid_data,
                    'xd': round(grid_parameters.grid.x_div, 6),
                    'yd': round(grid_parameters.grid.y_div, 6),
                    't': end_time.strftime("%Y%m%dT%H:%M:%S"),
                    'dt': duration.seconds,
                    'h': histogram_data}
        state.add_info_text(", total %.03fs" % state.get_seconds())
        state.log_timing('global_strikes_grid.total')
        print("".join(state.info_text))

        return response
