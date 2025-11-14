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

from .db import execute
from .. import db
from ..db.query import TimeInterval


class HistogramQuery:
    @inject
    def __init__(self, strike_query_builder: db.query_builder.Strike):
        self.strike_query_builder = strike_query_builder

    def create(self, time_interval: TimeInterval, connection_pool, region=None, envelope=None):
        reference_time = time.time()

        query = self.strike_query_builder.histogram_query(db.table.Strike.table_name, time_interval, 5, region,
                                                          envelope)

        result = execute(connection_pool, query)
        result.addCallback(self.build_result, minutes=time_interval.minutes(), bin_size=5,
                           reference_time=reference_time)
        return result

    @staticmethod
    def build_result(query_result, minutes, bin_size, reference_time):
        time_duration = time.time() - reference_time
        print("histogram: query %.03fs" % time_duration)
        value_count = int(minutes / bin_size)

        result = [0] * value_count

        for bin_data in query_result:
            result[bin_data[0] + value_count - 1] = bin_data[1]

        return result
