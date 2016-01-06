# -*- coding: utf8 -*-

"""
Copyright (C) 2012-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

from injector import inject
import time
import blitzortung.db.query_builder


class HistogramQuery(object):
    @inject(strike_query_builder=blitzortung.db.query_builder.Strike)
    def __init__(self, strike_query_builder):
        self.strike_query_builder = strike_query_builder

    def create(self, connection, minute_length, minute_offset, region=None, envelope=None, count_threshold=0):
        reference_time = time.time()
        query = self.strike_query_builder.histogram_query(blitzortung.db.table.Strike.TABLE_NAME, minute_length,
                                                          minute_offset, 5, region, envelope)
        histogram_query = connection.runQuery(str(query), query.get_parameters())
        histogram_query.addCallback(self.build_result, minutes=minute_length, bin_size=5,
                                    reference_time=reference_time)
        return histogram_query

    @staticmethod
    def build_result(query_result, minutes, bin_size, reference_time):
        time_duration = time.time() - reference_time
        print("histogram: query %.03fs" % time_duration)
        value_count = int(minutes / bin_size)

        result = [0] * value_count

        for bin_data in query_result:
            result[bin_data[0] + value_count - 1] = bin_data[1]

        return result
