from injector import inject
import time
import blitzortung.db.query_builder


class HistogramQuery(object):
    @inject(strike_query_builder=blitzortung.db.query_builder.Strike)
    def __init__(self, strike_query_builder):
        self.strike_query_builder = strike_query_builder

    def create(self, connection, minute_length, minute_offset, region=None):
        reference_time = time.time()
        query = self.strike_query_builder.histogram_query(blitzortung.db.table.Strike.TABLE_NAME, minute_length,
                                                          minute_offset, 5, region)
        histogram_query = connection.runQuery(str(query), query.get_parameters())
        histogram_query.addCallback(self.build_result, minutes=minute_length, bin_size=5,
                                    reference_time=reference_time)
        return histogram_query

    @staticmethod
    def build_result(query_result, minutes, bin_size, reference_time):
        time_duration = time.time() - reference_time
        print("histogram_query() %.03fs" % time_duration)
        value_count = int(minutes / bin_size)

        result = [0] * value_count

        for bin_data in query_result:
            result[bin_data[0] + value_count - 1] = bin_data[1]

        return result
