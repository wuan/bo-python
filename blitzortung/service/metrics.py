from typing import Optional

from statsd import StatsClient


class StatsDMetrics:

    def __init__(self, statsd: Optional[StatsClient]= None):
        self.statsd = statsd if statsd else StatsClient('localhost', 8125, prefix='org.blitzortung.service')

    def for_global_strikes(self, minute_length: int, cache_ratio: float):
        self.statsd.incr('strikes_grid.total_count')
        self.statsd.incr('global_strikes_grid.total_count')
        self.statsd.gauge('global_strikes_grid.cache_hits', cache_ratio)
        if minute_length == 10:
            self.statsd.incr('strikes_grid.bg_count')
            self.statsd.incr('global_strikes_grid.bg_count')

    def for_local_strikes(self, minute_length: int, data_area, cache_ratio: float):
        self.statsd.incr('strikes_grid.total_count')
        self.statsd.incr('local_strikes_grid.total_count')
        self.statsd.incr(f'local_strikes_grid.data_area.{data_area}')
        self.statsd.gauge('local_strikes_grid.cache_hits', cache_ratio)
        if minute_length == 10:
            self.statsd.incr('strikes_grid.bg_count')
            self.statsd.incr('local_strikes_grid.bg_count')

    def for_strikes(self, minute_length:int, region: int, cache_ratio: float):
        self.statsd.incr('strikes_grid.total_count')
        self.statsd.incr('strikes_grid.total_count.{}'.format(region))
        self.statsd.gauge('strikes_grid.cache_hits', cache_ratio)
        if minute_length == 10:
            self.statsd.incr('strikes_grid.bg_count')
            self.statsd.incr('strikes_grid.bg_count.{}'.format(region))
