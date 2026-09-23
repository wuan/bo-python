from typing import Optional

from statsd import StatsClient

STRIKES_GRID = 'strikes_grid'
GLOBAL_STRIKES_GRID = 'global_strikes_grid'
LOCAL_STRIKES_GRID = 'local_strikes_grid'
HISTOGRAM = 'histogram'
DB = 'db'

TOTAL_COUNT = 'total_count'
BG_COUNT = 'bg_count'
CACHE_HITS = 'cache_hits'
DATA_AREA = 'data_area'
SIZE = 'size'
POOL_WAIT = 'pool_wait'


class StatsDMetrics:

    def __init__(self, statsd: Optional[StatsClient] = None):
        self.statsd = statsd if statsd else StatsClient('localhost', 8125, prefix='org.blitzortung.service')

    def for_global_strikes(self, minute_length: int, cache_ratio: float) -> None:
        self.statsd.incr(self.name(STRIKES_GRID, TOTAL_COUNT))
        self.statsd.incr(self.name(GLOBAL_STRIKES_GRID, TOTAL_COUNT))
        self.statsd.gauge(self.name(GLOBAL_STRIKES_GRID, CACHE_HITS), cache_ratio)
        if minute_length == 10:
            self.statsd.incr(self.name(STRIKES_GRID, BG_COUNT))
            self.statsd.incr(self.name(GLOBAL_STRIKES_GRID, BG_COUNT))

    def for_local_strikes(self, minute_length: int, data_area: int, cache_ratio: float) -> None:
        self.statsd.incr(self.name(STRIKES_GRID, TOTAL_COUNT))
        self.statsd.incr(self.name(LOCAL_STRIKES_GRID, TOTAL_COUNT))
        self.statsd.incr(self.name(LOCAL_STRIKES_GRID, DATA_AREA, str(data_area)))
        self.statsd.gauge(self.name(LOCAL_STRIKES_GRID, CACHE_HITS), cache_ratio)
        if minute_length == 10:
            self.statsd.incr(self.name(STRIKES_GRID, BG_COUNT))
            self.statsd.incr(self.name(LOCAL_STRIKES_GRID, BG_COUNT))

    def for_strikes(self, minute_length: int, region: int, cache_ratio: float) -> None:
        self.statsd.incr(self.name(STRIKES_GRID, TOTAL_COUNT))
        self.statsd.incr(self.name(STRIKES_GRID, TOTAL_COUNT, str(region)))
        self.statsd.gauge(self.name(STRIKES_GRID, CACHE_HITS), cache_ratio)
        if minute_length == 10:
            self.statsd.incr(self.name(STRIKES_GRID, BG_COUNT))
            self.statsd.incr(self.name(STRIKES_GRID, BG_COUNT, str(region)))

    def for_histogram(self, cache_ratio: float, cache_size: int) -> None:
        """Report the hit ratio and entry count of the histogram cache."""
        self.statsd.gauge(self.name(HISTOGRAM, CACHE_HITS), cache_ratio)
        self.statsd.gauge(self.name(HISTOGRAM, SIZE), cache_size)

    def for_db_pool_wait(self, wait_seconds: float) -> None:
        """Report how long a query waited for a free pooled DB connection."""
        self.statsd.timing(self.name(DB, POOL_WAIT), max(1, int(wait_seconds * 1000)))

    @staticmethod
    def name(*args: str) -> str:
        return '.'.join(args)
