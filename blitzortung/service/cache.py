from ..cache import ObjectCache


class ServiceCache:
    CACHE_CLEANUP_PERIOD = 300  # 5 minutes
    CACHE_TTL_SHORT = 20  # seconds
    CACHE_TTL_LONG = 60  # seconds
    LOCAL_CACHE_SIZE_CURRENT = 100
    LOCAL_CACHE_SIZE_HISTORY = 400

    def __init__(self):
        self.__strikes_grid = ObjectCache(
            ttl_seconds=self.CACHE_TTL_SHORT, cleanup_period=self.CACHE_CLEANUP_PERIOD)
        self.__strikes_history_grid = ObjectCache(
            ttl_seconds=self.CACHE_TTL_LONG, cleanup_period=self.CACHE_CLEANUP_PERIOD)

        self.__global_strikes_grid = ObjectCache(
            ttl_seconds=self.CACHE_TTL_SHORT, cleanup_period=self.CACHE_CLEANUP_PERIOD)
        self.__global_strikes_history_grid = ObjectCache(
            ttl_seconds=self.CACHE_TTL_LONG, cleanup_period=self.CACHE_CLEANUP_PERIOD)

        self.__local_strikes_grid = ObjectCache(
            ttl_seconds=self.CACHE_TTL_SHORT, size=self.LOCAL_CACHE_SIZE_CURRENT,
            cleanup_period=self.CACHE_CLEANUP_PERIOD)
        self.__local_strikes_history_grid = ObjectCache(
            ttl_seconds=self.CACHE_TTL_LONG, size=self.LOCAL_CACHE_SIZE_HISTORY,
            cleanup_period=self.CACHE_CLEANUP_PERIOD)

        self.histogram = ObjectCache(
            ttl_seconds=self.CACHE_TTL_LONG, cleanup_period=self.CACHE_CLEANUP_PERIOD)

    def global_strikes(self, minute_offset):
        return self.__global_strikes_grid if minute_offset == 0 else self.__global_strikes_history_grid

    def local_strikes(self, minute_offset):
        return self.__local_strikes_grid if minute_offset == 0 else self.__local_strikes_history_grid

    def strikes(self, minute_offset):
        return self.__strikes_grid if minute_offset == 0 else self.__strikes_history_grid
