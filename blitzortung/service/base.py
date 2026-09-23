"""Blitzortung webservice classes."""

import calendar
import collections
import datetime
import gc
import json
import os
import platform
import time
from typing import Any

from twisted.internet.defer import succeed
from twisted.python import log
from twisted.python.log import FileLogObserver, textFromEventDict, _safeFormat
from twisted.python.util import untilConcludes
from txjsonrpc_ng.web import jsonrpc
from txjsonrpc_ng.web.data import CacheableResult
from txjsonrpc_ng.web.jsonrpc import with_request

from blitzortung.gis.constants import grid, global_grid
from blitzortung.gis.local_grid import LocalGrid
from blitzortung.service.cache import ServiceCache
from blitzortung.service.metrics import StatsDMetrics
from blitzortung.util import TimeConstraint
import blitzortung.service
from blitzortung.db.query import TimeInterval
from blitzortung.service.db import DictConnectionPool
from blitzortung.service.general import create_time_interval
from blitzortung.service.strike_grid import GridParameters


JSON_CONTENT_TYPE = 'text/json'

is_pypy = platform.python_implementation() == 'PyPy'

FORBIDDEN_IPS: dict[str, Any] = {}

USER_AGENT_PREFIX = 'bo-android-'


class Blitzortung(jsonrpc.JSONRPC):
    """
    Blitzortung.org JSON-RPC webservice for lightning strike data.

    Provides endpoints for querying strike data, grid-based visualizations,
    and histograms with caching and rate limiting.
    """

    # Grid validation constants
    MIN_GRID_BASE_LENGTH = 5000
    GLOBAL_MIN_GRID_BASE_LENGTH = 25000
    VALID_GRID_BASE_LENGTHS = frozenset({5000, 10000, 25000, 50000, 100000})
    MAX_REGION = 7

    # Time validation constants
    MAX_MINUTES_PER_DAY = 24 * 60  # 1440 minutes
    DEFAULT_MINUTE_LENGTH = 60
    HISTOGRAM_MINUTE_THRESHOLD = 10

    # User agent validation constants
    MAX_COMPATIBLE_ANDROID_VERSION = 177

    # The Android client predates JSON-RPC 1.0: it sends a fixed request id of
    # 0 and no ``jsonrpc`` version field, and expects the pre-1.0 bare-array
    # response.  txjsonrpc-ng treats id 0 as spec-correct JSON-RPC 1.0 by
    # default, so opt in to the legacy envelope to keep deployed clients
    # working (requires the txjsonrpc-ng release providing this flag).
    treat_zero_id_as_pre1 = True

    # Memory info interval
    MEMORY_INFO_INTERVAL = 300  # 5 minutes

    def __init__(self, db_connection_pool=None, log_directory=None,
                 strike_query=None, strike_grid_query=None,
                 global_strike_grid_query=None, histogram_query=None,
                 cache=None, metrics=None, forbidden_ips=None):
        super().__init__()
        self.connection_pool = db_connection_pool
        self.log_directory = log_directory
        self.strike_query = strike_query if strike_query is not None else blitzortung.service.strike_query()
        self.strike_grid_query = strike_grid_query if strike_grid_query is not None else blitzortung.service.strike_grid_query()
        self.global_strike_grid_query = global_strike_grid_query if global_strike_grid_query is not None else blitzortung.service.global_strike_grid_query()
        self.histogram_query = histogram_query if histogram_query is not None else blitzortung.service.histogram_query()
        self.check_count = 0
        self.cache = cache if cache is not None else ServiceCache()
        self.current_period = self.__current_period()
        self.current_data: dict[str, Any] = collections.defaultdict(list)
        self.next_memory_info = 0.0
        self.minute_constraints = TimeConstraint(self.DEFAULT_MINUTE_LENGTH, self.MAX_MINUTES_PER_DAY)
        self.metrics = metrics if metrics is not None else StatsDMetrics()
        if isinstance(self.connection_pool, DictConnectionPool):
            self.connection_pool.wait_observer = self.metrics.for_db_pool_wait
        self.forbidden_ips = forbidden_ips if forbidden_ips is not None else FORBIDDEN_IPS

    addSlash = True

    def __get_epoch(self, timestamp):
        return calendar.timegm(timestamp.timetuple()) * 1000000 + timestamp.microsecond

    def __current_period(self):
        return datetime.datetime.now(datetime.UTC).replace(second=0, microsecond=0)

    def __check_period(self):
        if self.current_period != self.__current_period():
            self.current_data['timestamp'] = self.__get_epoch(self.current_period)
            if self.log_directory:
                with open(os.path.join(self.log_directory, self.current_period.strftime("%Y%m%d-%H%M.json")),
                          'w', encoding='utf-8') as output_file:
                    output_file.write(json.dumps(self.current_data))
            self.__restart_period()

    def __restart_period(self):
        self.current_period = self.__current_period()
        self.current_data = collections.defaultdict(list)

    @staticmethod
    def __force_range(number, min_number, max_number):
        if number < min_number:
            return min_number
        elif number > max_number:
            return max_number
        else:
            return number

    @staticmethod
    def __to_int(value):
        """Coerce a JSON-RPC argument to an int, returning ``None`` if invalid."""
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def jsonrpc_check(self):
        self.check_count += 1
        return {'count': self.check_count}

    @with_request
    def jsonrpc_get_strikes(self, request, minute_length, id_or_offset=0):
        """This endpoint is currently blocked for all requests."""
        minute_length = self.__to_int(minute_length)
        id_or_offset = self.__to_int(id_or_offset)
        if minute_length is None or id_or_offset is None:
            log.msg('get_strikes: invalid request arguments')
            return None

        minute_length = self.__force_range(minute_length, 0, self.MAX_MINUTES_PER_DAY)

        client = self.get_request_client(request)
        user_agent = request.getHeader("User-Agent")
        log.msg('get_strikes(%d, %d) %s %s BLOCKED' % (minute_length, id_or_offset, client, user_agent))
        return None

    def get_strikes_grid(self, minute_length, grid_baselength, minute_offset, region, count_threshold):
        grid_parameters = GridParameters(grid[region].get_for(grid_baselength), grid_baselength, region,
                                         count_threshold=count_threshold)
        time_interval = create_time_interval(minute_length, minute_offset)

        grid_result, state = self.strike_grid_query.create(grid_parameters, time_interval, self.connection_pool,
                                                           self.metrics.statsd)

        histogram_result = self.get_histogram(time_interval, envelope=grid_parameters.grid,
                                              cache_key=(minute_length, minute_offset, grid_parameters.grid)) \
            if minute_length > self.HISTOGRAM_MINUTE_THRESHOLD else succeed([])

        combined_result = self.strike_grid_query.combine_result(grid_result, histogram_result, state)

        combined_result.addCallback(lambda value: CacheableResult(value))

        return combined_result

    def get_global_strikes_grid(self, minute_length, grid_baselength, minute_offset, count_threshold):
        grid_parameters = GridParameters(global_grid.get_for(grid_baselength), grid_baselength,
                                         count_threshold=count_threshold)
        time_interval = create_time_interval(minute_length, minute_offset)

        grid_result, state = self.global_strike_grid_query.create(grid_parameters, time_interval, self.connection_pool,
                                                                  self.metrics.statsd)

        histogram_result = self.get_histogram(
            time_interval, cache_key=(minute_length, minute_offset)) \
            if minute_length > self.HISTOGRAM_MINUTE_THRESHOLD else succeed([])

        combined_result = self.global_strike_grid_query.combine_result(grid_result, histogram_result, state)

        combined_result.addCallback(lambda value: CacheableResult(value))

        return combined_result

    def get_local_strikes_grid(self, x, y, grid_baselength, minute_length, minute_offset, count_threshold, data_area=5):
        local_grid = LocalGrid(data_area=data_area, x=x, y=y)
        grid_factory = local_grid.get_grid_factory()
        grid_parameters = GridParameters(grid_factory.get_for(grid_baselength), grid_baselength,
                                         count_threshold=count_threshold)
        time_interval = create_time_interval(minute_length, minute_offset)

        grid_result, state = self.strike_grid_query.create(grid_parameters, time_interval, self.connection_pool,
                                                           self.metrics.statsd)

        histogram_result = self.get_histogram(time_interval, envelope=grid_parameters.grid,
                                              cache_key=(minute_length, minute_offset, grid_parameters.grid)) \
            if minute_length > self.HISTOGRAM_MINUTE_THRESHOLD else succeed([])

        combined_result = self.strike_grid_query.combine_result(grid_result, histogram_result, state)

        combined_result.addCallback(lambda value: CacheableResult(value))

        return combined_result

    @with_request
    def jsonrpc_get_strikes_raster(self, request, minute_length, grid_base_length=10000, minute_offset=0, region=1):
        return self.jsonrpc_get_strikes_grid(request, minute_length, grid_base_length, minute_offset, region)

    @with_request
    def jsonrpc_get_strokes_raster(self, request, minute_length, grid_base_length=10000, minute_offset=0, region=1):
        return self.jsonrpc_get_strikes_grid(request, minute_length, grid_base_length, minute_offset, region)

    @with_request
    def jsonrpc_get_global_strikes_grid(self, request, minute_length, grid_base_length=10000, minute_offset=0,
                                        count_threshold=0):
        self.memory_info()
        minute_length = self.__to_int(minute_length)
        grid_base_length = self.__to_int(grid_base_length)
        minute_offset = self.__to_int(minute_offset)
        count_threshold = self.__to_int(count_threshold)
        if (minute_length is None or grid_base_length is None or minute_offset is None
                or count_threshold is None):
            log.msg('get_global_strikes_grid: invalid request arguments')
            return {}

        client = self.get_request_client(request)
        user_agent, user_agent_version = self.parse_user_agent(request)

        if self.is_forbidden(request, client, user_agent_version, grid_base_length,
                             self.GLOBAL_MIN_GRID_BASE_LENGTH):
            log.msg('get_global_strikes_grid(%d, %d, %d, >=%d) BLOCKED %.1f%% %s %s' % (
                minute_length, grid_base_length, minute_offset, count_threshold,
                0, client, user_agent))
            return {}

        original_grid_base_length = grid_base_length
        grid_base_length = max(self.GLOBAL_MIN_GRID_BASE_LENGTH, grid_base_length)
        minute_length, minute_offset = self.minute_constraints.enforce(minute_length, minute_offset, )
        count_threshold = max(0, count_threshold)

        cache = self.cache.global_strikes(minute_offset)
        response = cache.get(self.get_global_strikes_grid, minute_length=minute_length,
                             grid_baselength=grid_base_length,
                             minute_offset=minute_offset,
                             count_threshold=count_threshold)
        self.fix_bad_accept_header(request, user_agent)

        log.msg('get_global_strikes_grid(%d, %d, %d, >=%d) %.1f%% %s %s' % (
            minute_length, grid_base_length, minute_offset, count_threshold,
            cache.get_ratio() * 100, client, user_agent))

        self.__check_period()
        self.current_data['get_strikes_grid'].append(
            (self.__get_epoch(datetime.datetime.now(datetime.UTC)), minute_length, original_grid_base_length,
             minute_offset,
             0, count_threshold, client, user_agent))

        self.metrics.for_global_strikes(minute_length, cache.get_ratio())

        return response

    @with_request
    def jsonrpc_get_local_strikes_grid(self, request, x, y, grid_base_length=10000, minute_length=60, minute_offset=0,
                                       count_threshold=0, data_area=5):
        self.memory_info()
        x = self.__to_int(x)
        y = self.__to_int(y)
        grid_base_length = self.__to_int(grid_base_length)
        minute_length = self.__to_int(minute_length)
        minute_offset = self.__to_int(minute_offset)
        count_threshold = self.__to_int(count_threshold)
        data_area = self.__to_int(data_area)
        if (x is None or y is None or grid_base_length is None or minute_length is None
                or minute_offset is None or count_threshold is None or data_area is None):
            log.msg('get_local_strikes_grid: invalid request arguments')
            return {}

        client = self.get_request_client(request)
        user_agent, user_agent_version = self.parse_user_agent(request)

        if self.is_forbidden(request, client, user_agent_version, grid_base_length,
                             self.MIN_GRID_BASE_LENGTH):
            log.msg('get_local_strikes_grid(%d, %d, %d, %d, %d, >=%d, %d) BLOCKED %.1f%% %s %s' % (
                x, y, grid_base_length, minute_length, minute_offset, count_threshold, data_area,
                0, client, user_agent))
            return {}

        original_grid_base_length = grid_base_length
        grid_base_length = max(self.MIN_GRID_BASE_LENGTH, grid_base_length)
        minute_length, minute_offset = self.minute_constraints.enforce(minute_length, minute_offset, )
        count_threshold = max(0, count_threshold)
        data_area = round(max(5, data_area))

        cache = self.cache.local_strikes(minute_offset)
        response = cache.get(self.get_local_strikes_grid, x=x, y=y,
                             grid_baselength=grid_base_length,
                             minute_length=minute_length,
                             minute_offset=minute_offset,
                             count_threshold=count_threshold,
                             data_area=data_area)

        log.msg('get_local_strikes_grid(%d, %d, %d, %d, %d, >=%d, %d) %.1f%% %d# %s %s' % (
            x, y, minute_length, grid_base_length, minute_offset, count_threshold, data_area,
            cache.get_ratio() * 100, cache.get_size(), client,
            user_agent))

        self.__check_period()
        self.current_data['get_strikes_grid'].append(
            (
                self.__get_epoch(datetime.datetime.now(datetime.UTC)), minute_length, original_grid_base_length,
                minute_offset,
                -1, count_threshold, client, user_agent, x, y, data_area))

        self.metrics.for_local_strikes(minute_length, data_area, cache.get_ratio())

        return response

    @with_request
    def jsonrpc_get_strikes_grid(self, request, minute_length, grid_base_length=10000, minute_offset=0, region=1,
                                 count_threshold=0):
        self.memory_info()
        minute_length = self.__to_int(minute_length)
        grid_base_length = self.__to_int(grid_base_length)
        minute_offset = self.__to_int(minute_offset)
        region = self.__to_int(region)
        count_threshold = self.__to_int(count_threshold)
        if (minute_length is None or grid_base_length is None or minute_offset is None
                or region is None or count_threshold is None):
            log.msg('get_strikes_grid: invalid request arguments')
            return {}

        client = self.get_request_client(request)
        user_agent, user_agent_version = self.parse_user_agent(request)

        if self.is_forbidden(request, client, user_agent_version, grid_base_length,
                             self.MIN_GRID_BASE_LENGTH):
            log.msg('get_strikes_grid(%d, %d, %d, %d, >=%d) BLOCKED %.1f%% %s %s' % (
                minute_length, grid_base_length, minute_offset, region, count_threshold,
                0, client, user_agent))
            return {}

        original_grid_base_length = grid_base_length
        grid_base_length = max(self.MIN_GRID_BASE_LENGTH, grid_base_length)
        minute_length, minute_offset = self.minute_constraints.enforce(minute_length, minute_offset, )
        region = self.__force_range(region, 1, self.MAX_REGION)
        count_threshold = max(0, count_threshold)

        cache = self.cache.strikes(minute_offset)
        response = cache.get(self.get_strikes_grid, minute_length=minute_length,
                             grid_baselength=grid_base_length,
                             minute_offset=minute_offset, region=region,
                             count_threshold=count_threshold)
        self.fix_bad_accept_header(request, user_agent)

        log.msg('get_strikes_grid(%d, %d, %d, %d, >=%d) %.1f%% %s %s' % (
            minute_length, grid_base_length, minute_offset, region, count_threshold,
            cache.get_ratio() * 100, client, user_agent))

        self.__check_period()
        self.current_data['get_strikes_grid'].append(
            (self.__get_epoch(datetime.datetime.now(datetime.UTC)), minute_length, original_grid_base_length,
             minute_offset,
             region,
             count_threshold, client, user_agent))

        self.metrics.for_strikes(minute_length, region, cache.get_ratio())

        return response

    def is_forbidden(self, request, client, user_agent_version, grid_base_length, min_grid_base_length):
        """Return ``True`` when a data request violates the access limits.

        A request is forbidden when the client IP is blocked, the user agent is
        not a valid ``bo-android-<int>`` client, the content type is not
        ``text/json``, a referer is set, or the grid baseline is below the
        endpoint's minimum or not one of the supported sizes.
        """
        content_type = request.getHeader('content-type')
        referer = request.getHeader('referer')
        if (client in self.forbidden_ips
                or user_agent_version == 0
                or content_type != JSON_CONTENT_TYPE
                or referer
                or grid_base_length < min_grid_base_length
                or grid_base_length not in self.VALID_GRID_BASE_LENGTHS):
            log.msg(
                f"FORBIDDEN - client: {client}, user agent: {user_agent_version}, "
                f"content type: {content_type}, referer: {referer}")
            return True
        return False

    def parse_user_agent(self, request):
        """Parse user agent string to extract version information."""
        user_agent = request.getHeader("User-Agent")
        user_agent_version = 0
        if user_agent and user_agent.startswith(USER_AGENT_PREFIX):
            user_agent_parts = user_agent.split(' ')[0].rsplit('-', 1)
            if len(user_agent_parts) > 1 and user_agent_parts[0] == 'bo-android':
                try:
                    user_agent_version = int(user_agent_parts[1])
                except ValueError:
                    pass
        return user_agent, user_agent_version

    def fix_bad_accept_header(self, request, user_agent):
        """Remove Accept-Encoding header for old Android client versions that have bugs."""
        if user_agent and user_agent.startswith(USER_AGENT_PREFIX):
            user_agent_parts = user_agent.split(' ')[0].rsplit('-', 1)
            if len(user_agent_parts) > 1 and user_agent_parts[0] == 'bo-android':
                try:
                    version = int(user_agent_parts[1])
                    if version <= self.MAX_COMPATIBLE_ANDROID_VERSION:
                        request.requestHeaders.removeHeader("Accept-Encoding")
                except ValueError:
                    pass

    def get_histogram(self, time_interval: TimeInterval, region=None, envelope=None, cache_key=None):
        result = self.cache.histogram.get(self.histogram_query.create,
                                          time_interval=time_interval,
                                          connection_pool=self.connection_pool,
                                          region=region,
                                          envelope=envelope,
                                          cache_key=cache_key)
        self.metrics.for_histogram(self.cache.histogram.get_ratio(), self.cache.histogram.get_size())
        return result

    def get_request_client(self, request):
        forward = request.getHeader("X-Forwarded-For")
        if forward:
            return forward.split(',')[0].strip()
        return request.getClientIP()

    def memory_info(self):
        now = time.time()
        if now > self.next_memory_info:
            log.msg("### MEMORY INFO ###")
            # pylint: disable=no-member
            if is_pypy:
                log.msg(gc.get_stats(True))  # type: ignore[call-arg]
            else:
                log.msg(gc.get_stats())
            self.next_memory_info = now + self.MEMORY_INFO_INTERVAL


class LogObserver(FileLogObserver):

    def __init__(self, f, prefix=None):
        prefix = '' if prefix is None else prefix
        self.prefix = prefix
        FileLogObserver.__init__(self, f)

    def emit(self, event_dict):  # pyright: ignore[reportIncompatibleMethodOverride]
        # The parent class uses the camelCase name ``eventDict``; the override
        # keeps the snake_case name Sonar expects while remaining compatible.
        # pylint: disable=arguments-renamed
        text = textFromEventDict(event_dict)
        if text is None:
            return
        time_str = self.formatTime(event_dict["time"])
        msg_str = _safeFormat("[%(prefix)s] %(text)s\n", {
            "prefix": self.prefix,
            "text": text.replace("\n", "\n\t")
        })
        untilConcludes(self.write, time_str + " " + msg_str)
        untilConcludes(self.flush)
