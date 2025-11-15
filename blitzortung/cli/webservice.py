import calendar
import collections
import datetime
import gc
import json
import os
import platform
import time

import statsd
from twisted.application import internet, service
from twisted.internet.defer import succeed
from twisted.internet.error import ReactorAlreadyInstalledError
from twisted.python import log
from twisted.python.log import FileLogObserver, ILogObserver, textFromEventDict, _safeFormat
from twisted.python.logfile import DailyLogFile
from twisted.python.util import untilConcludes
from twisted.web import server
from txjsonrpc_ng.web import jsonrpc
from txjsonrpc_ng.web.data import CacheableResult
from txjsonrpc_ng.web.jsonrpc import with_request

from blitzortung.gis.constants import grid, global_grid
from blitzortung.gis.local_grid import LocalGrid
from blitzortung.service.cache import ServiceCache
from blitzortung.util import TimeConstraint

try:
    from twisted.internet import epollreactor as reactor, defer
except ImportError:
    from twisted.internet import kqreactor as reactor

try:
    reactor.install()
except ReactorAlreadyInstalledError:
    pass

import blitzortung.cache
import blitzortung.config
import blitzortung.db
import blitzortung.geom
import blitzortung.service
from blitzortung.db.query import TimeInterval
from blitzortung.service.db import create_connection_pool
from blitzortung.service.general import create_time_interval
from blitzortung.service.strike_grid import GridParameters

is_pypy = platform.python_implementation() == 'PyPy'

statsd_client = statsd.StatsClient('localhost', 8125, prefix='org.blitzortung.service')

FORBIDDEN_IPS = {}

USER_AGENT_PREFIX = 'bo-android-'


class Blitzortung(jsonrpc.JSONRPC):
    """
    Blitzortung.org JSON-RPC webservice for lightning strike data.

    Provides endpoints for querying strike data, grid-based visualizations,
    and histograms with caching and rate limiting.
    """

    # Grid validation constants
    MIN_GRID_BASE_LENGTH = 5000
    INVALID_GRID_BASE_LENGTH = 1000001
    GLOBAL_MIN_GRID_BASE_LENGTH = 10000

    # Time validation constants
    MAX_MINUTES_PER_DAY = 24 * 60  # 1440 minutes
    DEFAULT_MINUTE_LENGTH = 60
    HISTOGRAM_MINUTE_THRESHOLD = 10

    # User agent validation constants
    MAX_COMPATIBLE_ANDROID_VERSION = 177

    # Memory info interval
    MEMORY_INFO_INTERVAL = 300  # 5 minutes

    def __init__(self, db_connection_pool, log_directory):
        super().__init__()
        self.connection_pool = db_connection_pool
        self.log_directory = log_directory
        self.strike_query = blitzortung.service.strike_query()
        self.strike_grid_query = blitzortung.service.strike_grid_query()
        self.global_strike_grid_query = blitzortung.service.global_strike_grid_query()
        self.histogram_query = blitzortung.service.histogram_query()
        self.check_count = 0
        self.cache = ServiceCache()
        self.current_period = self.__current_period()
        self.current_data = collections.defaultdict(list)
        self.next_memory_info = 0.0
        self.minute_constraints = TimeConstraint(self.DEFAULT_MINUTE_LENGTH, self.MAX_MINUTES_PER_DAY)

    addSlash = True

    def __get_epoch(self, timestamp):
        return calendar.timegm(timestamp.timetuple()) * 1000000 + timestamp.microsecond

    def __current_period(self):
        return datetime.datetime.now(datetime.UTC).replace(second=0, microsecond=0)

    def __check_period(self):
        if self.current_period != self.__current_period():
            self.current_data['timestamp'] = self.__get_epoch(self.current_period)
            if log_directory:
                with open(os.path.join(log_directory, self.current_period.strftime("%Y%m%d-%H%M.json")),
                          'w') as output_file:
                    output_file.write(json.dumps(self.current_data))
            self.__restart_period()

    def __restart_period(self):
        self.current_period = self.__current_period()
        self.current_data = collections.defaultdict(list)

    def __force_range(self, number, min_number, max_number):
        if number < min_number:
            return min_number
        elif number > max_number:
            return max_number
        else:
            return number
            # return max(min_number, min(max_number, number))

    def jsonrpc_check(self):
        self.check_count += 1
        return {'count': self.check_count}

    @with_request
    def jsonrpc_get_strikes(self, request, minute_length, id_or_offset=0):
        """This endpoint is currently blocked for all requests."""
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
                                                           statsd_client)

        histogram_result = self.get_histogram(time_interval, envelope=grid_parameters.grid) \
            if minute_length > self.HISTOGRAM_MINUTE_THRESHOLD else succeed([])

        combined_result = self.strike_grid_query.combine_result(grid_result, histogram_result, state)

        combined_result.addCallback(lambda value: CacheableResult(value))

        return combined_result

    def get_global_strikes_grid(self, minute_length, grid_baselength, minute_offset, count_threshold):
        grid_parameters = GridParameters(global_grid.get_for(grid_baselength), grid_baselength,
                                         count_threshold=count_threshold)
        time_interval = create_time_interval(minute_length, minute_offset)

        grid_result, state = self.global_strike_grid_query.create(grid_parameters, time_interval, self.connection_pool,
                                                                  statsd_client)

        histogram_result = self.get_histogram(
            time_interval) if minute_length > self.HISTOGRAM_MINUTE_THRESHOLD else succeed([])

        combined_result = self.strike_grid_query.combine_result(grid_result, histogram_result, state)

        combined_result.addCallback(lambda value: CacheableResult(value))

        return combined_result

    def get_local_strikes_grid(self, x, y, grid_baselength, minute_length, minute_offset, count_threshold, data_area=5):
        local_grid = LocalGrid(data_area=data_area, x=x, y=y)
        grid_factory = local_grid.get_grid_factory()
        grid_parameters = GridParameters(grid_factory.get_for(grid_baselength), grid_baselength,
                                         count_threshold=count_threshold)
        time_interval = create_time_interval(minute_length, minute_offset)

        grid_result, state = self.strike_grid_query.create(grid_parameters, time_interval, self.connection_pool,
                                                           statsd_client)

        histogram_result = self.get_histogram(time_interval, envelope=grid_parameters.grid) \
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
        client = self.get_request_client(request)
        user_agent, user_agent_version = self.parse_user_agent(request)

        if client in FORBIDDEN_IPS or user_agent_version == 0 or request.getHeader(
                'content-type') != 'text/json' or request.getHeader(
            'referer') == '' or grid_base_length < self.MIN_GRID_BASE_LENGTH or grid_base_length == self.INVALID_GRID_BASE_LENGTH:
            log.msg(
                f"FORBIDDEN - client: {client}, user agent: {user_agent_version}, content type: {request.getHeader('content-type')}, referer: {request.getHeader('referer')}")
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

        statsd_client.incr('strikes_grid.total_count')
        statsd_client.incr('global_strikes_grid.total_count')
        statsd_client.gauge('global_strikes_grid.cache_hits', cache.get_ratio())
        if minute_length == 10:
            statsd_client.incr('strikes_grid.bg_count')
            statsd_client.incr('global_strikes_grid.bg_count')

        return response

    @with_request
    def jsonrpc_get_local_strikes_grid(self, request, x, y, grid_base_length=10000, minute_length=60, minute_offset=0,
                                       count_threshold=0, data_area=5):
        self.memory_info()
        client = self.get_request_client(request)
        user_agent, user_agent_version = self.parse_user_agent(request)

        if client in FORBIDDEN_IPS or request.getHeader(
                'content-type') != 'text/json' or request.getHeader(
            'referer') == '' or grid_base_length < self.MIN_GRID_BASE_LENGTH or grid_base_length == self.INVALID_GRID_BASE_LENGTH:
            log.msg(
                f"FORBIDDEN - client: {client}, user agent: {user_agent_version}, content type: {request.getHeader('content-type')}, referer: {request.getHeader('referer')}")
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

        statsd_client.incr('strikes_grid.total_count')
        statsd_client.incr('local_strikes_grid.total_count')
        statsd_client.incr(f'local_strikes_grid.data_area.{data_area}')
        statsd_client.gauge('local_strikes_grid.cache_hits', cache.get_ratio())
        if minute_length == 10:
            statsd_client.incr('strikes_grid.bg_count')
            statsd_client.incr('local_strikes_grid.bg_count')

        return response

    @with_request
    def jsonrpc_get_strikes_grid(self, request, minute_length, grid_base_length=10000, minute_offset=0, region=1,
                                 count_threshold=0):
        self.memory_info()
        client = self.get_request_client(request)
        user_agent, user_agent_version = self.parse_user_agent(request)

        if client in FORBIDDEN_IPS or user_agent_version == 0 or request.getHeader(
                'content-type') != 'text/json' or request.getHeader(
            'referer') == '' or grid_base_length < self.MIN_GRID_BASE_LENGTH or grid_base_length == self.INVALID_GRID_BASE_LENGTH:
            log.msg(
                f"FORBIDDEN - client: {client}, user agent: {user_agent_version}, content type: {request.getHeader('content-type')}, referer: {request.getHeader('referer')}")
            log.msg('get_strikes_grid(%d, %d, %d, %d, >=%d) BLOCKED %.1f%% %s %s' % (
                minute_length, grid_base_length, minute_offset, region, count_threshold,
                0, client, user_agent))
            return {}

        original_grid_base_length = grid_base_length
        grid_base_length = max(self.MIN_GRID_BASE_LENGTH, grid_base_length)
        minute_length, minute_offset = self.minute_constraints.enforce(minute_length, minute_offset, )
        region = max(1, region)
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

        statsd_client.incr('strikes_grid.total_count')
        statsd_client.incr('strikes_grid.total_count.{}'.format(region))
        statsd_client.gauge('strikes_grid.cache_hits', cache.get_ratio())
        if minute_length == 10:
            statsd_client.incr('strikes_grid.bg_count')
            statsd_client.incr('strikes_grid.bg_count.{}'.format(region))

        return response

    def parse_user_agent(self, request):
        """Parse user agent string to extract version information."""
        user_agent = request.getHeader("User-Agent")
        if user_agent and user_agent.startswith(USER_AGENT_PREFIX):
            user_agent_parts = user_agent.split(' ')[0].rsplit('-', 1)
            version_string = user_agent_parts[1] if len(user_agent_parts) > 1 else None
            user_agent_version = int(version_string) if user_agent_parts[0] == 'bo-android' else 0
        else:
            user_agent_version = 0
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

    def get_histogram(self, time_interval: TimeInterval, region=None, envelope=None):
        return self.cache.histogram.get(self.histogram_query.create,
                                        time_interval=time_interval,
                                        connection_pool=self.connection_pool,
                                        region=region,
                                        envelope=envelope)

    def get_request_client(self, request):
        forward = request.getHeader("X-Forwarded-For")
        if forward:
            return forward.split(', ')[0]
        return request.getClientIP()

    def memory_info(self):
        now = time.time()
        if now > self.next_memory_info:
            log.msg("### MEMORY INFO ###")
            log.msg(gc.get_stats(True) if is_pypy else gc.get_stats())
            self.next_memory_info = now + self.MEMORY_INFO_INTERVAL


class LogObserver(FileLogObserver):

    def __init__(self, f, prefix=None):
        prefix = '' if prefix is None else prefix
        if len(prefix) > 0:
            prefix += ''
        self.prefix = prefix
        FileLogObserver.__init__(self, f)

    def emit(self, event_dict):
        text = textFromEventDict(event_dict)
        if text is None:
            return
        timeStr = self.formatTime(event_dict["time"])
        msgStr = _safeFormat("[%(prefix)s] %(text)s\n", {
            "prefix": self.prefix,
            "text": text.replace("\n", "\n\t")
        })
        untilConcludes(self.write, timeStr + " " + msgStr)
        untilConcludes(self.flush)


application = service.Application("Blitzortung.org JSON-RPC Server")

if os.environ.get('BLITZORTUNG_TEST'):
    import tempfile

    log_directory = tempfile.mkdtemp()
    print("LOG_DIR", log_directory)
else:
    log_directory = "/var/log/blitzortung"
if os.path.exists(log_directory):
    logfile = DailyLogFile("webservice.log", log_directory)
    application.setComponent(ILogObserver, LogObserver(logfile).emit)
else:
    log_directory = None


def start_server(connection_pool):
    print("Connection pool is ready")
    root = Blitzortung(connection_pool, log_directory)
    config = blitzortung.config.config()
    site = server.Site(root)
    site.displayTracebacks = False
    jsonrpc_server = internet.TCPServer(config.get_webservice_port(), site, interface='127.0.0.1')
    jsonrpc_server.setServiceParent(application)
    return jsonrpc_server


def on_error(failure):
    log.err(failure, "Failed to create connection pool")
    raise failure.value


deferred_connection_pool = create_connection_pool()
deferred_connection_pool.addCallback(start_server).addErrback(on_error)
