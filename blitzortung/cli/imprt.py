# -*- coding: utf8 -*-

"""
   Copyright (C) 2011-2025 Andreas Würl

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

import datetime
import logging
import os
import time
from contextlib import nullcontext

import requests
import statsd
import stopit
from optparse import OptionParser

import blitzortung.dataimport
import blitzortung.db
import blitzortung.logger
from blitzortung import util
from blitzortung.data import Timestamp
from blitzortung.lock import LockWithTimeout, FailedToAcquireException

logger = logging.getLogger(os.path.basename(__file__))
blitzortung.set_parent_logger(logger)
blitzortung.add_log_handler(blitzortung.logger.create_console_handler())

statsd_client = statsd.StatsClient('localhost', 8125, prefix='org.blitzortung.import')


def timestamp_is_newer_than(timestamp, latest_time):
    if not latest_time:
        return True
    return timestamp and timestamp > latest_time and timestamp - latest_time != datetime.timedelta()


def import_strikes_for(region, start_time, is_update=False):
    logger.debug("work on region %d", region)
    strike_db = blitzortung.db.strike()
    latest_time_timer = util.Timer()
    latest_time = strike_db.get_latest_time(region)
    logger.debug("latest time for region %d: %s (%.03fs) ", region, latest_time, latest_time_timer.lap())
    if not latest_time:
        latest_time = start_time

    if is_update:
        start_time = update_start_time()
        if not latest_time or start_time > latest_time:
            latest_time = start_time

    reference_time = time.time()
    strike_source = blitzortung.dataimport.strikes()
    strikes = strike_source.get_strikes_since(latest_time, region=region)
    query_time = time.time()

    strike_group_size = 10000
    strike_count = 0
    global_start_time = start_time = time.time()
    for strike in strikes:
        strike_db.insert(strike, region)

        strike_count += 1
        if strike_count % strike_group_size == 0:
            strike_db.commit()
            logger.info("commit #{} ({:.1f}/s) @{} for region {}".format(
                strike_count, strike_group_size / (time.time() - start_time), strike.timestamp, region))
            start_time = time.time()

    if strike_count > 0:
        strike_db.commit()

    insert_time = time.time()
    stat_name = "strikes.%d" % region
    statsd_client.incr(stat_name)
    statsd_client.gauge(stat_name + ".count", strike_count)
    statsd_client.timing(stat_name + ".get", max(1, int((query_time - reference_time) * 1000)))
    statsd_client.timing(stat_name + ".insert", max(1, int((insert_time - query_time) * 1000)))

    logger.info("imported {} strikes ({:.1f}/s) for region {}".format(
        strike_count, strike_count / (time.time() - global_start_time), region))


def update_start_time() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=30)


def import_strikes(regions, start_time, no_timeout=False, is_update=False):

    error_count = 0
    for region in regions:
        for retry in range(5):
            try:
                with nullcontext() if no_timeout else stopit.SignalTimeout(300):
                    import_strikes_for(region, start_time, is_update=is_update)
                break
            except (requests.exceptions.ConnectionError, stopit.TimeoutException):
                logger.warning('import failed: retry {} region {}'.format(retry, region))
                error_count += 1
                time.sleep(2)
                continue
    statsd_client.gauge("strikes.error_count", error_count)


def main():
    parser = OptionParser()
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="verbose output")
    parser.add_option("-d", "--debug", dest="debug", action="store_true", help="debug output")
    parser.add_option("--no-timeout", dest="no_timeout", action="store_true", help="do not apply 5 minute timeout")
    parser.add_option("--startdate", dest="startdate", default=None, help="import start date")
    parser.add_option("--update", dest="update", action="store_true", help="run as regular update")

    (options, args) = parser.parse_args()

    lock = LockWithTimeout('/tmp/.bo-import.lock')

    try:
        with lock.locked(10):
            if options.debug:
                blitzortung.set_log_level(logging.DEBUG)
            elif options.verbose:
                blitzortung.set_log_level(logging.INFO)
            start_time = Timestamp(datetime.datetime.strptime(options.startdate, "%Y%m%d").replace(
                tzinfo=datetime.timezone.utc)) if options.startdate else None

            regions = [1, 2, 3, 4, 5, 6, 7, 10, 18, 19]
            import_strikes(regions=regions, start_time=start_time, no_timeout=options.no_timeout, is_update=options.update)
    except FailedToAcquireException:
        logger.warning("could not acquire lock")


if __name__ == "__main__":
    main()
