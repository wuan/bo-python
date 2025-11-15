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

def fetch_strikes_from_url(url, auth=None):
    """
    Fetch strike data from a given URL and parse it into Strike objects.

    The URL returns JSON-formatted strike data, one strike per line.
    Each line has the format:
    {"time":1763202124325980200,"lat":-15.296556,"lon":134.589548,"alt":0,"pol":0,...}

    Args:
        url: The URL to fetch strike data from
        auth: Optional tuple of (username, password) for authentication

    Yields:
        Strike objects parsed from the URL response
    """
    import json
    from blitzortung.builder import Strike as StrikeBuilder

    logger.info("Fetching strikes from URL: %s", url)

    try:
        response = requests.get(url, auth=auth, timeout=30)
        response.raise_for_status()

        builder = StrikeBuilder()

        strike_count = 0

        for line in response.text.splitlines():
            line = line.strip()
            if not line:
                continue

            try:
                # Parse JSON data
                data = json.loads(line)

                # Create strike from JSON data
                # Build strike object (create new builder for each strike)
                strike = (builder
                          .set_timestamp(Timestamp(data['time']))
                          .set_x(data['lon'])
                          .set_y(data['lat'])
                          .set_altitude(data.get('alt', 0))
                          .set_amplitude(data.get('pol', 0))
                          .build())

                strike_count += 1
                yield strike

            except (json.JSONDecodeError, KeyError) as e:
                logger.warning("Failed to parse strike: %s (%s)", e, line)
                continue
            except Exception as e:
                logger.warning("Failed to create strike object: %s (%s)", e, line)
                continue

        logger.info("Fetched %d strikes from URL", strike_count)

    except requests.RequestException as e:
        logger.error("Failed to fetch data from URL %s: %s", url, e)
        raise


def create_strike_key(strike):
    """
    Create a unique key for a strike based on its attributes.

    Since strikes from URLs don't have IDs, we identify them by:
    - timestamp (nanosecond precision)
    - location (x, y coordinates)
    - amplitude

    Args:
        strike: Strike object

    Returns:
        Tuple representing the strike's unique characteristics
    """
    # Get timestamp value (handle both Timestamp and datetime objects)
    if hasattr(strike.timestamp, 'value'):
        timestamp_value = strike.timestamp.value
    else:
        # For datetime objects, convert to nanoseconds since epoch
        timestamp_value = int(strike.timestamp.timestamp() * 1_000_000_000)

    return (
        timestamp_value,
        round(strike.x, 6),  # Round to 6 decimal places for location
        round(strike.y, 6),
        strike.amplitude
    )


def get_existing_strike_keys(strike_db, time_interval, region=None):
    """
    Retrieve keys of strikes already present in the database for a given time interval.

    Strikes are identified by their timestamp, location, and amplitude since
    strikes from URLs don't have database IDs.

    Args:
        strike_db: Database connection for strikes
        time_interval: Time interval to query
        region: Optional region filter

    Returns:
        Set of strike keys (tuples of timestamp, x, y, amplitude)
    """
    logger.debug("Querying existing strikes for interval %s - %s (region: %s)",
                 time_interval.start, time_interval.end, region)

    kwargs = {'time_interval': time_interval, 'order': 'timestamp'}
    if region is not None:
        kwargs['region'] = region

    existing_strikes = strike_db.select(**kwargs)
    strike_keys = {create_strike_key(strike) for strike in existing_strikes}

    logger.info("Found %d existing strikes in database", len(strike_keys))
    return strike_keys


def update_strikes(url=None, region=None, hours=1):
    """
    Update strike database by fetching data from a URL and inserting new strikes.

    This function:
    1. Calculates a time interval (default: last 1 hour)
    2. Retrieves existing strikes from the database for that interval
    3. Fetches strikes from the provided URL
    4. Inserts only strikes that are not already in the database

    Args:
        url: URL to fetch strike data from (if None, uses default config URL)
        region: Optional region to filter/tag strikes
        hours: Number of hours to look back (default: 1)

    Returns:
        Number of new strikes inserted into the database
    """
    logger.info("Starting strike update for region %s (looking back %d hour(s))", region, hours)

    # Get configuration if URL not provided
    config = blitzortung.config.config()
    if url is None:
        url = "https://data.blitzortung.org/Data/Protected/last_strikes.php"
        auth = (config.get_username(), config.get_password())
    else:
        auth = None

    # Calculate time interval (last N hours)
    now = datetime.datetime.now(datetime.timezone.utc)
    start_time = now - datetime.timedelta(hours=hours)
    end_time = now
    time_interval = blitzortung.db.query.TimeInterval(
        start_time,
        end_time
    )

    logger.info("Time interval: %s to %s", start_time, end_time)

    # Get database connection
    strike_db = blitzortung.db.strike()

    # Get existing strikes from database (identified by timestamp/location/amplitude)
    existing_strike_keys = get_existing_strike_keys(strike_db, time_interval, region)

    # Fetch strikes from URL
    try:
        url_strikes = list(fetch_strikes_from_url(url, auth=auth))
    except requests.RequestException as e:
        logger.error("Failed to fetch strikes from URL: %s", e)
        return 0

    # Filter strikes: only those within time interval and not in database
    new_strikes = []
    for strike in url_strikes:
        # Check if strike is within the time interval
        if not (time_interval.start <= strike.timestamp <= time_interval.end):
            logger.debug("Strike at %s outside time interval, skipping", strike.timestamp)
            continue

        # Check if strike already exists in database (by timestamp/location/amplitude)
        strike_key = create_strike_key(strike)
        if strike_key in existing_strike_keys:
            logger.debug("Strike at %s (%.6f, %.6f) already exists, skipping",
                        strike.timestamp, strike.x, strike.y)
            continue

        new_strikes.append(strike)

    logger.info("Found %d new strikes to insert (out of %d from URL)",
                len(new_strikes), len(url_strikes))

    # Insert new strikes
    insert_count = 0
    for strike in new_strikes:
        try:
            strike_db.insert(strike, region)
            insert_count += 1

            if insert_count % 1000 == 0:
                strike_db.commit()
                logger.info("Committed %d strikes so far", insert_count)

        except Exception as e:
            logger.error("Failed to insert strike %s: %s", strike.id, e)
            strike_db.rollback()
            raise

    # Final commit
    if insert_count > 0:
        strike_db.commit()
        logger.info("Successfully inserted %d new strikes", insert_count)
    else:
        logger.info("No new strikes to insert")

    strike_db.close()

    # Update statistics
    statsd_client.gauge("strikes.imported", insert_count)

    return insert_count





def main():
    """
    Command-line interface for the strike import tool.
    """
    parser = OptionParser(description="Import strike data from URL into database")
    parser.add_option("-u", "--url", dest="url", type="string", default=None,
                      help="URL to fetch strike data from (optional, uses default if not provided)")
    parser.add_option("-r", "--region", dest="region", type="int", default=None,
                      help="Region number (optional)")
    parser.add_option("--hours", dest="hours", type="int", default=1,
                      help="Number of hours to look back (default: 1)")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      help="Enable verbose logging")
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
                      help="Enable debug logging")
    parser.add_option("--no-lock", dest="no_lock", action="store_true",
                      help="Skip file locking (use with caution)")

    (options, args) = parser.parse_args()

    # Set logging level
    if options.debug:
        blitzortung.set_log_level(logging.DEBUG)
    elif options.verbose:
        blitzortung.set_log_level(logging.INFO)
    else:
        blitzortung.set_log_level(logging.WARNING)

    # Use lock unless disabled
    lock_context = nullcontext() if options.no_lock else LockWithTimeout('/tmp/.bo-import2.lock').locked(10)

    try:
        with lock_context:
            count = update_strikes(url=options.url, region=options.region, hours=options.hours)
            logger.info("Import completed: %d new strikes inserted", count)
            return 0

    except FailedToAcquireException:
        logger.warning("Could not acquire lock - another import may be running")
        return 1
    except Exception as e:
        logger.error("Import failed: %s", e, exc_info=options.debug)
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
