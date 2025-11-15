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
from contextlib import nullcontext

import requests
import statsd
from optparse import OptionParser

import blitzortung.config
import blitzortung.db
import blitzortung.db.query
import blitzortung.logger
from blitzortung import util
from blitzortung.data import Timestamp
from blitzortung.lock import LockWithTimeout, FailedToAcquireException

logger = logging.getLogger(os.path.basename(__file__))
blitzortung.set_parent_logger(logger)
blitzortung.add_log_handler(blitzortung.logger.create_console_handler())

statsd_client = statsd.StatsClient('localhost', 8125, prefix='org.blitzortung.import')


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

    try:
        timer = util.Timer()
        response = requests.get(url, auth=auth, timeout=30)
        response.raise_for_status()

        logger.info("Fetching strikes from URL: %s (%.03fs)", url, timer.lap())

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
                          .set_x(round(data['lon'], 4))
                          .set_y(round(data['lat'], 4))
                          .set_altitude(data.get('alt', 0))
                          .set_amplitude(data.get('pol', 0))
                          .set_lateral_error(data.get('mds', 0))
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
    - lateral error

    Args:
        strike: Strike object

    Returns:
        Tuple representing the strike's unique characteristics
    """
    return (
        strike.timestamp.value,
        round(strike.x, 4),  # Round to 4 decimal places for location
        round(strike.y, 4),
        strike.lateral_error
    )


def get_existing_strike_keys(strike_db, time_interval):
    """
    Retrieve keys of strikes already present in the database for a given time interval.

    Strikes are identified by their timestamp, location, and amplitude since
    strikes from URLs don't have database IDs.

    Args:
        strike_db: Database connection for strikes
        time_interval: Time interval to query

    Returns:
        Set of strike keys (tuples of timestamp, x, y, amplitude)
    """
    logger.debug("Querying existing strikes for interval %s - %s",
                 time_interval.start, time_interval.end)

    existing_strikes = strike_db.select(time_interval=time_interval, order="timestamp")
    strike_keys = {create_strike_key(strike) for strike in existing_strikes}

    logger.info("Found %d existing strikes in database", len(strike_keys))
    return strike_keys


def update_strikes(hours=1):
    """
    Update strike database by fetching data from a URL and inserting new strikes.

    This function:
    1. Calculates a time interval (default: last 1 hour)
    2. Retrieves existing strikes from the database for that interval
    3. Fetches strikes from the provided URL
    4. Inserts only strikes that are not already in the database

    Args:
        url: URL to fetch strike data from (if None, uses default config URL)
        hours: Number of hours to look back (default: 1)

    Returns:
        Number of new strikes inserted into the database
    """
    logger.info("Starting strike update (looking back %d hour(s))", hours)

    now = datetime.datetime.now(datetime.timezone.utc)
    start_time = now - datetime.timedelta(hours=hours)

    # Get configuration if URL not provided
    config = blitzortung.config.config()
    start_timestamp_ns = int(start_time.timestamp() * 1e6) * 1000
    url = f"https://data.blitzortung.org/Data/Protected/last_strikes.php?time={start_timestamp_ns}"
    auth = (config.get_username(), config.get_password())

    # Calculate time interval (last N hours)
    end_time = now
    time_interval = blitzortung.db.query.TimeInterval(
        start_time,
        end_time
    )

    logger.info("Time interval: %s to %s", start_time, end_time)

    # Get database connection
    strike_db = blitzortung.db.strike()

    # Get existing strikes from database (identified by timestamp/location/amplitude)
    existing_strike_keys = get_existing_strike_keys(strike_db, time_interval)
    for existing_strike_key in existing_strike_keys:
        logger.debug("Existing strike key: %s", existing_strike_key)

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
            logger.debug("Strike %s at %s (%.4f, %.4f) already exists, skipping", str(strike_key),
                         strike.timestamp, strike.x, strike.y)
            continue

        if strike.timestamp < now - datetime.timedelta(minutes=1):
            logger.debug("Strike %s at %s (%.4f, %.4f) new", str(strike_key),
                     strike.timestamp, strike.x, strike.y)
            new_strikes.append(strike)
        else:
            logger.debug("Strike %s at %s (%.4f, %.4f) too new", str(strike_key),
                         strike.timestamp, strike.x, strike.y)


    logger.info("Found %d new strikes to insert (out of %d from URL)",
                len(new_strikes), len(url_strikes))

    # Insert new strikes
    insert_count = 0
    for strike in new_strikes:
        try:
            strike_db.insert(strike)
            insert_count += 1

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
            count = update_strikes(hours=options.hours)
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
