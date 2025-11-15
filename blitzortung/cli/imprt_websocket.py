import json
import logging
import os
import random
import threading
import time
from optparse import OptionParser

import statsd
import websocket

import blitzortung.builder
import blitzortung.data
import blitzortung.logger
from websocket import WebSocketConnectionClosedException

from blitzortung.lock import LockWithTimeout, FailedToAcquireException
from blitzortung.websocket import decode

logger = logging.getLogger(os.path.basename(__file__))
blitzortung.set_parent_logger(logger)
blitzortung.add_log_handler(blitzortung.logger.create_console_handler())

strike_builder = blitzortung.builder.Strike()
strike_db = None

statsd_client = statsd.StatsClient('localhost', 8125, prefix='org.blitzortung.import')

strike_db = None

strike_count = 0
last_commit_time = time.time()

try:
    import thread
except ImportError:
    import _thread as thread


def on_message(ws, message):
    global strike_db
    message = decode(message if isinstance(message, str) else message.decode('utf-8'))
    logger.debug("message: %s", message)

    global strike_count, last_commit_time

    data = json.loads(message)

    timestamp = blitzortung.data.Timestamp.from_nanoseconds(data['time'])
    strike = strike_builder \
        .set_altitude(data['alt']) \
        .set_x(round(data['lon'],4)) \
        .set_y(round(data['lat'],4)) \
        .set_timestamp(*timestamp) \
        .set_lateral_error(data['mds']) \
        .build()
    delay = data['delay']
    local_time = time.time()
    local_delay = local_time - timestamp[0].timestamp()
    logger.info("%s - region %d - delay %.1f, local delay %.1f", strike, data['region'], delay, local_delay)

    if strike_db:
        strike_db.insert(strike, data['region'])
    strike_count += 1

    stat_name = "strikes"
    statsd_client.incr(stat_name)
    statsd_client.gauge(f"{stat_name}.delay", local_delay)

    if strike_count > 100 or (strike_count > 0 and time.time() > last_commit_time + 5):
        logger.info("commit #%d", strike_count)
        if strike_db:
            strike_db.commit()
        strike_count = 0
        last_commit_time = time.time()


def on_error(we, error):
    logger.warning("error '%s'", str(error))


def on_close(ws, close_status_code, close_msg):
    close_status_code = close_status_code if close_status_code else 0
    close_msg = close_msg if close_msg else 'n/a'
    logger.info("### closed ### %s:%d", close_msg, close_status_code)


def on_open(ws):
    initialization = '{"a":111}'
    logger.info(initialization)
    ws.send(initialization)

    def run(*args):
        while True:
            time.sleep(30)
            try:
                ws.send("{}")
            except WebSocketConnectionClosedException:
                logger.info("refresher exiting")
                return
            logger.info("sent refresh")

    threading.Thread(target=run).start()


def main():
    global strike_db
    parser = OptionParser()
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="verbose output")
    parser.add_option("-d", "--debug", dest="debug", action="store_true", help="debug output")
    parser.add_option("-t", "--test", dest="test", action="store_true", help="test connection only")

    (options, args) = parser.parse_args()

    if options.debug:
        blitzortung.set_log_level(logging.DEBUG)
        websocket.enableTrace(True)
    elif options.verbose:
        blitzortung.set_log_level(logging.INFO)

    lock = LockWithTimeout('/tmp/.bo-import-websocket.lock')

    try:
        with lock.locked(10):
            if not options.test:
                strike_db = blitzortung.db.strike()

            while True:
                server_index = random.choices([1, 7, 8])[0]
                url = f"wss://ws{server_index}.blitzortung.org/"
                logger.info(f"connect to {url}")
                ws = websocket.WebSocketApp(
                    url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                ws.run_forever(origin='https://www.blitzortung.org', skip_utf8_validation=True)
                logger.info("finished")
    except FailedToAcquireException:
        logger.warning("could not acquire lock")

if __name__ == "__main__":
    main()
