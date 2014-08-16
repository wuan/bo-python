import time
import datetime
import pytz
import blitzortung


def create_time_interval(minute_length, minute_offset):
    end_time = datetime.datetime.utcnow()
    end_time = end_time.replace(tzinfo=pytz.UTC)
    end_time = end_time.replace(microsecond=0)
    end_time += datetime.timedelta(minutes=minute_offset)
    start_time = end_time - datetime.timedelta(minutes=minute_length)
    return blitzortung.db.query.TimeInterval(start_time, end_time)


class TimingState(object):
    def __init__(self, name, statsd_client):
        self.statsd_client = statsd_client

        self.reference_time = time.time()
        self.info_text = name + ": "

    def get_seconds(self, reference_time=None):
        return time.time() - (reference_time if reference_time else self.reference_time)

    def get_milliseconds(self, reference_time=None):
        return max(1, int(self.get_seconds(reference_time) * 1000))

    def reset_timer(self):
        self.reference_time = time.time()

    def log_timing(self, key, reference_time=None):
        self.statsd_client.timing(key, self.get_milliseconds(reference_time))

    def log_gauge(self, key, value):
        self.statsd_client.gauge(key, value)

    def log_incr(self, key):
        self.statsd_client.incr(key)

    def add_info_text(self, info_text):
        self.info_text += info_text

    def get_info_text(self):
        return self.info_text
