# -*- coding: utf8 -*-

"""

   Copyright 2014-2016 Andreas Würl

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
import time

from .. import db


def create_time_interval(minute_length, minute_offset):
    end_time = datetime.datetime.now(datetime.timezone.utc)
    end_time = end_time.replace(microsecond=0)
    end_time += datetime.timedelta(minutes=minute_offset)
    start_time = end_time - datetime.timedelta(minutes=minute_length)
    return db.query.TimeInterval(start_time, end_time)


class TimingState:
    __slots__ = ['statsd_client', 'reference_time', 'name', 'info_text']

    def __init__(self, name, statsd_client):
        self.statsd_client = statsd_client

        self.reference_time = time.time()
        self.name = name
        self.info_text = []

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
        self.info_text += [info_text]
