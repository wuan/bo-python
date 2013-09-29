# -*- coding: utf8 -*-

import os
import datetime
import pytz
import time


class Timer():
    """
    simple timer for repeated time measurements
    """
    def __init__(self):
        self.start_time = time.time()
        self.lap_time = self.start_time

    def read(self):
        """
        read time duration of current lap
        """
        return time.time() - self.start_time

    def lap(self):
        """
        start a new lap returning the total time of the currently active lap
        """
        now = time.time()
        lap = now - self.lap_time
        self.lap_time = now
        return lap


def total_seconds(time):
    """
    return the total seconds of the given time or datetime (relative to midnight)
    """

    if isinstance(time, datetime.datetime):
        return time.hour * 3600 + time.minute * 60 + time.second
    elif isinstance(time, datetime.timedelta):
        return time.seconds + time.days * 24 * 3600
    else:
        raise Exception("unhandled type " + type(time))


def round_time(time, duration):
    """
    round time to a given timedelta duration
    """
    duration_seconds = total_seconds(duration)
    seconds = (total_seconds(time) // duration_seconds) * duration_seconds
    return time.replace(
        hour=seconds // 3600,
        minute=seconds // 60 % 60,
        second=seconds % 60,
        microsecond=0
    )


def time_intervals(start_time, duration, end_time=None):
    """
    generator for time interval start times for a specified duration and end time

    if end time is not specified the current time is assumed
    """
    current_time = round_time(start_time, duration)
    if not end_time:
        end_time = datetime.datetime.utcnow()
        end_time = end_time.replace(tzinfo=pytz.UTC)
    end_time = round_time(end_time, duration)

    while current_time <= end_time:
        yield current_time
        current_time += duration


