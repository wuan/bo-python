# -*- coding: utf8 -*-

"""

@author: Andreas Würl

"""

import time


class Timer():
    def __init__(self):
        self.start_time = time.time()
        self.lap_time = self.start_time

    def read(self):
        return time.time() - self.start_time

    def lap(self):
        now = time.time()
        lap = now - self.lap_time
        self.lap_time = now
        return lap

