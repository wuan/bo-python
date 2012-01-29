# -*- coding: utf8 -*-

'''

@author: Andreas Würl

'''

import time

class Timer():
    def __init__(self):
        self.starttime = time.time()
        self.laptime = self.starttime

    def read(self):
        return time.time() - self.starttime

    def lap(self):
        now = time.time()
        lap = now - self.laptime
        self.laptime = now
        return lap

