from unittest import TestCase
import datetime
from mockito import mock, when, verify
from hamcrest import assert_that, is_, instance_of, is_not, same_instance, contains
import time

import blitzortung


class TestClustering(TestCase):
    def setUp(self):
        pass
        

    def test_clustering(self):
        strokes_db = blitzortung.db.stroke()
        now = datetime.datetime.utcnow()
        start_time = now - datetime.timedelta(hours=2)
        time_interval = blitzortung.db.TimeInterval(start_time)
        strokes = strokes_db.select(time_interval)

        self.clustering = blitzortung.clustering.Clustering(strokes)

    def test_test(self):
        

