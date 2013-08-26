from unittest import TestCase
import datetime
import nose
import numpy as np
from mockito import mock, when, verify
from hamcrest import assert_that, is_, instance_of, is_not, same_instance, contains
import time

import blitzortung
import fastcluster
import scipy.cluster


class TestClustering(TestCase):
    def setUp(self):
        pass

    def test_clustering(self):
        raise nose.SkipTest("implement as an integration test later")

        strokes_db = blitzortung.db.stroke()
        now = datetime.datetime.utcnow()
        start_time = now - datetime.timedelta(hours=2)
        time_interval = blitzortung.db.TimeInterval(start_time)
        strokes = strokes_db.select(time_interval)

        self.clustering = blitzortung.clustering.Clustering(strokes)

    def test_basic_clustering(self):
        data = [
            [1.0, 2.0],
            [2.0, 1.0],
            [2.1, 1.1],
            [2, 1.1],
            [1.0, 2.1],
        ]
        data = np.array(data)

        dist = fastcluster.pdist(data)
        result = fastcluster.linkage(dist).tolist()

        print result

