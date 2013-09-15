from unittest import TestCase
import datetime

import nose
import numpy as np
import fastcluster

import blitzortung


class TestClustering(TestCase):
    def setUp(self):
        pass

    def test_clustering(self):
        raise nose.SkipTest("implement as an integration test later")

        strokes_db = blitzortung.db.db.stroke()
        now = datetime.datetime.utcnow()
        start_time = now - datetime.timedelta(hours=2)
        time_interval = blitzortung.db.db.TimeInterval(start_time)
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

