# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

from unittest import TestCase
import datetime
from assertpy import assert_that

import nose
import numpy as np

try:
    import fastcluster
except ImportError:
    raise nose.SkipTest("module fastcluster not available")

import blitzortung.builder
import blitzortung.db
import blitzortung.db.query
import blitzortung.clustering
import blitzortung.clustering.pdist
import blitzortung.util


class TestClustering(TestCase):
    def setUp(self):
        self.clustering = blitzortung.clustering.Clustering(blitzortung.builder.StrikeCluster())

    def test_clustering(self):
        event_builder = blitzortung.builder.Event()

        events = [
            event_builder.set_x(11).set_y(51).build(),
            event_builder.set_x(11.02).set_y(51.02).build(),
            event_builder.set_x(11.02).set_y(51.05).build(),
            event_builder.set_x(11.4).set_y(51.4).build(),
            event_builder.set_x(12).set_y(52).build()
        ]
        now = datetime.datetime.utcnow()
        time_interval = blitzortung.db.query.TimeInterval(now - datetime.timedelta(minutes=10), now)

        clusters = list(self.clustering.build_clusters(events, time_interval))

        assert_that(len(clusters)).is_equal_to(1)
        cluster = clusters[0]
        assert_that(cluster.timestamp).is_equal_to(time_interval.end)
        assert_that(cluster.interval_seconds).is_equal_to(10 * 60)
        shape = cluster.shape
        xy_arrays = shape.coords.xy
        assert_that(xy_arrays[0].tolist()).contains(11.0015, 11.034, 11.04, 11.0001, 10.98, 11.0015)
        assert_that(xy_arrays[1].tolist()).contains(51.0575, 51.0643, 51.02, 50.98, 50.9998, 51.0575)
        assert_that(cluster.strike_count).is_equal_to(3)

    def test_clustering_with_not_enough_events(self):
        event_builder = blitzortung.builder.Event()

        events = [
            event_builder.set_x(11).set_y(51).build(),
            event_builder.set_x(11.02).set_y(51.02).build(),
        ]
        now = datetime.datetime.utcnow()
        time_interval = blitzortung.db.query.TimeInterval(now - datetime.timedelta(minutes=10), now)

        clusters = list(self.clustering.build_clusters(events, time_interval))
        assert_that(clusters).is_empty()
        
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

        assert_that(int(result[0][0])).is_equal_to(0)
        assert_that(int(result[0][1])).is_equal_to(4)
        assert_that(result[0][2]).is_close_to(0.1, 0.00001)
        assert_that(int(result[0][3])).is_equal_to(2)

        assert_that(int(result[1][0])).is_equal_to(1)
        assert_that(int(result[1][1])).is_equal_to(3)
        assert_that(result[1][2]).is_close_to(0.1, 0.00001)
        assert_that(int(result[1][3])).is_equal_to(2)

        assert_that(int(result[2][0])).is_equal_to(2)
        assert_that(int(result[2][1])).is_equal_to(6)
        assert_that(result[2][2]).is_close_to(0.1, 0.00001)
        assert_that(int(result[2][3])).is_equal_to(3)

        assert_that(int(result[3][0])).is_equal_to(5)
        assert_that(int(result[3][1])).is_equal_to(7)
        assert_that(result[3][2]).is_close_to(1.34536, 0.00001)
        assert_that(int(result[3][3])).is_equal_to(5)


class TestPdist(TestCase):
    def test_distance(self):
        distance = blitzortung.clustering.distance(11, 51, 11.1, 51.1)

        assert_that(distance).is_close_to(13.133874300397196, 1e-9)
