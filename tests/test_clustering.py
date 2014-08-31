# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

from unittest import TestCase
import datetime
from hamcrest import assert_that, is_, close_to

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
import blitzortung.util


class TestClustering(TestCase):
    def setUp(self):
        pass

    def test_clustering(self):
        raise nose.SkipTest("create unittest later")

        print("retrieve strikes")
        strikes_db = blitzortung.db.strike()
        now = datetime.datetime.utcnow()
        end_time = now - datetime.timedelta(minutes=2)
        start_time = end_time - datetime.timedelta(minutes=10)
        time_interval = blitzortung.db.query.TimeInterval(start_time, end_time)
        strikes = strikes_db.select(time_interval)

        self.clustering = blitzortung.clustering.Clustering(blitzortung.builder.StrikeCluster())

        clusters = self.clustering.build_clusters(strikes, time_interval)

        print("{} clusters found".format(len(clusters)))

    def test_basic_clustering(self):
        if not fastcluster:
            raise nose.SkipTest("implement as an integration test later")
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

        assert_that(int(result[0][0]), is_(0))
        assert_that(int(result[0][1]), is_(4))
        assert_that(result[0][2], is_(close_to(0.1, 0.00001)))
        assert_that(int(result[0][3]), is_(2))

        assert_that(int(result[1][0]), is_(1))
        assert_that(int(result[1][1]), is_(3))
        assert_that(result[1][2], is_(close_to(0.1, 0.00001)))
        assert_that(int(result[1][3]), is_(2))

        assert_that(int(result[2][0]), is_(2))
        assert_that(int(result[2][1]), is_(6))
        assert_that(result[2][2], is_(close_to(0.1, 0.00001)))
        assert_that(int(result[2][3]), is_(3))

        assert_that(int(result[3][0]), is_(5))
        assert_that(int(result[3][1]), is_(7))
        assert_that(result[3][2], is_(close_to(1.34536, 0.00001)))
        assert_that(int(result[3][3]), is_(5))

