# -*- coding: utf8 -*-

"""
Copyright (C) 2013-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

from __future__ import print_function

import numpy as np
# import scipy.cluster
from scipy.spatial import ConvexHull

from shapely.geometry.polygon import LinearRing

import fastcluster
import six


class Clustering(object):
    distance_limit = 0.1

    def __init__(self, cluster_builder):
        self.cluster_builder = cluster_builder


    def build_clusters(self, events, time_interval):
        event_count = len(events)

        clusters = []
        if events:
            print("initialize data")
            clustered_points, points = self.initialize_clusters(event_count, events)

            print("calculate clusters")
            dist = fastcluster.pdist(points)
            results = fastcluster.linkage(dist)

            print("apply results")
            self.apply_results(results, clustered_points)

            print("reduced {} events to {} clusters".format(event_count, len(clustered_points)))

            self.cluster_builder \
                .with_start_time(time_interval.get_start()) \
                .with_end_time(time_interval.get_end())

            for clustered_strikes in self.get_clustered_strikes(event_count, clustered_points):
                cluster_points = len(clustered_strikes)
                if cluster_points > 2:
                    points = np.ndarray([cluster_points, 2])
                    for index, strike in enumerate(clustered_strikes):
                        points[index][0] = strike.get_x()
                        points[index][1] = strike.get_y()
                    hull = ConvexHull(points)

                    shape_points = []
                    for vertex in hull.vertices:
                        shape_points.append([points[vertex, 0], points[vertex, 1]])

                    shape = LinearRing(shape_points)

                    clusters.append(
                        self.cluster_builder
                        .with_strike_count(cluster_points)
                        .with_shape(shape)
                        .build())

        return clusters

    def initialize_clusters(self, event_count, events):
        points = np.ndarray([event_count, 2])
        clusters = {}
        for index, event in enumerate(events):
            clusters[index] = (event,)
            points[index][0] = event.get_x()
            points[index][1] = event.get_y()
        return clusters, points

    def apply_results(self, results, clusters):
        index = len(clusters)
        for result in results:

            index_1 = int(result[0])
            index_2 = int(result[1])
            distance = result[2]
            cluster_size = int(result[3])

            if distance > self.distance_limit:
                break

            print("{} + {} -> {} ({:.4f}, #{})".format(index_1, index_2, index, distance, cluster_size))
            clusters[index] = clusters[index_1] + clusters[index_2]
            del clusters[index_1]
            del clusters[index_2]

            assert cluster_size == len(clusters[index])

            index += 1

    def get_clustered_strikes(self, event_count, clusters):
        return tuple(value for key, value in six.iteritems(clusters) if key > event_count)

