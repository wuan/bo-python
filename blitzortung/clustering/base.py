# -*- coding: utf8 -*-

"""
Copyright (C) 2013-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

from __future__ import print_function
import logging
from scipy.spatial.qhull import QhullError
import six

import numpy as np
from scipy.spatial import ConvexHull
from shapely.geometry.polygon import LinearRing
import fastcluster

from .pdist import pdist


class Clustering(object):
    distance_limit = 10
    coordinate_accuracy = 0.01
    coordinate_precision = 4

    def __init__(self, cluster_builder):
        self.cluster_builder = cluster_builder
        self.logger = logging.getLogger("{}.{}".format(__name__, self.__class__.__name__))

    def build_clusters(self, events, time_interval):
        events = list(events)
        event_count = len(events)

        cluster_count = 0

        if events:
            clustered_points, points = self.initialize_clusters(event_count, events)

            if len(points) < 3:
                return

            distances = pdist(points)
            results = fastcluster.linkage(distances)

            self.apply_results(results, clustered_points)

            self.cluster_builder \
                .with_timestamp(time_interval.get_end()) \
                .with_interval_seconds(time_interval.get_duration().seconds)

            for clustered_strikes in self.get_clustered_strikes(event_count, clustered_points):
                cluster_points = len(clustered_strikes)
                if cluster_points > 2:
                    points = np.ndarray([cluster_points, 2])
                    for index, strike in enumerate(clustered_strikes):
                        points[index][0] = strike.get_x()
                        points[index][1] = strike.get_y()
                    try:
                        hull = ConvexHull(points)
                    except QhullError:
                        self.logger.error("".join(str(points).splitlines()))
                        continue

                    shape_points = [
                        [
                            round(points[vertex, 0], self.coordinate_precision),
                            round(points[vertex, 1], self.coordinate_precision)
                        ] for vertex in hull.vertices]

                    shape = LinearRing(shape_points)
                    shape = shape.simplify(self.coordinate_accuracy, preserve_topology=False)

                    cluster_count += 1

                    yield self.cluster_builder \
                        .with_strike_count(cluster_points) \
                        .with_shape(shape).build()

            self.logger.debug("build_clusters({} +{}): {} events -> {} clusters -> {} filtered"
                             .format(time_interval.get_start(),
                                     time_interval.get_end() - time_interval.get_start(),
                                     event_count,
                                     len(clustered_points),
                                     cluster_count))

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

            # print("{} + {} -> {} ({:.4f}, #{})".format(index_1, index_2, index, distance, cluster_size))
            clusters[index] = clusters[index_1] + clusters[index_2]
            del clusters[index_1]
            del clusters[index_2]

            assert cluster_size == len(clusters[index])

            index += 1

    def get_clustered_strikes(self, event_count, clusters):
        return tuple(value for index, value in six.iteritems(clusters) if index > event_count)


