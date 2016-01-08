# -*- coding: utf8 -*-

"""

   Copyright 2014-2016 Andreas Würl

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

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
    distance_limit = 8
    coordinate_accuracy = 0.01
    buffer_size = 0.02
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
                .with_timestamp(time_interval.end) \
                .with_interval_seconds(time_interval.duration.seconds)

            for clustered_events in self.extract_clustered_events(event_count, clustered_points):
                events_in_cluster = len(clustered_events)
                if events_in_cluster > 2:
                    points = np.ndarray([events_in_cluster, 2])
                    for index, strike in enumerate(clustered_events):
                        points[index][0] = strike.x
                        points[index][1] = strike.y
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
                    shape = shape.buffer(self.buffer_size)
                    shape = shape.simplify(self.coordinate_accuracy, preserve_topology=False)
                    shape = shape.exterior

                    if shape is None:
                        continue

                    x_values = np.round(shape.coords.xy[0], self.coordinate_precision)
                    y_values = np.round(shape.coords.xy[1], self.coordinate_precision)
                    shape = LinearRing(zip(x_values, y_values))

                    cluster_count += 1

                    yield self.cluster_builder \
                        .with_strike_count(events_in_cluster) \
                        .with_shape(shape).build()

            self.logger.debug("build_clusters({} +{}): {} events -> {} clusters -> {} filtered"
                              .format(time_interval.start,
                                      time_interval.duration,
                                      event_count,
                                      len(clustered_points),
                                      cluster_count))

    @staticmethod
    def initialize_clusters(event_count, events):
        points = np.ndarray([event_count, 2])
        clusters = {}
        for index, event in enumerate(events):
            clusters[index] = (event,)
            points[index][0] = event.x
            points[index][1] = event.y
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

    @staticmethod
    def extract_clustered_events(self, event_count, clusters):
        return tuple(value for index, value in six.iteritems(clusters) if index > event_count)
