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

from geographiclib.geodesic import Geodesic
from geographiclib.polygonarea import PolygonArea

from .. import data


class StrikeCluster(object):
    """
    class for building strike cluster objects
    """

    def __init__(self):
        self.cluster_id = -1
        self.timestamp = None
        self.interval_seconds = 0
        self.shape = None
        self.strike_count = 0

    def with_id(self, cluster_id):
        self.cluster_id = cluster_id
        return self

    def with_timestamp(self, timestamp):
        self.timestamp = timestamp
        return self

    def with_interval_seconds(self, interval_seconds):
        self.interval_seconds = interval_seconds
        return self

    def with_shape(self, shape):
        self.shape = shape

        return self

    def with_strike_count(self, strike_count):
        self.strike_count = strike_count
        return self

    def build(self):
        if self.shape is not None:
            poly_area = PolygonArea(Geodesic.WGS84)
            if self.shape.coords:
                try:
                    for (x, y) in zip(self.shape.coords.xy[0], self.shape.coords.xy[1]):
                        poly_area.AddPoint(x, y)
                    area = round(poly_area.Compute(False, True)[2] / 1e6, 1)
                except ValueError:
                    area = None
            else:
                area = None
        else:
            area = None

        return data.StrikeCluster(self.cluster_id, self.timestamp, self.interval_seconds, self.shape, self.strike_count,
                                  area)
