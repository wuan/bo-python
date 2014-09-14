# -*- coding: utf8 -*-

"""
Copyright (C) 2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

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

