# -*- coding: utf8 -*-

"""
Copyright (C) 2013-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import numpy as np
import fastcluster


class Clustering(object):
    def __init__(self, events):
        events = events[0:1000]
        coordinates = np.ndarray([len(events), 2])

        for index, event in enumerate(events):
            coordinates[index][0] = event.get_x()
            coordinates[index][1] = event.get_y()

        self.result = fastcluster.linkage(coordinates)
        self.clusters = [[value for value in cluster] for cluster in self.result]

        print(self.clusters)
