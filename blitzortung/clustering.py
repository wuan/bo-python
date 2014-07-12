import bitarray
import numpy as np
import scipy.cluster
import fastcluster

import blitzortung
import blitzortung.geom


class Clustering(object):
    def __init__(self, events):
        events = events[0:1000]
        coordinates = np.ndarray([len(events), 2])

        for index, event in enumerate(events):
            coordinates[index][0] = event.get_x()
            coordinates[index][1] = event.get_y()

        self.result = scipy.cluster.hierarchy.linkage(coordinates)
        self.clusters = [[value for value in cluster] for cluster in self.result]

        print(self.clusters)


class ClusterContainer(blitzortung.geom.Grid):
    """ class for clustering via a 2-d-tree """

    def __init__(self, x_min, x_max, y_min, y_max, x_div, y_div, srid=blitzortung.geom.Geometry.DefaultSrid):
        super(ClusterContainer, self).__init__(x_min, x_max, y_min, y_max, x_div, y_div, srid)
        self.x_bits = self.get_bits_for_number(self.get_x_bin_count())
        self.y_bits = self.get_bits_for_number(self.get_x_bin_count())
        self.bits = (len(self.x_bits) + len(self.y_bits)) * bitarray.bitarray('0')
        self.cluster_map = {}

    def add_point(self, point):
        if not self.contains(point):
            return

        self.x_bits.setall(False)
        x_current_bits = self.get_number_as_bitarray(self.get_x_bin(point.get_x()))
        self.bitwise_add_to(self.x_bits, x_current_bits)

        self.y_bits.setall(False)
        y_current_bits = self.get_number_as_bitarray(self.get_y_bin(point.get_y()))
        self.bitwise_add_to(self.y_bits, y_current_bits)

    @staticmethod
    def get_bits_for_number(number):
        return number.bit_length() * bitarray.bitarray('0')

    @staticmethod
    def get_number_as_bitarray(number):
        return bitarray.bitarray(bin(number)[2:])

    @staticmethod
    def bitwise_add_to(target, source):
        target[-len(source)::1] = source
