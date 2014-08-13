import numpy as np
#import scipy.cluster
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
