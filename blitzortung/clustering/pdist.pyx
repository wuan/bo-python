# -*- coding: utf8 -*-

"""
Copyright (C) 2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import cython

from libc.math cimport sin, cos, sqrt, asin

cimport numpy as np

@cython.boundscheck(False)
@cython.wraparound(False)
def pdist(np.ndarray[double, ndim=2] data not None):
    cdef int number_of_points = len(data)
    cdef np.ndarray[double, ndim=1] distances = np.ndarray(choose(number_of_points, 2))
    cdef unsigned int i, j, index = 0
    for i in range(number_of_points - 1):
        for j in range(i + 1, number_of_points):
            distances[index] = distance_(data[i, 0], data[i, 1], data[j, 0], data[j, 1])
            index += 1

    return distances

cdef unsigned int choose(unsigned int n, unsigned int k):
    """ build numerator and denominator for fast calculation of the number of combinations
    """
    cdef unsigned int numerator = 1, denominator = 1
    cdef unsigned int ascending = 1, descending = n

    if 0 <= k <= n:
        for ascending in range(1, min(k, n - k) + 1):
            numerator *= descending
            denominator *= ascending
            descending -= 1
        return numerator // denominator
    else:
        return 0

def distance(lamba_1, phi_1, lambda_2, phi_2):
    return distance_(lamba_1, phi_1, lambda_2, phi_2)

cdef double distance_(double lambda_1, double phi_1, double lambda_2, double phi_2):
    cdef double to_rad = 3.1415 / 180.0
    cdef double cos_lambda_1 = cos(to_rad * lambda_1)
    cdef double sin_lambda_1 = sin(to_rad * lambda_1)
    cdef double cos_phi_1 = cos(to_rad * phi_1)
    cdef double sin_phi_1 = sin(to_rad * phi_1)
    cdef double cos_lambda_2 = cos(to_rad * lambda_2)
    cdef double sin_lambda_2 = sin(to_rad * lambda_2)
    cdef double cos_phi_2 = cos(to_rad * phi_2)
    cdef double sin_phi_2 = sin(to_rad * phi_2)

    cdef double delta_x = cos_phi_2 * cos_lambda_2 - cos_phi_1 * cos_lambda_1
    cdef double delta_y = cos_phi_2 * sin_lambda_2 - cos_phi_1 * sin_lambda_1
    cdef double delta_z = sin_phi_2 - sin_phi_1

    cdef double c = sqrt(delta_x * delta_x + delta_y * delta_y + delta_z * delta_z)

    cdef double delta_theta = 2 * asin(c / 2)

    return 6371 * delta_theta
