import cython

from libc.math cimport sin, cos, sqrt, asin
import numpy as np

cimport numpy as np

@cython.boundscheck(False)
@cython.wraparound(False)
def pdist(np.ndarray[double, ndim=2] data not None):
    cdef int number_of_points = len(data)
    cdef np.ndarray[double, ndim=1] distances = np.ndarray(choose(number_of_points, 2))
    cdef unsigned int i, j, index = 0
    for i in range(number_of_points - 1):
        for j in range(i + 1, number_of_points):
            distances[index] = distance(data[i, 0], data[i, 1], data[j, 0], data[j, 1])
            index += 1

    return distances

cdef unsigned int choose(unsigned int n, unsigned int k):
    """
    A fast way to calculate binomial coefficients by Andrew Dalke (contrib).
    """
    cdef unsigned int ntok = 1, ktok = 1, t

    if 0 <= k <= n:
        for t in range(1, min(k, n - k) + 1):
            ntok *= n
            ktok *= t
            n -= 1
        return ntok // ktok
    else:
        return 0

cdef double distance(double lambda_1, double phi_1, double lambda_2, double phi_2):
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

    cdef double delta_theta = 2 * asin(c/2)

    return 6371 * delta_theta
