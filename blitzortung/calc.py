# -*- coding: utf8 -*-

"""
Copyright (C) 2012-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import math
import datetime
import itertools
import collections
import numpy as np
import pandas as pd
from injector import singleton

from blitzortung import data, builder, types


@singleton
class SignalVelocity(object):
    """
    class for time/distance conversion regarding the reduced speed of light
    """

    # speed of light in m / ns
    __c0 = 0.299792458

    __c_reduction_permille = 2.5

    __c = (1 - 0.001 * __c_reduction_permille) * __c0

    def get_distance_time(self, distance):
        """ return the time in nanoseconds for a given distance in meters """
        return int(distance / self.__c)

    def get_time_distance(self, time_ns):
        """ return the distance in meters for a given time interval in nanoseconds """
        return time_ns * self.__c


class SimulatedData(object):
    def __init__(self, x_coord_or_point, y_coord=None):
        self.strike_location = types.Point(x_coord_or_point, y_coord)
        self.signal_velocity = SignalVelocity()
        self.event_builder = builder.Event()
        self.timestamp = pd.Timestamp(datetime.datetime.utcnow())

    def get_timestamp(self):
        return self.timestamp

    def get_event_at(self, x_coord_or_point, y_coord=None, distance_offset=0.0):
        event_location = types.Point(x_coord_or_point, y_coord)
        distance = self.strike_location.distance_to(event_location)

        nanosecond_offset = self.signal_velocity.get_distance_time(distance + distance_offset)

        self.event_builder.set_x(x_coord_or_point)
        self.event_builder.set_y(y_coord)
        self.event_builder.set_timestamp(self.timestamp, nanosecond_offset)

        return self.event_builder.build()

    def get_source_event(self):
        self.event_builder.set_x(self.strike_location.x)
        self.event_builder.set_y(self.strike_location.y)
        self.event_builder.set_timestamp(self.timestamp)
        return self.event_builder.build()


class ThreePointSolution(data.Event):
    def __init__(self, reference_event, azimuth, distance, signal_velocity):
        location = reference_event.geodesic_shift(azimuth, distance)
        distance = reference_event.distance_to(location)

        total_nanoseconds = reference_event.timestamp.value
        total_nanoseconds -= signal_velocity.get_distance_time(distance)
        self.signal_velocity = signal_velocity

        super(ThreePointSolution, self).__init__(pd.Timestamp(total_nanoseconds), location)

    def get_residual_time_at(self, event):
        distance = self.distance_to(event)
        distance_runtime = self.signal_velocity.get_distance_time(distance)

        measured_runtime = event.timestamp.value - self.timestamp.value

        return (measured_runtime - distance_runtime) / 1000.0

    def get_total_residual_time_of(self, events):
        residual_time_sum = 0.0
        for event in events:
            residual_time = self.get_residual_time_at(event)

            residual_time_sum += residual_time * residual_time

        return math.sqrt(residual_time_sum) / len(events)

    def __str__(self):
        return super(ThreePointSolution, self).__str__()


class ThreePointSolver(object):
    """
    calculates the exact coordinates of the intersection of two hyperbola defined by three event points/times

    The solution is calculated in a polar coordinate system which has its origin in the location
    of the first event point
    """

    def __init__(self, events):
        if len(events) != 3:
            raise ValueError("ThreePointSolution requires three events")

        self.events = events
        from . import INJECTOR

        self.signal_velocity = INJECTOR.get(SignalVelocity)

        distance_0_1 = events[0].distance_to(events[1])
        distance_0_2 = events[0].distance_to(events[2])

        azimuth_0_1 = events[0].azimuth_to(events[1])
        azimuth_0_2 = events[0].azimuth_to(events[2])

        time_difference_0_1 = events[0].ns_difference_to(events[1])
        time_difference_0_2 = events[0].ns_difference_to(events[2])

        time_distance_0_1 = self.signal_velocity.get_time_distance(time_difference_0_1)
        time_distance_0_2 = self.signal_velocity.get_time_distance(time_difference_0_2)

        self.solutions = self.solve(time_distance_0_1, distance_0_1, azimuth_0_1, time_distance_0_2,
                                    distance_0_2, azimuth_0_2)

    def get_solution_for(self, events):
        solution_count = len(self.solutions)

        if solution_count > 1:
            solutions = {}
            for solution in self.solutions:
                solutions[solution] = solution.get_total_residual_time_of(self.events)
                print("      3PointSolution:", solution, solutions[solution])
            return min(solutions, key=solutions.get)
        elif solution_count == 1:
            return self.solutions[0]
        else:
            return None

    def solve(self, d1, g1, azimuth1, d2, g2, azimuth2):

        phi1 = self.azimuth_to_angle(azimuth1)
        phi2 = self.azimuth_to_angle(azimuth2)

        (p1, q1) = self.calculate_p_q(d1, g1)
        (p2, q2) = self.calculate_p_q(d2, g2)

        (cosine, sine) = self.calculate_angle_projection(phi1, phi2)

        denominator = q1 * q1 + q2 * q2 - 2 * q1 * q2 * cosine

        root_argument = q2 * q2 * (-self.square(p1 - p2) + denominator) * sine * sine

        solutions = []

        if root_argument < 0.0 or denominator == 0.0:
            print("%.1f %.1f %.1f°, %.1f %.1f %.1f°" % (d1, g1, azimuth1, d2, g2, azimuth2))
            return solutions

        part_1 = (-p1 * q1 + p2 * q1 + (p1 - p2) * q2 * cosine) / denominator
        part_2 = math.sqrt(root_argument) / denominator

        solution_angles = []
        if abs(part_1 + part_2) <= 1.0:
            solution_angles.append(math.acos(part_1 + part_2))
            solution_angles.append(-math.acos(part_1 + part_2))
        if abs(part_1 - part_2) <= 1.0:
            solution_angles.append(math.acos(part_1 - part_2))
            solution_angles.append(-math.acos(part_1 - part_2))

        for solution_angle in solution_angles:
            if self.is_angle_valid_for_hyperbola(solution_angle, d1, g1, phi1) and \
                    self.is_angle_valid_for_hyperbola(solution_angle, d2, g2, phi2):

                solution_distance = self.hyperbola_radius(solution_angle, d1, g1, 0)
                solution_azimuth = self.angle_to_azimuth(solution_angle + phi1)
                solution_a = ThreePointSolution(self.events[0], solution_azimuth, solution_distance,
                                                self.signal_velocity)

                solution_distance = self.hyperbola_radius(solution_angle, d2, g2, phi2 - phi1)
                solution_azimuth = self.angle_to_azimuth(solution_angle + phi1)
                solution_b = ThreePointSolution(self.events[0], solution_azimuth, solution_distance,
                                                self.signal_velocity)

                if solution_a.has_same_location(solution_b):
                    solutions.append(solution_a)

        return solutions

    @staticmethod
    def calculate_angle_projection(phi1, phi2):
        angle = phi2 - phi1
        return math.cos(angle), math.sin(angle)

    @staticmethod
    def hyperbola_radius(theta, d, g, phi):
        return 0.5 * (g * g - d * d) / (d + g * math.cos(theta - phi))

    def is_angle_valid_for_hyperbola(self, angle, d, g, phi):
        angle -= phi

        if angle <= math.pi:
            angle += 2 * math.pi
        if angle > math.pi:
            angle -= 2 * math.pi

        asymptotic_angle = self.calculate_asymptotic_angle(d, g)

        return -asymptotic_angle < angle < asymptotic_angle

    @staticmethod
    def calculate_asymptotic_angle(d, g):
        return math.acos(-d / g)

    @staticmethod
    def calculate_p_q(d, g):
        denominator = g * g - d * d
        return d / denominator, g / denominator

    @staticmethod
    def square(x):
        return x * x

    @staticmethod
    def azimuth_to_angle(azimuth):
        return math.pi / 2 - azimuth

    @staticmethod
    def angle_to_azimuth(angle):
        return math.pi / 2 - angle


class FitSeed(object):
    def __init__(self, events, signal_velocity):
        self.events = events
        self.signal_velocity = signal_velocity

        self.solutions = {}

    def iterate_combinations(self):

        for combination in itertools.combinations(self.events[1:5], 2):
            self.find_three_point_solution([self.events[0]] + list(combination))

    def find_three_point_solution(self, selected_events):
        three_point_solver = ThreePointSolver(selected_events)

        solution = three_point_solver.get_solution_for(self.events)
        if solution:
            self.solutions[solution] = solution.get_total_residual_time_of(self.events)

    def get_seed_event(self):

        self.iterate_combinations()

        if self.solutions:
            for solution, residual_time in self.solutions.iteritems():
                print("%.1f %s" % (residual_time, str(solution)))
            return min(self.solutions, key=self.solutions.get)
        else:
            return None


class FitParameter:
    Time, Longitude, Latitude = range(3)

    def __init__(self):
        pass


class LeastSquareFit(object):
    TIME_FACTOR = 1000.0

    def __init__(self, three_point_solution, events, signal_velocity):
        self.n_dim = 3
        self.m_dim = len(events)
        self.events = events
        self.time_reference = events[0].timestamp
        self.signal_velocity = signal_velocity

        self.parameters = collections.OrderedDict()
        self.parameters[FitParameter.Time] = self.calculate_time_value(three_point_solution.timestamp)
        self.parameters[FitParameter.Longitude] = three_point_solution.x
        self.parameters[FitParameter.Latitude] = three_point_solution.y

        self.a_matrix = np.zeros((self.m_dim, self.n_dim))
        self.b_vector = np.zeros(self.m_dim)
        self.residuals = np.zeros(self.m_dim)

        self.least_square_sum = None
        self.previous_least_square_sum = None

        self.successful = False
        self.iteration_count = 0

    def get_parameter(self, parameter):
        return self.parameters[parameter]

    def calculate_time_value(self, timestamp):
        return (timestamp.value - self.time_reference.value) / self.TIME_FACTOR

    def calculate_partial_derivative(self, event_index, parameter_index):

        delta = 0.001

        self.parameters[parameter_index] += delta

        slope = (self.residuals[event_index] - self.get_residual_time_at(self.events[event_index])) / delta

        self.parameters[parameter_index] -= delta

        return slope

    def perform_fit_step(self):

        self.iteration_count += 1

        self.initialize_data()

        (solution, residues, rank, singular_values) = np.linalg.lstsq(self.a_matrix, self.b_vector)

        self.update_parameters(solution)

        self.calculate_least_square_sum()

    def initialize_data(self):

        for index, event in enumerate(self.events):
            self.residuals[index] = self.get_residual_time_at(event)

        for event_index in range(self.m_dim):
            for parameter_index in self.parameters:
                self.a_matrix[event_index][parameter_index] = self.calculate_partial_derivative(event_index,
                                                                                                parameter_index)
            self.b_vector[event_index] = self.get_residual_time_at(self.events[event_index])

    def update_parameters(self, solution):
        for parameter_index, delta in enumerate(solution):
            self.parameters[parameter_index] += delta

    def calculate_least_square_sum(self):
        residual_time_sum = 0.0

        for event in self.events:
            residual_time = self.get_residual_time_at(event)

            residual_time_sum += residual_time * residual_time

        overdetermination_factor = max(1, self.m_dim - self.n_dim)

        residual_time_sum /= overdetermination_factor

        self.previous_least_square_sum = self.least_square_sum
        self.least_square_sum = residual_time_sum

        return residual_time_sum

    def get_residual_time_at(self, event):
        location = self.get_location()
        distance = location.distance_to(event)
        distance_runtime = self.signal_velocity.get_distance_time(distance) / self.TIME_FACTOR

        measured_runtime = self.calculate_time_value(event.timestamp) - self.parameters[FitParameter.Time]

        return measured_runtime - distance_runtime

    def get_location(self):
        return types.Point(self.parameters[FitParameter.Longitude], self.parameters[FitParameter.Latitude])

    def get_timestamp(self):
        return pd.Timestamp(self.time_reference.value + int(round(self.parameters[FitParameter.Time] * 1000, 0)))

    def get_least_square_sum(self):
        return self.least_square_sum

    def get_least_square_change(self):
        if self.least_square_sum and self.previous_least_square_sum:
            return self.least_square_sum / self.previous_least_square_sum - 1.0
        return float("nan")

    def requires_another_iteration(self):
        if self.least_square_sum and self.previous_least_square_sum:
            if self.least_square_sum / self.previous_least_square_sum > 1.1:
                return False
            if abs(self.get_least_square_change()) < 10e-4:
                self.successful = True
                return False

        if self.iteration_count >= 20:
            return False

        return True

    def is_successful(self):
        return self.successful

    def get_solution(self):
        event_builder = builder.Event()

        location = self.get_location()
        event_builder.set_x(location.x())
        event_builder.set_y(location.y())
        event_builder.set_timestamp(self.get_timestamp())

        return event_builder.build()

    def get_number_of_iterations(self):
        return self.iteration_count
