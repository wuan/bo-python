# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

import math
from injector import Module, inject, singleton, provides

import blitzortung

class SignalVelocity(object):
    
    # speed of light in m / ns
    __c0 = 0.299792458 
    
    __c_reduction_permille = 2.5
    
    __c = (1 - 0.001 * __c_reduction_permille) * __c0
    
    def get_runtime(self, distance):
        return distance / self.__c


class CalcModule(Module):
    
    @singleton
    @provides(SignalVelocity)
    def provide_signal_velocity(self):
        return SignalVelocity()

class ThreePointSolution(object):
    
    def __init__(self, center_event, azimuth, distance):
        self.location = center_event.geodesic_shift(azimuth, distance)
        
        distance = center_event.distance_to(self.location)
        
        timestamp = center_event.get_timestamp();
        
        print timestamp.value
        
    def get_location(self):
        return self.location
    
class ThreePointSolver(object):
    
    """ calculates the exact coordinates of the intersection of two hyperbola defined by three event points/times
    
    The solution is calculated in a polar coordinate system which has its origin in the location of the first event point"""
    
    def __init__(self, events):
        if len(events) != 3:
            raise ValueError("ThreePointSolution requires three events")
            
        self.events = events
        from __init__ import injector
        self.signal_velocity = injector.get(SignalVelocity)
        print self.signal_velocity

        distance_0_1 = events[0].distance_to(events[1])
        distance_0_2 = events[0].distance_to(events[2])
        
        azimuth_0_1 = events[0].azimuth_to(events[1])
        azimuth_0_2 = events[0].azimuth_to(events[1])
        
        time_difference_0_1 = events[0].ns_difference_to(events[1])
        time_difference_0_2 = events[0].ns_difference_to(events[1])

        self.solutions = self.solve(time_difference_0_1, distance_0_1, azimuth_0_1, time_difference_0_2, distance_0_2, azimuth_0_2)
        
    
    def solve(self, D1, G1, phi1, D2, G2, phi2):
        (p1, q1) = self.calculate_P_Q(D1, G1)
        (p2, q2) = self.calculate_P_Q(D2, G2)
        
        (cosine, sine) = self.calculateAngleProjection(phi1, phi2)
        
        denominator = q1 * q1 + q2 * q2 - 2 * q1 * q2 * cosine
        
        part_1 = (-p1 * q1 + p2 * q1 + (p1 - p2) * q2 * cosine) / denominator
        part_2 = math.sqrt( q2 * q2 * (-self.square(p1 - p2) + denominator) * sine * sine) / denominator
        
        soultion_directions = []
        soultion_directions.append(math.acos(part_1 + part_2))
        soultion_directions.append(math.acos(part_1 - part_2))
        soultion_directions.append(-math.acos(part_1 + part_2))
        soultion_directions.append(-math.acos(part_1 - part_2))
        
        solutions = []
        
        for solution_direction in soultion_directions:
            if self.is_angle_valid_for_hyperbola(solution_direction, D1, G1, phi1) and \
               self.is_angle_valid_for_hyperbola(solution_direction, D2, G2, phi2):
                solution_distance = self.hyperbola_radius(solution_direction, D1, G1, phi1)
                solution = ThreePointSolution(self.events[0], phi1 - solution_direction, solution_distance)
                solutions.append(solution)
                
        return solutions
                
            
    def calculateAngleProjection(self, A1, A2):
        angle = A1 - A2
        return (math.cos(angle), math.sin(angle))
    
    def hyperbola_radius(self, theta, D, G, phi):
        0.5 * (G * G - D * D)/(D + G * math.cos(theta - phi))
        
    def is_angle_valid_for_hyperbola(self, angle, D, G, phi):
        angle -= phi
        
        if angle <= math.pi:
            angle += 2 * math.pi
        if angle > math.pi:
            angle -= 2 * math.pi
            
        asymptotic_angle = self.calculate_asymptotic_angle(D, G)
        
        return -asymptotic_angle < angle < asymptotic_angle
        
    def calculate_asymptotic_angle(self, D, G):
        return math.acos(-D/G)
        
    def calculate_P_Q(self, D, G):
        denominator = G * G - D * D
        return (D / denominator, G / denominator)
   
    def square(self, x):
        return x*x
         