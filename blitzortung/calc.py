# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

import math
import numpy
from injector import Module, inject, singleton, provides

import blitzortung

class SignalVelocity(object):
    
    # speed of light in m / ns
    __c0 = 0.299792458 
    
    __c_reduction_permille = 2.5
    
    __c = (1 - 0.001 * __c_reduction_permille) * __c0
    
    def get_distance_time(self, distance):
        return int(distance / self.__c)
    
    def get_time_distance(self, time_ns):
        return time_ns * self.__c


class CalcModule(Module):
    
    @singleton
    @provides(SignalVelocity)
    def provide_signal_velocity(self):
        return SignalVelocity()

class ThreePointSolution(object):
    
    def __init__(self, center_event, azimuth, distance):
        #print azimuth, distance
        self.location = center_event.geodesic_shift(azimuth, distance)
        
        distance = center_event.distance_to(self.location)        
        timestamp = center_event.get_timestamp();
        
    def get_location(self):
        return self.location
    
    def __eq__(self, other):
        return self.location == other.location
    
class ThreePointSolver(object):
    
    """ calculates the exact coordinates of the intersection of two hyperbola defined by three event points/times
    
    The solution is calculated in a polar coordinate system which has its origin in the location of the first event point"""
    
    def __init__(self, events):
        if len(events) != 3:
            raise ValueError("ThreePointSolution requires three events")
            
        self.events = events
        from __init__ import INJECTOR
        self.signal_velocity = INJECTOR.get(SignalVelocity)

        distance_0_1 = events[0].distance_to(events[1])
        distance_0_2 = events[0].distance_to(events[2])
        
        azimuth_0_1 = events[0].azimuth_to(events[1])
        azimuth_0_2 = events[0].azimuth_to(events[2])
        
        time_difference_0_1 = events[0].ns_difference_to(events[1])
        time_difference_0_2 = events[0].ns_difference_to(events[2])
        
        time_distance_0_1 = self.signal_velocity.get_time_distance(time_difference_0_1)
        time_distance_0_2 = self.signal_velocity.get_time_distance(time_difference_0_2)

        #print "0-1 %.2f %1f %1f" % (azimuth_0_1 / math.pi * 180, distance_0_1, time_distance_0_1)
        #print "0-2 %.2f %1f %1f" % (azimuth_0_2 / math.pi * 180, distance_0_2, time_distance_0_2)
        
        self.solutions = self.solve(time_distance_0_1, distance_0_1, azimuth_0_1, time_distance_0_2, distance_0_2, azimuth_0_2)
        
    def get_solutions(self):
        return self.solutions
    
    def solve(self, D1, G1, azimuth1, D2, G2, azimuth2):
        
        phi1 = self.azimuth_to_angle(azimuth1)
        phi2 = self.azimuth_to_angle(azimuth2)
        
        (p1, q1) = self.calculate_P_Q(D1, G1)
        (p2, q2) = self.calculate_P_Q(D2, G2)
        
        (cosine, sine) = self.calculateAngleProjection(phi1, phi2)
        
        denominator = q1 * q1 + q2 * q2 - 2 * q1 * q2 * cosine
        
        part_1 = (-p1 * q1 + p2 * q1 + (p1 - p2) * q2 * cosine) / denominator
        part_2 = math.sqrt( q2 * q2 * (-self.square(p1 - p2) + denominator) * sine * sine) / denominator
        
        angle_offset = (phi1 - phi2) / 2
        angle_offset = 0
        solution_angles = []
        solution_angles.append(math.acos(part_1 + part_2) + angle_offset)
        solution_angles.append(math.acos(part_1 - part_2) + angle_offset)
        solution_angles.append(-math.acos(part_1 + part_2) + angle_offset)
        solution_angles.append(-math.acos(part_1 - part_2) + angle_offset)
        
        #print numpy.array(solution_angles) / math.pi * 180
        
        solutions = []
        
        for solution_angle in solution_angles:
            if self.is_angle_valid_for_hyperbola(solution_angle, D1, G1, phi1) and \
               self.is_angle_valid_for_hyperbola(solution_angle, D2, G2, phi2):
                
                solution_distance = self.hyperbola_radius(solution_angle, D1, G1, 0)
                solution_azimuth = self.angle_to_azimuth(solution_angle + phi1)
                solution_a = ThreePointSolution(self.events[0], solution_azimuth, solution_distance)
                
                solution_distance = self.hyperbola_radius(solution_angle, D2, G2, phi2 - phi1)
                solution_azimuth = self.angle_to_azimuth(solution_angle + phi1)
                solution_b = ThreePointSolution(self.events[0], solution_azimuth, solution_distance)
                
                if  solution_a.get_location() == solution_b.get_location():
                  solutions.append(solution_a)
                
        return solutions
                
            
    def calculateAngleProjection(self, phi1, phi2):
        angle = phi2 - phi1
        return (math.cos(angle), math.sin(angle))
    
    def hyperbola_radius(self, theta, D, G, phi):
        return 0.5 * (G * G - D * D) / (D + G * math.cos(theta - phi))
        
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
         
    def azimuth_to_angle(self, azimuth):
        return math.pi / 2 - azimuth
    
    def angle_to_azimuth(self, angle):
        return math.pi / 2 - angle
        