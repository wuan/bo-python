import unittest
import math
import datetime

import blitzortung

class ThreePointSolutionTest(unittest.TestCase):

    def setUp(self):
        self.timestamp = datetime.datetime.utcnow()
        event_builder = blitzortung.builder.Event()
        event_builder.set_x(11.0)
        event_builder.set_y(49.0)
        event_builder.set_timestamp(self.timestamp)
        self.center_event = event_builder.build()    
        self.radians_factor = math.pi / 180

    def test_get_solution_location(self):
        solution = blitzortung.calc.ThreePointSolution(self.center_event, 0, 100000)
        location = solution.get_location()

        self.assertAlmostEqual(location.get_x(), 11)
        self.assertAlmostEqual(location.get_y(), 49.89913151)

        solution = blitzortung.calc.ThreePointSolution(self.center_event, math.pi / 2, 100000)
        location = solution.get_location()

        self.assertAlmostEqual(location.get_x(), 12.3664992)
        self.assertAlmostEqual(location.get_y(), 48.9919072)    

    def test_get_solution_timestamp(self):
        solution = blitzortung.calc.ThreePointSolution(self.center_event, 0, 100000)

class ThreePointSolverTest(unittest.TestCase):

    def setUp(self):
        self.timestamp = datetime.datetime.utcnow()
        event_builder = blitzortung.builder.Event()
        
        event_builder.set_x(11.0)
        event_builder.set_y(49.0)
        event_builder.set_timestamp(self.timestamp)
        self.center_event = event_builder.build()
        
        event_builder.set_x(12.0)
        event_builder.set_y(49.0)
        event_builder.set_timestamp(self.timestamp + datetime.timedelta(microseconds=200))
        self.event_1 = event_builder.build()
        
        event_builder.set_x(11.0)
        event_builder.set_y(50.0)
        event_builder.set_timestamp(self.timestamp + datetime.timedelta(microseconds=200))
        self.event_2 = event_builder.build()
        
        self.events = [self.center_event, self.event_1, self.event_2]
        
    def test_solve(self):
        solver = blitzortung.calc.ThreePointSolver(self.events)
        
        
        
        
