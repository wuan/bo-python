import unittest
import math
import datetime
import pandas as pd

import blitzortung

class TestSignalVelocity(unittest.TestCase):
    
    def setUp(self):
        self.signal_velocity = blitzortung.calc.SignalVelocity()
        
    def test_get_distance_time(self):
        self.assertAlmostEqual(33440, self.signal_velocity.get_distance_time(10000.0))
        
    def test_get_time_distance(self):
        self.assertAlmostEqual(29.90429768, self.signal_velocity.get_time_distance(100))     
        
    
class ThreePointSolutionTest(unittest.TestCase):

    def setUp(self):
        self.timestamp = datetime.datetime.utcnow()
        event_builder = blitzortung.builder.Event()
        event_builder.set_x(11.0)
        event_builder.set_y(49.0)
        event_builder.set_timestamp(self.timestamp)
        self.center_event = event_builder.build()    
        self.radians_factor = math.pi / 180
        self.signal_velocity = blitzortung.calc.SignalVelocity()

    def test_get_solution_location(self):
        solution = blitzortung.calc.ThreePointSolution(self.center_event, 0, 100000, self.signal_velocity)
        location = solution.get_location()

        self.assertAlmostEqual(location.get_x(), 11)
        self.assertAlmostEqual(location.get_y(), 49.89913151)

        solution = blitzortung.calc.ThreePointSolution(self.center_event, math.pi / 2, 100000, self.signal_velocity)
        location = solution.get_location()

        self.assertAlmostEqual(location.get_x(), 12.3664992)
        self.assertAlmostEqual(location.get_y(), 48.9919072)    

    def test_get_solution_timestamp(self):
        solution = blitzortung.calc.ThreePointSolution(self.center_event, 0, 100000, self.signal_velocity)

        timestamp = self.center_event.get_timestamp()
        total_nanoseconds = timestamp.value - self.signal_velocity.get_distance_time(100000)
        stroke_timestamp = pd.Timestamp(total_nanoseconds, tz=timestamp.tzinfo)
        
        self.assertEqual(stroke_timestamp, solution.get_timestamp())
        
class ThreePointSolverTest(unittest.TestCase):

    def setUp(self):
        self.signal_velocity = blitzortung.calc.SignalVelocity()
        self.prepare_solution(11.5, 49.5)
        
    def prepare_solution(self, x_coord, y_coord):
        self.simulated_data = blitzortung.calc.SimulatedData(x_coord, y_coord)
        self.center_event = self.simulated_data.get_event_at(11.0, 49.0)
        self.event_1 = self.simulated_data.get_event_at(12.0, 49.0)
        self.event_2 = self.simulated_data.get_event_at(11.0, 50.0)
        
        self.events = [self.center_event, self.event_1, self.event_2]
        
    def test_solve_with_no_solution(self):
        
        self.prepare_solution(11.5, 49.0)
        self.event_1 = self.simulated_data.get_event_at(10.0, 49.0)
        self.event_2 = self.simulated_data.get_event_at(12.0, 49.0)
        
        self.events = [self.center_event, self.event_1, self.event_2]
        
        solver = blitzortung.calc.ThreePointSolver(self.events)
        
        solutions = solver.get_solutions()
        
        self.assertEqual(0, len(solutions))       

    def test_solve_with_one_solution(self):
        
        location = blitzortung.types.Point(11.7, 49.3)
        self.prepare_solution(location.get_x(), location.get_y())
        
        solver = blitzortung.calc.ThreePointSolver(self.events)
        
        solutions = solver.get_solutions()
        
        self.assertEqual(1, len(solutions))
        
        self.assertEqual(location, solutions[0].get_location())
        
            
    def test_solve_with_two_solutions(self):
        
        location = blitzortung.types.Point(11.1, 49.1)
        self.prepare_solution(location.get_x(), location.get_y())
        
        solver = blitzortung.calc.ThreePointSolver(self.events)
        
        solutions = solver.get_solutions()
        
        self.assertEqual(2, len(solutions))
        
        self.assertEqual(location, solutions[0].get_location())
        
        self.assertNotEqual(location, solutions[1].get_location())
            
    def test_azimuth_to_angle(self):
        solver = blitzortung.calc.ThreePointSolver(self.events)
                
        self.assertAlmostEqual(math.pi / 2, solver.azimuth_to_angle(0))
        self.assertAlmostEqual(0, solver.azimuth_to_angle(math.pi / 2))
        self.assertAlmostEqual(math.pi, solver.azimuth_to_angle(-math.pi / 2))
        
    def test_angle_to_azimuth(self):
        solver = blitzortung.calc.ThreePointSolver(self.events)
                
        self.assertAlmostEqual(math.pi / 2, solver.angle_to_azimuth(0))
        self.assertAlmostEqual(0, solver.angle_to_azimuth(math.pi / 2))
        self.assertAlmostEqual(-math.pi / 2, solver.angle_to_azimuth(math.pi))
        
        
class TestLeastSquareFit(unittest.TestCase):
    
    def setUp(self):
        self.timestamp = pd.Timestamp(datetime.datetime.utcnow())
        
        event_builder = blitzortung.builder.Event()
        
        event_builder.set_timestamp(self.timestamp)
        event_builder.set_x(11.0)
        event_builder.set_y(49.0)
        self.three_point_solution = event_builder.build()
        
        self.events = []
        
        event_builder.set_timestamp(self.timestamp + datetime.timedelta(microseconds=100))
        event_builder.set_x(10.6)
        self.events.append(event_builder.build())
        
        self.fit = blitzortung.calc.LeastSquareFit(self.three_point_solution, self.events, blitzortung.calc.SignalVelocity())
        
    def test_get_parameter(self):
        self.assertAlmostEqual(11.0, self.fit.get_parameter(blitzortung.calc.FitParameter.Longitude))
        self.assertAlmostEqual(49.0, self.fit.get_parameter(blitzortung.calc.FitParameter.Latitude))
        self.assertAlmostEquals(-100.0, self.fit.get_parameter(blitzortung.calc.FitParameter.Time))

    def test_get_location(self):
        self.assertEqual(blitzortung.types.Point(11.0, 49.0), self.fit.get_location())
        
    def test_calculate_time_value(self):
        self.assertEqual(-100.0, self.fit.calculate_time_value(self.timestamp))
        self.assertEqual(0.0, self.fit.calculate_time_value(self.events[0].get_timestamp()))
        
    def test_calculate_residual_time(self):
        self.assertAlmostEqual(2.126, self.fit.calculate_residual_time(self.events[0]))
        
    def test_fit(self):
        pass
        