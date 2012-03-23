import unittest
import mock

import png

import numpy as np
import matplotlib as plt

import blitzortung

class Plot(blitzortung.plot.Plot):
    
    def plot(self, *args):
        x = np.arange(-5, 5, 0.1)
        y = np.sin(x)
        
        plot = self.figure.add_subplot(111)
        plot.plot(x,y)
        
class PlotTest(unittest.TestCase):
    
    def test_plot_generation(self):
        plot = Plot()
        
        picture = png.Reader(bytes=plot.read())
        
        (width, height, pixels, metadata) = picture.read()
        
        self.assertEqual(480, width)
        self.assertEqual(320, height)
        self.assertEqual(8, metadata['bitdepth'])