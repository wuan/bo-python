import StringIO
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["font.size"] = 8.0
import matplotlib.pyplot as plt

class Plot(object):
  def __init__(self, *args):
    self.xsize = 480
    self.ysize = 320
    self.args = args
    self.output = None

  def set_width(width):
    self.xsize = width
    
  def set_height(height):
    self.ysize = height
    
  def plot(self, *args):
    pass

  def create(self):
    if self.output is None:
      self.figure = plt.figure(dpi=100, figsize=(self.xsize/100.0, self.ysize/100.0))
      
      self.plot(*self.args)
      png_data = StringIO.StringIO()
      self.figure.savefig(png_data, transparent=True)
      
      self.output = png_data.getvalue()

  def read(self):
    self.create()
    return self.output
  
  def error_occured(self):
    self.create()
    return len(self.error) > 0
  
  def read_error(self):
    self.create()
    return self.error

  def write(self, cmd):
    self.gnuplot.stdin.write(cmd + '\n')

  def empty_page(self, message=""):
    plot = self.figure.add_subplot()
    plot.title(message)

class Data(object):
  
  def __init__(self):
    self.array = []
  
  def add(self, data):
    self.array.append(data)
    
  def __str__(self):
    
    result = ""
    for data in self.array:
      result += ','.join(map(str,data)) + '\n'
  
    result += 'e\n'
    return result  
    
