import subprocess

class Plot(object):
  def __init__(self, xsize=480, ysize=320, *args):
    self.xsize = xsize
    self.ysize = ysize
    self.args = args
    self.output = None

  def plot(self, *args):
    pass

  def create(self):
    if self.output is None:
      self.gnuplot = subprocess.Popen(['gnuplot'], stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
      self.write('set terminal png transparent enhanced size %d, %d font "/usr/share/fonts/truetype/ttf-dejavu/DejaVuSans.ttf" 8' %(self.xsize, self.ysize))
      self.write('set output')
      
      self.plot(*self.args)
      
      self.write('\nquit')
      (self.output, self.error) = self.gnuplot.communicate()

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
    self.write('reset; unset xtics; unset ytics')
    self.write('unset border; unset key')
    self.write('set title "%s"' % message)
    self.write('plot [][0:1] 2')

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
    
