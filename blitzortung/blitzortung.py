# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

class Config(object):
  def __init__(self, configfilename=None):

    self.config = {}

    if configfilename == None:
      configfilename = '/etc/default/blitzortung-tracker'

    configfile = open(configfilename, 'r')
    for line in configfile:
      line = line.strip()

      if len(line) > 0 and line[0] != '#':

        index = line.find('=')

        variable = line[0:index]

        basename = 'BLITZORTUNG_'

        if variable.find(' ') < 0:
          if variable[:len(basename)] == basename:
            self.config[variable[len(basename):].upper()] = line[index+1:].replace('"', '')

  def get(self, key):
    key = key.upper()

    if self.config.has_key(key):
      return self.config[key]

    raise Exception("Config.get() key '%s' not found" % key)
