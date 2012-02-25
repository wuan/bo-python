#!/usr/bin/env python
# -*- coding: utf8 -*-
#-----------------------------------------------------------------------------
#   Copyright (c) 2011, Andreas Wuerl. All rights reserved.
#
#   Released under the GPLv3 license. See the LICENSE file for details.
#-----------------------------------------------------------------------------
"""
blitzortung python modules
"""
__version__ = '0.9.5'

import struct as _struct

#-----------------------------------------------------------------------------
#  Constants.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
#   Custom exceptions.
#-----------------------------------------------------------------------------

class Error(Exception):
  """
  General Blitzortung error class.
  """
  pass


#-----------------------------------------------------------------------------
#   Public interface and exports.
#-----------------------------------------------------------------------------


from blitzortung import Config
import builder
import data
import db
import geom
import files
import plot
import util
import web

__all__ = [
  'Config', # main classes
  
  'builder.Stroke', 'builder.Station'

  'data.TimeIntervals', 'data.Timestamp', 'data.NanosecondTimestamp', # data items

  'db.Stroke', 'db.Location', # database access

  'Error', # custom exceptions

  'files.Raw', 'files.Data',

  'geom.Point',

  'plot.Plot', 'plot.Data', # gnuplot integration

  'util.Timer',

  'web.Url',
]
