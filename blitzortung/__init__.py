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
__version__ = '1.1.0'

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

import builder
import calc
import config
import data
import db
import geom
import files
import util
import web

from injector import Injector

INJECTOR = Injector([config.Config(), calc.CalcModule(), db.Connection()])

__all__ = [

    'builder.Stroke', 'builder.Station',
    
    'calc.ThreePointSolution', 'calc.ThreePointSolver',

    'data.TimeIntervals', 'data.Timestamp', 'data.NanosecondTimestamp', # data items

    'db.Stroke', 'db.Location', 'db.Station', 'db.StationOffline', # database access

    'Error', # custom exceptions

    'files.Raw', 'files.Data',

    'geom.Point',

    'util.Timer',

    'web.Url',
]
