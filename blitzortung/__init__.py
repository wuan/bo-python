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
__version__ = '1.2.0'

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

from injector import Injector

import builder
import cache
import calc
import config
import data
import db
import geom
import files
import types
import util
import web


INJECTOR = Injector([config.ConfigModule(), calc.CalcModule(), db.DbModule(), web.WebModule()])

__all__ = [

    'builder.Stroke', 'builder.Station',

    'calc.ObjectCache'

    'calc.ThreePointSolution', 'calc.ThreePointSolver',

    'data.TimeIntervals', 'data.Timestamp', 'data.NanosecondTimestamp',  # data items

    'db.Stroke', 'db.Location', 'db.Station', 'db.StationOffline',  # database access

    'Error',  # custom exceptions

    'files.Raw', 'files.Data',

    'geom.Point',

    'types.Point',

    'util.Timer',

    'web.Url',
]
