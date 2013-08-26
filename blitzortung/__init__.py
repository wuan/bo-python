# -*- coding: utf8 -*-
#-----------------------------------------------------------------------------
#   Copyright (c) 2011, Andreas Wuerl. All rights reserved.
#
#   See the LICENSE file for details.
#-----------------------------------------------------------------------------
"""
blitzortung python modules
"""
__version__ = '1.3.0'

#-----------------------------------------------------------------------------
#  Constants.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
#   Custom ex   ceptions.
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
import clustering
import config
import data
import db
import geom
import files
import types
import util
import dataimport


INJECTOR = Injector([builder.BuilderModule(), config.ConfigModule(), calc.CalcModule(), db.DbModule(), dataimport.WebModule()])

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

    'dataimport.StrokesBlitzortungDataProvider', 'dataimport.StrokesBlitzortungDataProvider',
]
