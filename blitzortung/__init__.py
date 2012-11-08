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
import data
import db
import geom
import files
import util
import web

import injector
import ConfigParser

Configuration = injector.Key('configuration')

class Config(injector.Module):
    def __init__(self, configfilename='/etc/blitzortung.conf'):

        self.config = ConfigParser.ConfigParser()
        self.config.read(configfilename)
        
    def get_username(self):
        return self.config.get('auth', 'username')
    
    def get_password(self):
        return self.config.get('auth', 'password')
    
    def get_raw_path(self):
        return self.config.get('path', 'raw')
    
    def get_archive_path(self):
        return self.config.get('path', 'archive')

    def configure(self, binder):
        binder.bind(Configuration, to={'db_connection_string': "host='localhost' dbname='blitzortung' user='blitzortung' password='blitzortung'"}, scope=singleton)
    

__all__ = [
    'Config', 'Configuration', # main classes

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
