# -*- coding: utf8 -*-

"""

   Copyright 2014-2022 Andreas Würl

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

"""

import logging

__version__ = '1.7.1'


# -----------------------------------------------------------------------------
# Constants.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
#   Custom exceptions.
# -----------------------------------------------------------------------------


class Error(Exception):
    """
    General Blitzortung error class.
    """
    pass


# -----------------------------------------------------------------------------
#   Public interface and exports.
# -----------------------------------------------------------------------------


import injector

from . import builder
from . import config
from . import data
from . import dataimport
from . import db
from . import geom
from . import util
from . import base

INJECTOR = injector.Injector(
    [config.ConfigModule(), db.DbModule()])

root_logger = logging.getLogger(__name__)
root_logger.setLevel(logging.WARN)


def set_parent_logger(logger):
    logger.parent = root_logger


def set_log_level(log_level):
    root_logger.setLevel(log_level)


def add_log_handler(log_handler):
    root_logger.addHandler(log_handler)
