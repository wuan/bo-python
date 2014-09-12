# -*- coding: utf8 -*-

"""
Copyright (C) 2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import logging


def create_console_handler():
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")
    console_handler.setFormatter(console_formatter)
    return console_handler


def get_logger_name(clazz):
    return "{}.{}".format(clazz.__module__, clazz.__name__)