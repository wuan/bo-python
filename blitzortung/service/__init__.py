# -*- coding: utf8 -*-

"""
Copyright (C) 2012-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

from . import histogram, strike, strike_grid


def strike_query():
    from blitzortung import INJECTOR

    return INJECTOR.get(strike.StrikeQuery)


def strike_grid_query():
    from blitzortung import INJECTOR

    return INJECTOR.get(strike_grid.StrikeGridQuery)


def histogram_query():
    from blitzortung import INJECTOR

    return INJECTOR.get(histogram.HistogramQuery)