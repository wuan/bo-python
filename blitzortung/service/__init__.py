# -*- coding: utf8 -*-

"""

   Copyright 2014-2016 Andreas Würl

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

from . import histogram, strike, strike_grid
from .histogram import HistogramQuery
from .strike import StrikeQuery
from .strike_grid import GlobalStrikeGridQuery, StrikeGridQuery


def strike_query() -> StrikeQuery:
    from .. import INJECTOR

    return INJECTOR.get(strike.StrikeQuery)


def strike_grid_query() -> StrikeGridQuery:
    from .. import INJECTOR

    return INJECTOR.get(strike_grid.StrikeGridQuery)


def global_strike_grid_query() -> GlobalStrikeGridQuery:
    from .. import INJECTOR

    return INJECTOR.get(strike_grid.GlobalStrikeGridQuery)


def histogram_query() -> HistogramQuery:
    from .. import INJECTOR

    return INJECTOR.get(histogram.HistogramQuery)
