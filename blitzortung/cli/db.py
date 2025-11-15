# -*- coding: utf8 -*-

"""

   Copyright (C) 2011-2025 Andreas Würl

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

from __future__ import print_function

import datetime
import sys
from zoneinfo import ZoneInfo

import math
import shapely.wkt
from optparse import OptionParser

import blitzortung.config
import blitzortung.db.query
import blitzortung.geom
import blitzortung.util

DEFAULT_GRID = 1.0, 1.0

DATE_FORMAT = '%Y%m%d'
TIME_FORMAT = '%H%M'
SECONDS_FORMAT = '%S'


def parse_time(date_string, time_string, tz, description, is_end_time=False):
    try:
        has_seconds = len(time_string) > 4
        tmp_date = datetime.datetime.strptime(date_string, DATE_FORMAT).date()
        tmp_time = datetime.datetime.strptime(time_string,
                                              TIME_FORMAT + (SECONDS_FORMAT if has_seconds else '')).time()
        parsed_time = datetime.datetime.combine(tmp_date, tmp_time).replace(tzinfo=tz)
        if is_end_time:
            parsed_time += (datetime.timedelta(seconds=1) if has_seconds else datetime.timedelta(minutes=1))
        return parsed_time
    except Exception:
        print("parse error in %s: '%s %s'" % description, date_string, time_string)
        sys.exit(5)


def prepare_grid_if_applicable(options, area):
    if options.grid is not None or options.xgrid is not None or options.ygrid is not None:

        grid_x, grid_y = DEFAULT_GRID

        if options.grid is not None:
            grid_x, grid_y = options.grid, options.grid

        if options.xgrid is not None:
            grid_x = options.xgrid
            if options.ygrid is None and options.grid is None:
                grid_y = grid_x

        if options.ygrid is not None:
            grid_y = options.ygrid
            if options.xgrid is None and options.grid is None:
                grid_x = grid_y

        if options.area is None:
            print("grid options requires declaration of envelope area")
            sys.exit(1)

        env = area.envelope.bounds
        return blitzortung.geom.Grid(env[0], env[2], env[1], env[3], grid_x, grid_y, options.srid)


def main():
    end_time = datetime.datetime.now(datetime.UTC)
    start_time = end_time - datetime.timedelta(hours=1)
    end_time -= datetime.timedelta(minutes=1)

    parser = OptionParser()

    parser.add_option("--startdate", dest="startdate", default="default",
                      help="start date for data retrieval", type="string")

    parser.add_option("--starttime", dest="starttime", default="default",
                      help="start time for data retrieval", type="string")

    parser.add_option("--enddate", dest="enddate", default="default",
                      help="end date for data retrieval", type="string")

    parser.add_option("--endtime", dest="endtime", default="default",
                      help="end time for data retrieval", type="string")

    parser.add_option("--area", dest="area",
                      help="area for which strikes are selected", type="string")

    parser.add_option("--tz", dest="tz", default=str(datetime.UTC),
                      help="used timezone", type="string")

    parser.add_option("--useenv", dest="useenv", default=False, action="store_true",
                      help="use envelope of given area for query")

    parser.add_option("--srid", dest="srid", default=blitzortung.geom.Geometry.default_srid,
                      help="srid for query area and results", type="int")

    parser.add_option("--precision", dest="precision", default=4,
                      help="precision of coordinates", type="int")

    parser.add_option("--grid", dest="grid",
                      help="grid width", type="float")

    parser.add_option("--x-grid", dest="xgrid",
                      help="grid x width", type="float")

    parser.add_option("--y-grid", dest="ygrid",
                      help="grid y width", type="float")

    parser.add_option("--map", dest="map", action="store_true",
                      help="show ascii map instead of numerical grid")

    (options, args) = parser.parse_args()

    try:
        tz = ZoneInfo(options.tz)
    except:
        print('parse error in timezone "' + options.tz + '"')
        sys.exit(1)

    start_time = start_time.astimezone(tz)
    end_time = end_time.astimezone(tz)

    if options.startdate == 'default':
        options.startdate = start_time.strftime(DATE_FORMAT)
    if options.starttime == 'default':
        options.starttime = start_time.strftime(TIME_FORMAT)
    non_default_end = options.enddate != 'default' or options.endtime != 'default'
    if options.enddate == 'default':
        options.enddate = end_time.strftime(DATE_FORMAT)
    if options.endtime == 'default':
        options.endtime = end_time.strftime(TIME_FORMAT)

    start_time = parse_time(options.startdate, options.starttime, tz, "starttime")
    end_time = parse_time(options.enddate, options.endtime, tz, "endtime",
                          is_end_time=True) if non_default_end else None

    area = None
    if options.area:
        try:
            area = shapely.wkt.loads(options.area)
        except shapely.errors.GEOSException:
            print('parse error in area "' + options.area + '"')

        if area and options.useenv:
            area = area.envelope

    # open strike database
    strike_db = blitzortung.db.strike()

    # set data parameters
    strike_db.set_srid(options.srid)
    strike_db.set_timezone(tz)

    # set time range
    time_interval = blitzortung.db.query.TimeInterval(start_time, end_time)

    # set query parameters
    order = 'timestamp'

    grid = prepare_grid_if_applicable(options, area)

    # start timer for database select
    timer = blitzortung.util.Timer()

    if grid:
        grid_result = strike_db.select_grid(grid, time_interval=time_interval)

        select_time = timer.lap()

        print(grid_result.to_map() if options.map else grid_result.to_arcgrid())
        sys.stderr.write('received grid data in %.3f seconds\n' % (select_time))

    else:
        strikes = strike_db.select(time_interval=time_interval, geometry=area, order=order)

        precision_factor = math.pow(10.0, options.precision)

        strike_count = 0
        for strike in strikes:
            strike_count += 1
            strike.x = round(strike.x * precision_factor) / precision_factor
            strike.y = round(strike.y * precision_factor) / precision_factor
            print(str(strike))

        select_time = timer.lap()

        sys.stderr.write('received %d strikes in %.3f seconds\n' % (strike_count, select_time))


if __name__ == '__main__':
    main()
