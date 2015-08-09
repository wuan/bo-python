# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""
import logging
import os
import time
import pytz
import datetime

from injector import singleton, inject
import pandas as pd

from .base import HttpFileTransport, BlitzortungDataPath, BlitzortungDataPathGenerator
from .. import builder

@singleton
class StrikesBlitzortungDataProvider(object):
    logger = logging.getLogger(__name__)

    @inject(data_transport=HttpFileTransport, data_url=BlitzortungDataPath,
            url_path_generator=BlitzortungDataPathGenerator, strike_builder=builder.Strike)
    def __init__(self, data_transport, data_url, url_path_generator, strike_builder):
        self.data_transport = data_transport
        self.data_url = data_url
        self.url_path_generator = url_path_generator
        self.strike_builder = strike_builder

    def get_strikes_since(self, latest_strike=None, region=1):
        latest_strike = latest_strike if latest_strike else \
            (datetime.datetime.utcnow() - datetime.timedelta(hours=6)).replace(tzinfo=pytz.UTC)
        self.logger.debug("import strikes since %s" % latest_strike)

        for url_path in self.url_path_generator.get_paths(latest_strike):
            strike_count = 0
            start_time = time.time()
            target_url = self.data_url.build_path(os.path.join('Protected', 'Strokes', url_path), region=region)
            for strike_line in self.data_transport.read_lines(target_url):
                try:
                    strike = self.strike_builder.from_line(strike_line).build()
                except builder.BuilderError as e:
                    self.logger.warn("%s: %s (%s)" % (e.__class__, e.args, strike_line))
                    continue
                except Exception as e:
                    self.logger.error("%s: %s (%s)" % (e.__class__, e.args, strike_line))
                    raise e
                timestamp = strike.timestamp
                timestamp.nanoseconds = 0
                if not pd.isnull(timestamp) and latest_strike < timestamp:
                    strike_count += 1
                    yield strike
            end_time = time.time()
            self.logger.debug("imported %d strikes for region %d in %.2fs from %s",
                              strike_count,
                              region, end_time - start_time, url_path)

