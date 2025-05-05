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

import datetime
import logging
import os
import time

from injector import singleton, inject

from .base import HttpFileTransport, BlitzortungDataPath, BlitzortungDataPathGenerator
from .. import builder

logger = logging.getLogger(__name__)


@singleton
class StrikesBlitzortungDataProvider:

    @inject
    def __init__(self, data_transport: HttpFileTransport, data_url: BlitzortungDataPath,
                 url_path_generator: BlitzortungDataPathGenerator, strike_builder: builder.Strike):
        self.data_transport = data_transport
        self.data_url = data_url
        self.url_path_generator = url_path_generator
        self.strike_builder = strike_builder

    def get_strikes_since(self, latest_strike=None, region=1):
        latest_strike = latest_strike if latest_strike else \
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=6)
        logger.debug("import strikes since %s" % latest_strike)

        for url_path in self.url_path_generator.get_paths(latest_strike):
            strike_count = 0
            start_time = time.time()
            target_url = self.data_url.build_path(os.path.join('Protected', 'Strikes_{region}', url_path), region=region)
            for strike_line in self.data_transport.read_lines(target_url):
                try:
                    strike = self.strike_builder.from_line(strike_line).build()
                except builder.BuilderError as e:
                    logger.warning("%s: %s (%s)" % (e.__class__, e.args, strike_line))
                    continue
                except Exception as e:
                    logger.error("%s: %s (%s)" % (e.__class__, e.args, strike_line))
                    raise e
                if strike.timestamp.is_valid and strike.timestamp > latest_strike:
                    strike_count += 1
                    yield strike
            end_time = time.time()
            logger.debug("imported %d strikes for region %d in %.2fs from %s",
                         strike_count,
                         region, end_time - start_time, target_url)
