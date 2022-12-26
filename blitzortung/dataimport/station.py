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

import gzip
import io
import logging

from injector import singleton, inject

from .base import HttpFileTransport, BlitzortungDataPath
from .. import builder


@singleton
class StationsBlitzortungDataProvider:
    logger = logging.getLogger(__name__)

    @inject
    def __init__(self, data_transport: HttpFileTransport, data_url: BlitzortungDataPath,
                 station_builder: builder.Station):
        self.data_transport = data_transport
        self.data_url = data_url
        self.station_builder = station_builder

    def get_stations(self, region=1):
        current_stations = []
        target_url = self.data_url.build_path('Protected/stations.txt.gz', region=region)
        for station_line in self.data_transport.read_lines(target_url, post_process=self.pre_process):
            try:
                current_stations.append(self.station_builder.from_line(station_line).build())
            except builder.BuilderError:
                self.logger.debug("error parsing station data '%s'" % station_line)
        return current_stations

    @staticmethod
    def pre_process(data):
        data = io.BytesIO(data)
        out_data = gzip.GzipFile(fileobj=data).read()
        return out_data
