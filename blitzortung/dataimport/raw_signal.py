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

import logging
import os

from injector import singleton, inject

from .base import HttpFileTransport, BlitzortungDataPath, BlitzortungDataPathGenerator
from .. import builder


@singleton
class RawSignalsBlitzortungDataProvider:
    logger = logging.getLogger(__name__)

    @inject
    def __init__(self, data_transport: HttpFileTransport, data_url: BlitzortungDataPath,
                 url_path_generator: BlitzortungDataPathGenerator, waveform_builder: builder.RawWaveformEvent):
        self.data_transport = data_transport
        self.data_url = data_url
        self.url_path_generator = url_path_generator
        self.waveform_builder = waveform_builder

    def get_raw_data_since(self, latest_data, region, station_id):
        self.logger.debug("import raw data since %s" % latest_data)

        for url_path in self.url_path_generator.get_paths(latest_data):
            target_url = self.data_url.build_path(
                os.path.join(str(station_id), url_path),
                region=region,
                host_name='signals')

            for line in self.data_transport.read_lines(target_url):
                try:
                    yield self.waveform_builder.from_string(line).build()
                except builder.BuilderError as e:
                    self.logger.warn(str(e))
