# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""
import logging
import os

from injector import singleton, inject

from .base import HttpFileTransport, BlitzortungDataPath, BlitzortungDataPathGenerator
from .. import builder


@singleton
class RawSignalsBlitzortungDataProvider(object):
    logger = logging.getLogger(__name__)

    @inject(data_transport=HttpFileTransport, data_url=BlitzortungDataPath,
            url_path_generator=BlitzortungDataPathGenerator, waveform_builder=builder.RawWaveformEvent)
    def __init__(self, data_transport, data_url, url_path_generator, waveform_builder):
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
