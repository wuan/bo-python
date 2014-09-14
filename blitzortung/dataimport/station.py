# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""
import gzip
import io
import logging

from injector import singleton, inject

from .base import HttpFileTransport, BlitzortungDataPath
from .. import builder

@singleton
class StationsBlitzortungDataProvider(object):
    logger = logging.getLogger(__name__)

    @inject(data_transport=HttpFileTransport, data_url=BlitzortungDataPath,
            station_builder=builder.Station)
    def __init__(self, data_transport, data_url, station_builder):
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

