# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""
from abc import abstractmethod

import os
import logging
import datetime

try:
    from html.parser import HTMLParser
except ImportError:
    from HTMLParser import HTMLParser

from injector import inject

try:
    from requests import Session
except ImportError:
    class Session(object):
        pass

from .. import config, util


class TransportAbstract(object):
    @abstractmethod
    def read_lines(self, source_path, post_process=None):
        pass


class FileTransport(TransportAbstract):
    def read_lines(self, source_path, post_process=None):
        if os.path.isfile(source_path):
            with open(source_path) as data_file:
                for line in data_file:
                    yield line


class HttpFileTransport(FileTransport):
    TIMEOUT_SECONDS = 60

    logger = logging.getLogger(__name__)
    html_parser = HTMLParser()

    @inject(config=config.Config)
    def __init__(self, config, session=None):
        self.config = config
        self.session = session if session else Session()

    def __del__(self):
        if self.session:
            try:
                self.logger.debug("close http session '%s'" % self.session)
                self.session.close()
            except ReferenceError:
                pass

    def read_lines(self, source_url, post_process=None):
        response = self.session.get(
            source_url,
            auth=(self.config.get_username(), self.config.get_password()),
            stream=True,
            timeout=self.TIMEOUT_SECONDS)

        if response.status_code != 200:
            self.logger.debug("http status %d for get '%s" % (response.status_code, source_url))
            return []

        return self.split_lines(post_process(response.content).splitlines() if post_process else response.iter_lines())

    def split_lines(self, lines):
        return (self.process_line(html_line) for html_line in lines)

    def process_line(self, line):
        return line.decode('utf8')


class BlitzortungDataPath(object):
    default_host_name = 'data'
    default_region = 1

    def __init__(self, base_path=None):
        self.data_path = os.path.join(
            (base_path if base_path else 'http://{host_name}.blitzortung.org'),
            'Data_{region}'
        )

    def build_path(self, sub_path, **kwargs):
        parameters = kwargs

        if 'host_name' not in parameters:
            parameters['host_name'] = self.default_host_name

        if 'region' not in parameters:
            parameters['region'] = self.default_region

        return os.path.join(self.data_path, sub_path).format(**parameters)


class BlitzortungDataPathGenerator(object):
    time_granularity = datetime.timedelta(minutes=10)
    url_path_format = '%Y/%m/%d/%H/%M.log'

    def get_paths(self, start_time, end_time=None):
        for interval_start_time in util.time_intervals(start_time, self.time_granularity, end_time):
            yield interval_start_time.strftime(self.url_path_format)




