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
from abc import abstractmethod
from html.parser import HTMLParser

from injector import inject
from requests import Session

from .. import config, util


class TransportAbstract:
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

    @inject
    def __init__(self, configuration: config.Config, session=None):
        self.config = configuration
        self.session = session if session else Session()

    def read_lines(self, source_url, post_process=None):
        timer = util.Timer()
        response = self.session.get(
            source_url,
            auth=(self.config.get_username(), self.config.get_password()),
            stream=True,
            timeout=self.TIMEOUT_SECONDS)

        if response.status_code != 200:
            self.logger.debug("http status %d for get '%s' (%.03fs)" % (response.status_code, source_url, timer.lap()))
            return []
        else:
            self.logger.debug("get '%s' (%.03fs)" % (source_url, timer.lap()))

        return self.split_lines(post_process(response.content).splitlines() if post_process else response.iter_lines())

    def split_lines(self, lines):
        return (self.process_line(html_line) for html_line in lines)

    @staticmethod
    def process_line(line):
        return line.decode('utf8')


class BlitzortungDataPath:
    default_host_name = 'data'
    default_region = 1

    def __init__(self, base_path=None):
        self.data_path = os.path.join(
            (base_path if base_path else 'https://{host_name}.blitzortung.org'),
            'Data'
        )

    def build_path(self, sub_path, **kwargs):
        parameters = kwargs

        if 'host_name' not in parameters:
            parameters['host_name'] = self.default_host_name

        if 'region' not in parameters:
            parameters['region'] = self.default_region

        return os.path.join(self.data_path, sub_path).format(**parameters)


class BlitzortungDataPathGenerator:
    time_granularity = datetime.timedelta(minutes=10)
    url_path_format = '%Y/%m/%d/%H/%M.log'

    def get_paths(self, start_time, end_time=None):
        for interval_start_time in util.time_intervals(start_time, self.time_granularity, end_time):
            yield interval_start_time.strftime(self.url_path_format)
