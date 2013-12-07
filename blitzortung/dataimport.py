# -*- coding: utf8 -*-

"""

@author: awuerl

"""
import StringIO
import os
import logging
import time
import urllib2
import datetime
from urlparse import urlparse
import gzip
import HTMLParser
import cStringIO

from injector import singleton, inject
import pytz

import blitzortung


class HttpDataTransport(object):
    logger = logging.getLogger(__name__)

    @inject(config=blitzortung.config.Config)
    def __init__(self, config):
        self.config = config

    def read_from_url(self, target_url):
        parsed_url = urlparse(target_url)

        server_url = "%s://%s/" % (parsed_url.scheme, parsed_url.hostname)

        auth_info = urllib2.HTTPPasswordMgrWithDefaultRealm()

        auth_info.add_password(None, server_url, self.config.get_username(), self.config.get_password())

        auth_handler = urllib2.HTTPBasicAuthHandler(auth_info)
        url_opener = urllib2.build_opener(auth_handler)

        try:
            url_connection = url_opener.open(target_url, timeout=60)
        except urllib2.URLError, error:
            self.logger.debug("%s when opening '%s'" % (error, target_url))
            return None

        data_string = url_connection.read()

        url_connection.close()

        return data_string


class BlitzortungDataUrl(object):
    default_host_name = 'data'
    default_region = 1

    target_url = 'http://%(host_name)s.blitzortung.org/Data_%(region)d/Protected/'

    def build_url(self, url_path, **kwargs):
        url_parameters = kwargs

        if 'host_name' not in url_parameters:
            url_parameters['host_name'] = self.default_host_name

        if 'region' not in url_parameters:
            url_parameters['region'] = self.default_region

        return os.path.join(self.target_url, url_path) % url_parameters


@singleton
class BlitzortungDataProvider(object):
    logger = logging.getLogger(__name__)
    html_parser = HTMLParser.HTMLParser()

    @inject(http_data_transport=HttpDataTransport)
    def __init__(self, http_data_transport):
        self.http_data_transport = http_data_transport

    def read_text(self, target_url):
        return self.http_data_transport.read_from_url(target_url)

    def read_data(self, target_url, pre_process=None):
        response = self.read_text(target_url)

        if response:
            if pre_process:
                response = pre_process(response)

            response = self.html_parser.unescape(response.decode('latin1')).replace(u'\xa0', ' ')

            for entry_text in response.split('\n'):
                entry_text = entry_text.strip()
                if entry_text:
                    yield entry_text


class BlitzortungHistoryUrlGenerator(object):
    url_path_minute_increment = 10
    url_path_format = '%Y/%m/%d/%H/%M.log'

    def __init__(self):
        self.duration = datetime.timedelta(minutes=self.url_path_minute_increment)

    def get_url_paths(self, start_time, end_time=None):
        for interval_start_time in blitzortung.util.time_intervals(start_time, self.duration, end_time):
            yield interval_start_time.strftime(self.url_path_format)


@singleton
class StrokesBlitzortungDataProvider(object):
    logger = logging.getLogger(__name__)

    @inject(data_provider=BlitzortungDataProvider, data_url=BlitzortungDataUrl,
            url_path_generator=BlitzortungHistoryUrlGenerator, stroke_builder=blitzortung.builder.Stroke)
    def __init__(self, data_provider, data_url, url_path_generator, stroke_builder):
        self.data_provider = data_provider
        self.data_url = data_url
        self.url_path_generator = url_path_generator
        self.stroke_builder = stroke_builder

    def get_strokes_since(self, latest_stroke=None, region=1):
        latest_stroke = latest_stroke if latest_stroke else \
            (datetime.datetime.utcnow() - datetime.timedelta(hours=6)).replace(tzinfo=pytz.UTC)
        self.logger.debug("import strokes since %s" % latest_stroke)
        strokes_since = []

        for url_path in self.url_path_generator.get_url_paths(latest_stroke):
            initial_stroke_count = len(strokes_since)
            start_time = time.time()
            target_url = self.data_url.build_url(os.path.join('Strokes', url_path), region=region)
            for stroke_line in self.data_provider.read_data(target_url):
                try:
                    stroke = self.stroke_builder.from_line(stroke_line).build()
                except blitzortung.builder.BuilderError as e:
                    self.logger.warn("%s: %s (%s)" % (e.__class__, e.message, stroke_line))
                    continue
                except Exception as e:
                    self.logger.error("%s: %s (%s)" % (e.__class__, e.message, stroke_line))
                    raise e
                timestamp = stroke.get_timestamp()
                timestamp.nanoseconds = 0
                if latest_stroke < timestamp:
                    strokes_since.append(stroke)
            end_time = time.time()
            self.logger.debug("imported %d strokes for region %d in %.2fs from %s",
                              len(strokes_since) - initial_stroke_count,
                              region, end_time - start_time, url_path)
        return strokes_since


def strokes():
    from __init__ import INJECTOR

    return INJECTOR.get(StrokesBlitzortungDataProvider)


@singleton
class StationsBlitzortungDataProvider(object):
    logger = logging.getLogger(__name__)

    @inject(data_provider=BlitzortungDataProvider, data_url=BlitzortungDataUrl,
            station_builder=blitzortung.builder.Station)
    def __init__(self, data_provider, data_url, station_builder):
        self.data_provider = data_provider
        self.data_url = data_url
        self.station_builder = station_builder

    def get_stations(self, region=1):
        current_stations = []
        target_url = self.data_url.build_url('stations.txt.gz', region=region)
        for station_line in self.data_provider.read_data(target_url, pre_process=self.pre_process):
            try:
                current_stations.append(self.station_builder.from_line(station_line).build())
            except blitzortung.builder.BuilderError:
                self.logger.debug("error parsing station data '%s'" % station_line)
        return current_stations

    def pre_process(self, data):
        data = cStringIO.StringIO(data)
        return gzip.GzipFile(fileobj=data).read()


def stations():
    from __init__ import INJECTOR

    return INJECTOR.get(StationsBlitzortungDataProvider)


@singleton
class RawSignalsBlitzortungDataProvider(object):
    logger = logging.getLogger(__name__)

    @inject(data_provider=BlitzortungDataProvider, data_url=BlitzortungDataUrl,
            url_path_generator=BlitzortungHistoryUrlGenerator, waveform_builder=blitzortung.builder.RawWaveformEvent)
    def __init__(self, data_provider, data_url, url_path_generator, waveform_builder):
        self.data_provider = data_provider
        self.data_url = data_url
        self.url_path_generator = url_path_generator
        self.waveform_builder = waveform_builder

    def get_raw_data_since(self, latest_data, region, station_id):
        self.logger.debug("import raw data since %s" % latest_data)

        raw_data = []

        for url_path in self.url_path_generator.get_url_paths(latest_data):

            target_url = self.data_url.build_url(
                os.path.join('Strokes', url_path),
                region=region,
                host='signals')

            data = self.data_provider.read_data(
                target_url,
                region=region,
                station_id=station_id,
                host='signals'
            )

            for line in data.split('\n'):
                try:
                    self.waveform_builder.from_string(line.strip())
                except blitzortung.builder.BuilderError:
                    self.logger.debug("error parsing raw data '%s'" % line)
                    continue
                raw_data.append(self.waveform_builder.build())

        return raw_data


def raw():
    from __init__ import INJECTOR

    return INJECTOR.get(RawSignalsBlitzortungDataProvider)