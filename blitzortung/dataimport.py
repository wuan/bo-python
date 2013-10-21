# -*- coding: utf8 -*-

"""

@author: awuerl

"""
import os
import logging
import time
import urllib2
import datetime
from urlparse import urlparse
import cStringIO
import gzip
import HTMLParser
import shlex

from injector import singleton, inject

import blitzortung


@singleton
class BlitzortungDataTransformer(object):
    single_value_index = {0: u'date', 1: u'time'}

    def transform_entry(self, entry_text):
        entry_text = entry_text.encode('latin1')
        entry_text = HTMLParser.HTMLParser().unescape(entry_text).replace(u'\xa0', ' ')

        parameters = [parameter.decode('latin1') for parameter in shlex.split(entry_text.encode('latin1'))]

        result = {}

        for index, parameter in enumerate(parameters):

            values = parameter.split(u';')
            if values:
                if len(values) > 1:
                    parameter_name = unicode(values[0])
                    result[parameter_name] = values[1] if len(values) == 2 else values[1:]
                else:
                    if index in self.single_value_index:
                        result[self.single_value_index[index]] = values[0]

        return result


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
            self.logger.debug("%s when opening '%s'\n" % (error, target_url))
            return None

        data_string = url_connection.read().strip()

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

    @inject(http_data_transport=HttpDataTransport, data_transformer=BlitzortungDataTransformer)
    def __init__(self, http_data_transport, data_transformer):
        self.http_data_transport = http_data_transport
        self.data_transformer = data_transformer

    def read_text(self, target_url):

        return self.http_data_transport.read_from_url(target_url)

    def read_data(self, target_url, post_process=None):
        response = self.read_text(target_url)

        result = []

        if response:
            if post_process:
                response = post_process(response)

            for entry_text in response.split('\n'):
                entry_text = entry_text.strip()
                entry_text = entry_text.decode('latin1')
                if entry_text:
                    try:
                        result.append(self.data_transformer.transform_entry(entry_text))
                    except UnicodeDecodeError:
                        self.logger.debug("decoding error: '%s'" % entry_text)

        return result


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

    def get_strokes_since(self, latest_stroke, region=1):
        self.logger.debug("import strokes since %s" % latest_stroke)
        strokes_since = []

        for url_path in self.url_path_generator.get_url_paths(latest_stroke):
            initial_stroke_count = len(strokes_since)
            start_time = time.time()
            target_url = self.data_url.build_url(os.path.join('Strokes', url_path), region=region)
            for stroke_data in self.data_provider.read_data(target_url):
                try:
                    stroke = self.stroke_builder.from_data(stroke_data).build()
                except blitzortung.builder.BuilderError as e:
                    self.logger.warn("%s: %s (%s)" % (e.__class__, e.message, stroke_data))
                    continue
                except Exception as e:
                    self.logger.error("%s: %s (%s)" % (e.__class__, e.message, stroke_data))
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
        for station_data in self.data_provider.read_data(target_url, post_process=self.post_process):
            try:
                current_stations.append(self.station_builder.from_data(station_data).build())
            except blitzortung.builder.BuilderError:
                self.logger.debug("error parsing station data '%s'" % station_data)
        return current_stations

    def post_process(self, data):
        data = cStringIO.StringIO(data)
        return gzip.GzipFile(fileobj=data).read()


def stations():
    from __init__ import INJECTOR

    return INJECTOR.get(StationsBlitzortungDataProvider)


@singleton
class RawSignalsBlitzortungDataProvider(object):

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
            data = self.data_provider.read_data(
                url_path,
                region=region,
                station_id=station_id,
                host='signals')

            for line in data.split('\n'):
                self.waveform_builder.from_string(line.strip())
                raw_data.append(self.waveform_builder.build())

        return raw_data


def raw():
    from __init__ import INJECTOR

    return INJECTOR.get(RawSignalsBlitzortungDataProvider)