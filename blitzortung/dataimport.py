# -*- coding: utf8 -*-

"""

@author: awuerl

"""
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

        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password("blitzortung.org", parsed_url.hostname, self.config.get_username(),
                                  self.config.get_password())
        handler = urllib2.HTTPBasicAuthHandler(password_mgr)

        opener = urllib2.build_opener(handler)

        try:
            url_connection = opener.open(target_url, timeout=60)
        except urllib2.URLError, error:
            self.logger.debug("%s when opening '%s'\n" % (error, target_url))
            return None

        response = url_connection.read().strip()

        url_connection.close()

        return response


class BlitzortungDataProvider(object):
    host = 'http://data.blitzortung.org'
    target_url = host + '/Data_%(region)d/Protected/%(url_path)s'
    logger = logging.getLogger(__name__)

    def __init__(self, http_data_transport, data_transformer, url_path=None):
        self.http_data_transport = http_data_transport
        self.data_transformer = data_transformer
        self.url_parameters = {
            'region': 1,
            'url_path': url_path if url_path else '',
        }

    def set_url_parameter(self, parameter_key, parameter_value):
        self.url_parameters[parameter_key] = parameter_value

    def set_region(self, region):
        self.set_url_parameter('region', region)

    def read_data(self, **kwargs):
        url_parameters = self.url_parameters.copy()
        url_parameters.update(kwargs)

        target_url = self.target_url % url_parameters

        response = self.http_data_transport.read_from_url(target_url)

        result = []

        if response:
            response = self.process(response)

            for entry_text in response.split('\n'):
                entry_text = entry_text.strip()
                entry_text = entry_text.decode('latin1')
                if entry_text:
                    try:
                        result.append(self.data_transformer.transform_entry(entry_text))
                    except UnicodeDecodeError:
                        self.logger.debug("decoding error: '%s'" % entry_text)

        return result

    def process(self, data):
        return data


class BlitzortungStrokeUrlGenerator(object):
    url_path_minute_increment = 10
    url_path_format = 'Strokes/%Y/%m/%d/%H/%M.log'

    def __init__(self):
        self.duration = datetime.timedelta(minutes=self.url_path_minute_increment)

    def get_url_paths(self, start_time, end_time=None):
        for interval_start_time in blitzortung.util.time_intervals(start_time, self.duration, end_time):
            yield interval_start_time.strftime(self.url_path_format)


@singleton
class StrokesBlitzortungDataProvider(BlitzortungDataProvider):
    logger = logging.getLogger(__name__)

    @inject(data_transport=HttpDataTransport, data_transformer=BlitzortungDataTransformer,
            url_path_generator=BlitzortungStrokeUrlGenerator, stroke_builder=blitzortung.builder.Stroke)
    def __init__(self, data_transport, data_transformer, url_path_generator, stroke_builder):
        super(StrokesBlitzortungDataProvider, self).__init__(data_transport, data_transformer, None)
        self.url_path_generator = url_path_generator
        self.stroke_builder = stroke_builder

    def get_strokes_since(self, latest_stroke):
        self.logger.debug("import strokes since %s" % latest_stroke)
        strokes_since = []

        for url_path in self.url_path_generator.get_url_paths(latest_stroke):
            initial_stroke_count = len(strokes_since)
            start_time = time.time()
            for stroke_data in self.read_data(url_path=url_path):
                try:
                    stroke = self.stroke_builder.from_data(stroke_data).build()
                except Exception as e:
                    self.logger.error("%s: %s (%s)" % (e.__class__, e.message, stroke_data))
                    raise e
                timestamp = stroke.get_timestamp()
                timestamp.nanoseconds = 0
                if latest_stroke < timestamp:
                    strokes_since.append(stroke)
            end_time = time.time()
            self.logger.debug("imported %d strokes in %.2fs from %s", len(strokes_since) - initial_stroke_count,
                              end_time - start_time, url_path)
        return strokes_since


def strokes():
    from __init__ import INJECTOR

    return INJECTOR.get(StrokesBlitzortungDataProvider)


@singleton
class StationsBlitzortungDataProvider(BlitzortungDataProvider):
    logger = logging.getLogger(__name__)

    @inject(data_transport=HttpDataTransport, data_transformer=BlitzortungDataTransformer,
            station_builder=blitzortung.builder.Station)
    def __init__(self, data_transport, data_transformer, station_builder):
        super(StationsBlitzortungDataProvider, self).__init__(data_transport, data_transformer, 'stations.txt.gz')
        self.station_builder = station_builder

    def get_stations(self):
        current_stations = []
        for station_data in self.read_data():
            try:
                current_stations.append(self.station_builder.from_data(station_data).build())
            except blitzortung.builder.BuilderError:
                self.logger.debug("error parsing station data '%s'" % station_data)
        return current_stations

    def process(self, data):
        data = cStringIO.StringIO(data)
        return gzip.GzipFile(fileobj=data).read()


def stations():
    from __init__ import INJECTOR

    return INJECTOR.get(StationsBlitzortungDataProvider)


@singleton
class RawSignalsBlitzortungDataProvider(BlitzortungDataProvider):
    @inject(data_transport=HttpDataTransport, data_transformer=BlitzortungDataTransformer)
    def __init__(self, data_transport, data_transformer):
        super(RawSignalsBlitzortungDataProvider, self).__init__(data_transport, data_transformer,
                                                                'raw_data/%(station_id)s/%(hour)02d.log')
        # http://signals.blitzortung.org/Data_1/<station_id>/2013/09/28/20/00.log

    def set_station_id(self, station_id):
        self.set_url_parameter('station_id', station_id)

    def set_hour(self, hour):
        self.set_url_parameter('hour', hour)


def raw():
    from __init__ import INJECTOR

    return INJECTOR.get(RawSignalsBlitzortungDataProvider)