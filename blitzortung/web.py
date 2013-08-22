"""

@author: awuerl

"""

import sys
import urllib2
import datetime
from urlparse import urlparse

from injector import Module, singleton, provides, inject

import cStringIO
import gzip
import HTMLParser
import shlex
import pytz

import blitzortung


@singleton
class BlitzortungDataTransformer(object):
    single_value_index = {0: 'date', 1: 'time'}

    def transform_entry(self, entry_text):
        entry_text = entry_text.encode('latin1')
        entry_text = HTMLParser.HTMLParser().unescape(entry_text).replace(u'\xa0', ' ')

        parameters = [parameter.decode('latin1') for parameter in shlex.split(entry_text.encode('latin1'))]

        result = {}

        for index, parameter in enumerate(parameters):

            values = parameter.split(';')
            if values:
                if len(values) > 1:
                    parameter_name = unicode(values[0])
                    result[parameter_name] = values[1:]
                else:
                    if index in self.single_value_index:
                        result[self.single_value_index[index]] = values[0]

        return result


class HttpDataTransport(object):
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
            sys.stderr.write("%s when opening '%s'\n" % (error, target_url))
            return None

        response = url_connection.read().strip()

        url_connection.close()

        return response


class BlitzortungDataProvider(object):
    host = 'http://data.blitzortung.org'
    target_url = host + '/Data_%(region)d/Protected/%(url_path)s'

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
                        print "decoding error:", entry_text

        return result

    def process(self, data):
        return data


class BlitzortungStrokeUrlGenerator(object):
    url_path_minute_increment = 10
    url_path_format = 'Strokes/%Y/%m/%d/%H/%M.log'

    def get_url_paths(self, latest_time):
        self.current_time = self.__round_time(latest_time)
        now_utc = datetime.datetime.utcnow()
        now_utc = now_utc.replace(tzinfo=pytz.UTC)
        self.end_time = self.__round_time(now_utc)

        url_paths = []

        while self.current_time <= self.end_time:
            url_paths.append(self.current_time.strftime(self.url_path_format))
            self.current_time += datetime.timedelta(minutes=self.url_path_minute_increment)

        return url_paths

    def __round_time(self, time):
        return time.replace(
            minute=time.minute // self.url_path_minute_increment * self.url_path_minute_increment,
            second=0,
            microsecond=0)


@singleton
class StrokesBlitzortungDataProvider(BlitzortungDataProvider):
    @inject(data_transport=HttpDataTransport, data_transformer=BlitzortungDataTransformer,
            url_path_generator=BlitzortungStrokeUrlGenerator)
    def __init__(self, data_transport, data_transformer, url_path_generator):
        super(StrokesBlitzortungDataProvider, self).__init__(data_transport, data_transformer, None)
        self.url_path_generator = url_path_generator

    def get_strokes_since(self, latest_stroke):
        strokes = []

        for url_path in self.url_path_generator.get_url_paths(latest_stroke):
            initial_stroke_count = len(strokes)
            for stroke_data in self.read_data(url_path=url_path):
                stroke_builder = blitzortung.builder.Stroke()
                stroke_builder.from_data(stroke_data)
                stroke = stroke_builder.build()
                if latest_stroke < stroke.get_timestamp():
                    strokes.append(stroke)
            print url_path, len(strokes) - initial_stroke_count
        return strokes


def strokes():
    from __init__ import INJECTOR

    return INJECTOR.get(StrokesBlitzortungDataProvider)


@singleton
class StationsBlitzortungDataProvider(BlitzortungDataProvider):
    @inject(data_transport=HttpDataTransport, data_transformer=BlitzortungDataTransformer)
    def __init__(self, data_transport, data_transformer):
        super(StationsBlitzortungDataProvider, self).__init__(data_transport, data_transformer, 'stations.txt.gz')

    def process(self, data):
        data = cStringIO.StringIO(data)
        return gzip.GzipFile(fileobj=data).read()


def stations():
    from __init__ import INJECTOR

    return INJECTOR.get(StationsBlitzortungDataProvider)


@singleton
class Raw(BlitzortungDataProvider):
    @inject(data_transport=HttpDataTransport, data_transformer=BlitzortungDataTransformer)
    def __init__(self, data_transport, data_transformer):
        super(Raw, self).__init__(data_transport, data_transformer, 'raw_data/%(station_id)s/%(hour)02d.log')

    def set_station_id(self, station_id):
        self.set_url_parameter('station_id', station_id)

    def set_hour(self, hour):
        self.set_url_parameter('hour', hour)


def raw():
    from __init__ import INJECTOR

    return INJECTOR.get(Raw)


class WebModule(Module):
    @provides(BlitzortungDataTransformer)
    def provide_data_format(self):
        return BlitzortungDataTransformer()
    
