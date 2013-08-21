"""

@author: awuerl

"""

import sys
import urllib2

from injector import Module, singleton, provides, inject

import cStringIO
import gzip
import HTMLParser
import shlex

import blitzortung


class Url(object):
    host = 'http://data.blitzortung.org'
    base_url = host + '/Data_%(region)d/Protected/'

    def __init__(self, url_path, config, data_format):
        self.url = self.base_url + url_path
        self.config = config
        self.data_format = data_format
        self.parameters = {'region': 1}

    def set_parameter(self, key, value):
        self.parameters[key] = value

    def set_region(self, region):
        self.set_parameter('region', region)

    def read(self):
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password("Blitzortung.org", Url.host, self.config.get_username(), self.config.get_password())
        handler = urllib2.HTTPBasicAuthHandler(password_mgr)

        opener = urllib2.build_opener(handler)

        url_string = self.url % self.parameters

        try:
            url_connection = opener.open(url_string, timeout=60)
        except urllib2.URLError, error:
            sys.stderr.write("%s when opening '%s'\n" % (error, url_string))
            return None

        response = url_connection.read().strip()
        url_connection.close()

        response = self.process(response)

        result = []
        for line in response.split('\n'):
            line = line.strip()
            if line:
                result.append(self.data_format.parse_line(line))

        return result

    def process(self, data):
        return data


class StrokesBase(Url):
    def __init__(self, url_path, config, data_format):
        super(StrokesBase, self).__init__(url_path, config, data_format)

    def get_strokes(self, time_interval=None):
        strokes = []
        for line in self.read().split('\n'):
            if line.strip():
                stroke_builder = blitzortung.builder.Stroke()
                stroke_builder.from_string(line)
                stroke = stroke_builder.build()
                if not time_interval or time_interval.contains(stroke.get_timestamp()):
                    strokes.append(stroke)
        return strokes


@singleton
class Strokes(StrokesBase):
    @inject(config=blitzortung.config.Config, data_format=blitzortung.web.DataFormat)
    def __init__(self, config, data_format):
        super(Strokes, self).__init__('strikes.txt', config, data_format)


def strokes():
    from __init__ import INJECTOR

    return INJECTOR.get(Strokes)


@singleton
class Participants(StrokesBase):
    @inject(config=blitzortung.config.Config, data_format=blitzortung.web.DataFormat)
    def __init__(self, config, data_format):
        super(Participants, self).__init__('participants.txt', config, data_format)


def participants():
    from __init__ import INJECTOR

    return INJECTOR.get(Participants)


@singleton
class Stations(Url):
    @inject(config=blitzortung.config.Config, data_format=blitzortung.web.DataFormat)
    def __init__(self, config, data_format):
        super(Stations, self).__init__('stations.txt.gz', config, data_format)

    def process(self, data):
        data = cStringIO.StringIO(data)
        return gzip.GzipFile(fileobj=data).read()


def stations():
    from __init__ import INJECTOR

    return INJECTOR.get(Stations)


@singleton
class Raw(Url):
    @inject(config=blitzortung.config.Config)
    def __init__(self, config):
        super(Raw, self).__init__('raw_data/%(station_id)s/%(hour)02d.log', config)

    def set_station_id(self, station_id):
        self.set_parameter('station_id', station_id)

    def set_hour(self, hour):
        self.set_parameter('hour', hour)


def raw():
    from __init__ import INJECTOR

    return INJECTOR.get(Raw)


@singleton
class DataFormat(object):
    def parse_line(self, line):
        line = HTMLParser.HTMLParser().unescape(line).replace(u'\xa0', ' ').encode('latin1')

        parameters = shlex.split(line)

        result = {}

        for parameter in parameters:

            values = parameter.decode('latin1').split(';')
            if values and len(values) > 1:
                parameter_name = unicode(values[0])
                result[parameter_name] = values[1:]

        return result


def dataFormat():
    from __init__ import INJECTOR

    return INJECTOR.get(DataFormat)


class WebModule(Module):

    @singleton
    @provides(DataFormat)
    def provide_data_format(self):
        return DataFormat()
