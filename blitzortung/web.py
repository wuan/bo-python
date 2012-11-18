'''

@author: awuerl

'''

import sys
import urllib2

from injector import inject, singleton

import blitzortung

class Url(object):

    host = 'http://blitzortung.net'
    base_url = host + '/Data_%(region)d/Protected/'

    def __init__(self, url_path, config):
        self.url = self.base_url + url_path
        self.config = config
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
        
        url_string = self.url %(self.parameters)

        try:
            urlconnection = opener.open(url_string, timeout=60)
        except urllib2.URLError, error:
            sys.stderr.write("%s when opening '%s'\n" % (error, url_string))
            return None

        response = urlconnection.read().strip()
        urlconnection.close()

        return response.decode('ISO-8859-1')

class StrokesBase(Url):

    def __init__(self, url_path, config):
        super(StrokesBase, self).__init__(url_path, config)

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

    @inject(config=blitzortung.config.Config)
    def __init__(self, config):
        super(Strokes, self).__init__('strikes.txt', config)

def strokes():
    from __init__ import INJECTOR
    return INJECTOR.get(Strokes)

@singleton
class Participants(StrokesBase):

    @inject(config=blitzortung.config.Config)
    def __init__(self, config):
        super(Participants, self).__init__('participants.txt', config)
        
def participants():
    from __init__ import INJECTOR
    return INJECTOR.get(Participants)

@singleton
class Stations(Url):

    @inject(config=blitzortung.config.Config)
    def __init__(self, config):
        super(Stations, self).__init__('stations.txt', config)

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
