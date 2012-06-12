'''

@author: awuerl

'''

import sys
import urllib2

import builder

class Url(object):

    host = 'http://blitzortung.net'
    base = host + '/Data_%d/Protected/'

    def __init__(self, baseurl):
        self.url = baseurl
        self.username = ''
        self.password = ''

    def set_credentials(self, username, password):
        self.username = username
	self.password = password

    def read(self):
	password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
	password_mgr.add_password("Blitzortung.org", "http://blitzortung.net", self.username, self.password)
	handler = urllib2.HTTPBasicAuthHandler(password_mgr)

	opener = urllib2.build_opener(handler)

  	try:
	    urlconnection = opener.open(self.url)
        except Error, e:
	    sys.stderr.write("%s when opening '%s'\n" %(e, self.url))
	    return None

	data = urlconnection.read().strip()
	urlconnection.close()

        return data.decode('ISO-8859-1')
    
class StrokesBase(Url):
    
    def __init__(self, base_url):
        super(StrokesBase, self).__init__(base_url)
        
    def get_strokes(self, time_interval=None):
        strokes = [] 
        for line in self.read().split('\n'): 
            if line.strip(): 
                stroke_builder = builder.Stroke() 
                stroke_builder.from_string(line)
                stroke = stroke_builder.build()
                if not time_interval or time_interval.contains(stroke.get_timestamp()):
                    strokes.append(stroke)
        return strokes

class Strokes(StrokesBase):

    def __init__(self, config, region=1):
        super(Strokes, self).__init__(Url.base %(region) + 'strikes.txt')
        self.set_credentials(config.get('USERNAME'), config.get('PASSWORD'))

class Participants(StrokesBase):

    def __init__(self, config, region=1):
        super(Participants, self).__init__(Url.base %(region) + 'participants.txt')
        self.set_credentials(config.get('USERNAME'), config.get('PASSWORD'))

class Stations(Url):

    def __init__(self, config, region=1):
        super(Stations, self).__init__(Url.base %(region) + 'stations.txt')
        self.set_credentials(config.get('USERNAME'), config.get('PASSWORD'))

class Raw(Url):

    def __init__(self, config, region, station_id, offset=0):
        super(Raw, self).__init__(Url.base %(region) + 'raw_data/%s/%02d.log' %(station_id, offset))
        self.set_credentials(config.get('USERNAME'), config.get('PASSWORD'))
