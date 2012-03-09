'''

@author: awuerl

'''

import urllib

import builder

class Url(object):

    base = 'blitzortung.net/Data/Protected/'

    def __init__(self, baseurl):
        self.url = baseurl
        self.protocol = 'http'
        self.credentials = ''

    def add(self, name, value):
        self.url += '&' + str(name).strip() + '=' + str(value).strip()

    def set_credentials(self, username, password):
        self.credentials = username + ':' + password + '@'

    def get(self):
        return self.protocol + '://' + self.credentials + self.url

    def read(self):
        urlconnection = urllib.urlopen(self.get())

        data = urlconnection.read().strip()

        urlconnection.close()

        return unicode(data)
    
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

    def __init__(self, config):
        super(Strokes, self).__init__(Url.base + 'strikes.txt')
        self.set_credentials(config.get('USERNAME'), config.get('PASSWORD'))

class Participants(StrokesBase):

    def __init__(self, config):
        super(Participants, self).__init__(Url.base + 'participants.txt')
        self.set_credentials(config.get('USERNAME'), config.get('PASSWORD'))



class Stations(Url):

    def __init__(self, config):
        super(Stations, self).__init__(Url.base + 'stations.txt')
        self.set_credentials(config.get('USERNAME'), config.get('PASSWORD'))