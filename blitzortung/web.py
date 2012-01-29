'''

@author: awuerl

'''

import urllib

class Url:

    def __init__(self, baseurl):
        self.url = baseurl
        self.protocol = 'http'
        self.credentials = ''

    def add(self, name, value):
        self.url += '&' + str(name).strip() + '=' + str(value).strip()

    def setCredentials(self, username, password):
        self.credentials = username + ':' + password + '@'

    def get(self):
        return self.protocol + '://' + self.credentials + self.url

    def read(self):
        urlconnection = urllib.urlopen(self.get())

        data = urlconnection.read().strip()

        urlconnection.close()

        return data

class Strokes(Url):

    def __init__(self, config):
        Url.__init__(self, 'blitzortung.tmt.de/Data/Protected/strikes.txt')
        self.setCredentials(config.get('USERNAME'), config.get('PASSWORD'))


class Participants(Url):

    def __init__(self, config):
        Url.__init__(self, 'blitzortung.tmt.de/Data/Protected/participants.txt')
        self.setCredentials(config.get('USERNAME'), config.get('PASSWORD'))

