# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''
import ConfigParser

class Config(object):
    def __init__(self, configfilename='/etc/blitzortung.conf'):

        self.config = ConfigParser.ConfigParser()
        self.config.read(configfilename)
        
    def get_username(self):
        return self.config.get('auth', 'username')
    
    def get_password(self):
        return self.config.get('auth', 'password')
    
    def get_raw_path(self):
        return self.config.get('path', 'raw')
    
    def get_archive_path(self):
        return self.config.get('path', 'archive')