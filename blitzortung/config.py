# -*- coding: utf8 -*-

from injector import Key, Module, singleton
import ConfigParser

configuration = Key('configuration')

class ConfigFile(object):
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
    
    def get_db_connection_string(self):
        host = self.config.get('db', 'host')
        dbname = self.config.get('db', 'dbname')
        username = self.config.get('db', 'username')
        password = self.config.get('db', 'password')
        
        return "host='%s' dbname='%s' user='%s' password='%s'" % (host, dbname, username, password)
        
        
    def get_dict(self):
        config = {}
        config['username'] = self.get_username()
        config['password'] = self.get_password()
        config['raw_path'] = self.get_raw_path()
        config['archive_path'] = self.get_archive_path()
        config['db_connection_string'] = self.get_db_connection_string()
        return config
        

class Config(Module):
    def configure(self, binder):
        config_file = ConfigFile()

        binder.bind(configuration, to=config_file.get_dict(), scope=singleton)
