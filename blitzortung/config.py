# -*- coding: utf8 -*-

import ConfigParser

from injector import Module, singleton, provides


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

    def get_db_connection_string(self):
        host = self.config.get('db', 'host')
        dbname = self.config.get('db', 'dbname')
        username = self.config.get('db', 'username')
        password = self.config.get('db', 'password')

        return "host='%s' dbname='%s' user='%s' password='%s'" % (host, dbname, username, password)

    def get_webservice_port(self):
        return int(self.config.get('webservice', 'port'))

    def __str__(self):
        return "user: %s, pass: %s" % (self.get_username(), len(self.get_password()) * '*')


def config():
    from __init__ import INJECTOR

    return INJECTOR.get(Config)


class ConfigModule(Module):
    @singleton
    @provides(Config)
    def provide_config(self):
        return Config()
