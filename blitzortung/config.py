# -*- coding: utf8 -*-

"""

   Copyright 2014-2016 Andreas Würl

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

"""

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

from injector import Module, singleton, inject, provider


@singleton
class Config:
    @inject
    def __init__(self, config_parser: configparser.ConfigParser):
        self.config_parser = config_parser

    def get_username(self):
        return self.config_parser.get('auth', 'username')

    def get_password(self):
        return self.config_parser.get('auth', 'password')

    def get_raw_path(self):
        return self.config_parser.get('path', 'raw')

    def get_archive_path(self):
        return self.config_parser.get('path', 'archive')

    def get_db_connection_string(self):
        host = self.config_parser.get('db', 'host')
        dbname = self.config_parser.get('db', 'dbname')
        username = self.config_parser.get('db', 'username')
        password = self.config_parser.get('db', 'password')

        return "host='%s' dbname='%s' user='%s' password='%s'" % (host, dbname, username, password)

    def get_webservice_port(self):
        return int(self.config_parser.get('webservice', 'port'))

    def __str__(self):
        return "Config(user: %s, pass: %s)" % (self.get_username(), len(self.get_password()) * '*')


def config():
    from blitzortung import INJECTOR

    return INJECTOR.get(Config)


class ConfigModule(Module):
    @singleton
    @provider
    def provide_config_parser(self) -> configparser.ConfigParser:
        config_parser = configparser.ConfigParser()
        config_parser.read('/etc/blitzortung.conf')
        return config_parser
