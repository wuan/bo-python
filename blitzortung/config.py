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

from __future__ import annotations

import configparser
import os
from typing import Optional

from injector import Module, singleton, inject, provider


def _quote_conninfo_value(value: str) -> str:
    """Quote a value for use in a PostgreSQL connection string.

    Mirrors the escaping performed by ``psycopg2.extensions.make_dsn`` so that
    hosts, usernames or passwords containing spaces, single quotes or
    backslashes are handled correctly. ``make_dsn`` itself cannot be used here
    because ``psycopg2cffi`` (the PyPy drop-in replacement) does not provide
    it, and importing ``psycopg2`` at this point would bypass the
    ``psycopg2cffi`` compatibility registration.
    """
    value = str(value)
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    if value == "" or any(char.isspace() for char in value):
        return "'%s'" % escaped
    return escaped


@singleton
class Config:
    config_parser: configparser.ConfigParser

    @inject
    def __init__(self, config_parser: configparser.ConfigParser) -> None:
        self.config_parser = config_parser

    def get_username(self) -> str:
        return self.config_parser.get('auth', 'username')

    def get_password(self) -> str:
        return self.config_parser.get('auth', 'password')

    def get_db_connection_string(self) -> str:
        host = self.config_parser.get('db', 'host')
        port = self.config_parser.get('db', 'port', fallback='5432')
        dbname = self.config_parser.get('db', 'dbname')
        username = self.config_parser.get('db', 'username')
        password = self.config_parser.get('db', 'password')

        # Escape values (quotes, spaces, backslashes) like ``make_dsn`` does,
        # unlike naive string interpolation.
        return " ".join(
            "%s=%s" % (key, _quote_conninfo_value(value))
            for key, value in (
                ("host", host),
                ("port", port),
                ("dbname", dbname),
                ("user", username),
                ("password", password),
            )
        )

    def get_webservice_port(self) -> int:
        return int(self.config_parser.get('webservice', 'port'))

    def get_db_connection_count(self) -> int:
        """Return the number of pooled database connections to open.

        Each grid request issues a grid and a histogram query concurrently, so
        the pool size directly caps the number of requests that can be served
        in parallel.  Defaults to the txpostgres default of 3 to preserve the
        previous behaviour when the option is absent.
        """
        return int(self.config_parser.get('db', 'connection_count', fallback='3'))

    def __str__(self) -> str:
        return "Config(user: %s, pass: %s)" % (self.get_username(), len(self.get_password()) * '*')


def config() -> Config:
    from blitzortung import INJECTOR

    result: Config = INJECTOR.get(Config)
    return result


class ConfigModule(Module):
    @singleton
    @provider
    def provide_config_parser(self) -> configparser.ConfigParser:
        config_file_path = self.find_config_file_path()

        if config_file_path is None:
            raise ValueError("No configuration file found")

        config_parser = configparser.ConfigParser()
        config_parser.read(config_file_path)
        return config_parser

    def find_config_file_path(self) -> Optional[str]:
        config_file_name = "blitzortung.conf"
        for config_dir_name in [".", "/etc/"]:
            config_file_path = os.path.join(config_dir_name, config_file_name)
            if os.path.exists(config_file_path):
                return config_file_path
        return None
