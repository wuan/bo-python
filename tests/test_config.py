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
import os
import sys

import pytest

from assertpy import assert_that
from mock import Mock, call, patch

import blitzortung

config_parser_module = "configparser"


class TestConfig:
    def setup_method(self):
        self.config_parser = Mock()
        self.config = blitzortung.config.Config(self.config_parser)

    def test_get_username(self):
        self.config_parser.get.return_value = '<username>'
        assert_that(self.config.get_username()).is_equal_to('<username>')
        assert_that(self.config_parser.mock_calls).contains(call.get('auth', 'username'))

    def test_get_password(self):
        self.config_parser.get.return_value = '<password>'
        assert_that(self.config.get_password()).is_equal_to('<password>')
        assert_that(self.config_parser.mock_calls).contains(call.get('auth', 'password'))

    def test_get_db_connection_string(self):
        self.config_parser.get.side_effect = lambda *args, **kwargs: {
            ('db', 'host'): '<host>',
            ('db', 'port'): '<port>',
            ('db', 'dbname'): '<dbname>',
            ('db', 'username'): '<username>',
            ('db', 'password'): '<password>'}[args]

        assert_that(self.config.get_db_connection_string()) \
            .is_equal_to("host=<host> port=<port> dbname=<dbname> user=<username> password=<password>")

        assert_that(self.config_parser.mock_calls).contains(
            call.get('db', 'host'),
            call.get('db', 'port', fallback='5432'),
            call.get('db', 'dbname'),
            call.get('db', 'username'),
            call.get('db', 'password'))

    def test_get_webservice_port(self):
        self.config_parser.get.return_value = 1234
        assert_that(self.config.get_webservice_port()).is_equal_to(1234)
        assert_that(self.config_parser.mock_calls).contains(call.get('webservice', 'port'))

    def test_get_db_connection_count_default(self):
        self.config_parser.get.side_effect = lambda *args, **kwargs: kwargs.get('fallback')
        assert_that(self.config.get_db_connection_count()).is_equal_to(3)
        assert_that(self.config_parser.mock_calls).contains(
            call.get('db', 'connection_count', fallback='3'))

    def test_get_db_connection_count_configured(self):
        self.config_parser.get.return_value = '12'
        assert_that(self.config.get_db_connection_count()).is_equal_to(12)
        assert_that(self.config_parser.mock_calls).contains(
            call.get('db', 'connection_count', fallback='3'))

    def test_get_db_min_connection_count_default(self):
        self.config_parser.get.side_effect = lambda *args, **kwargs: kwargs.get('fallback')
        assert_that(self.config.get_db_min_connection_count()).is_equal_to(4)
        assert_that(self.config_parser.mock_calls).contains(
            call.get('db', 'min_connections', fallback='4'))

    def test_get_db_max_connection_count_default(self):
        self.config_parser.get.side_effect = lambda *args, **kwargs: kwargs.get('fallback')
        assert_that(self.config.get_db_max_connection_count()).is_equal_to(50)
        assert_that(self.config_parser.mock_calls).contains(
            call.get('db', 'max_connections', fallback='50'))

    def test_get_db_pool_sizes_configured(self):
        self.config_parser.get.side_effect = lambda *args, **kwargs: {
            ('db', 'min_connections'): '2',
            ('db', 'max_connections'): '7'}[args]
        assert_that(self.config.get_db_min_connection_count()).is_equal_to(2)
        assert_that(self.config.get_db_max_connection_count()).is_equal_to(7)

    def test_string_representation(self):
        self.config_parser.get.side_effect = lambda *x: {
            ('auth', 'username'): '<username>',
            ('auth', 'password'): '<password>'}[x]

        assert_that(str(self.config)).is_equal_to("Config(user: <username>, pass: **********)")

        assert_that(self.config_parser.mock_calls).contains(
            call.get('auth', 'username'),
            call.get('auth', 'password'))


class TestConfigModule:

    def setup_method(self):
        self.config_module = blitzortung.config.ConfigModule()

    @patch("configparser.ConfigParser")
    def test_provide_config_parser(self, config_parser_class_mock, tmp_path):
        with open(os.path.join(tmp_path, "blitzortung.conf"), "w") as config_file:
            config_file.write("\n")

        os.chdir(tmp_path)

        config_parser = self.config_module.provide_config_parser()

        assert_that(config_parser).is_equal_to(config_parser_class_mock.return_value)
        assert_that(config_parser_class_mock.mock_calls).contains(call())
        assert_that(config_parser.mock_calls).contains(call.read('./blitzortung.conf'))

    @patch('blitzortung.INJECTOR')
    def test_get_config(self, injector_class_mock):
        config = Mock()
        injector_class_mock.get.return_value = config

        assert_that(blitzortung.config.config()).is_equal_to(config)

    def test_provide_config_parser_raises_without_config_file(self):
        with patch.object(blitzortung.config.ConfigModule, "find_config_file_path", return_value=None):
            with pytest.raises(ValueError, match="No configuration file found"):
                self.config_module.provide_config_parser()

    def test_find_config_file_path_returns_none(self):
        with patch("blitzortung.config.os.path.exists", return_value=False) as exists:
            assert_that(self.config_module.find_config_file_path()).is_none()

        assert_that(exists.call_count).is_equal_to(2)
        assert_that(exists.call_args_list).contains(
            call(os.path.join(".", "blitzortung.conf")),
            call(os.path.join("/etc/", "blitzortung.conf")),
        )
