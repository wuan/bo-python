# -*- coding: utf8 -*-
import ConfigParser

import unittest
from hamcrest import assert_that, is_, equal_to, instance_of, contains, same_instance, has_item
from mock import Mock, call, patch

import blitzortung


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.config_parser = Mock()
        self.config = blitzortung.config.Config(self.config_parser)

    def test_get_username(self):
        self.config_parser.get.return_value = '<username>'
        assert_that(self.config.get_username(), is_(equal_to('<username>')))
        assert_that(self.config_parser.mock_calls, contains(call.get('auth', 'username')))

    def test_get_password(self):
        self.config_parser.get.return_value = '<password>'
        assert_that(self.config.get_password(), is_(equal_to('<password>')))
        assert_that(self.config_parser.mock_calls, contains(call.get('auth', 'password')))

    def test_get_raw_path(self):
        self.config_parser.get.return_value = '<raw_path>'
        assert_that(self.config.get_raw_path(), is_(equal_to('<raw_path>')))
        assert_that(self.config_parser.mock_calls, contains(call.get('path', 'raw')))

    def test_get_archive_path(self):
        self.config_parser.get.return_value = '<archive_path>'
        assert_that(self.config.get_archive_path(), is_(equal_to('<archive_path>')))
        assert_that(self.config_parser.mock_calls, contains(call.get('path', 'archive')))

    def test_get_db_connection_string(self):
        self.config_parser.get.side_effect = lambda *x: {
            ('db', 'host'): '<host>',
            ('db', 'dbname'): '<dbname>',
            ('db', 'username'): '<username>',
            ('db', 'password'): '<password>'}[x]

        assert_that(self.config.get_db_connection_string(),
                    is_(equal_to("host='<host>' dbname='<dbname>' user='<username>' password='<password>'")))

        assert_that(self.config_parser.mock_calls, contains(
            call.get('db', 'host'),
            call.get('db', 'dbname'),
            call.get('db', 'username'),
            call.get('db', 'password')))

    def test_get_webservice_port(self):
        self.config_parser.get.return_value = 1234
        assert_that(self.config.get_webservice_port(), is_(equal_to(1234)))
        assert_that(self.config_parser.mock_calls, contains(call.get('webservice', 'port')))

    def test_string_representation(self):
        self.config_parser.get.side_effect = lambda *x: {
            ('auth', 'username'): '<username>',
            ('auth', 'password'): '<password>'}[x]

        assert_that(str(self.config), is_(equal_to("Config(user: <username>, pass: **********)")))

        assert_that(self.config_parser.mock_calls, contains(
            call.get('auth', 'username'),
            call.get('auth', 'password')))


class TestConfigModule(unittest.TestCase):

    def setUp(self):
        self.config_module = blitzortung.config.ConfigModule()

    @patch('ConfigParser.ConfigParser')
    def test_provide_config_parser(self, config_parser_class_mock):
        config_parser = self.config_module.provide_config_parser()

        assert_that(config_parser, is_(config_parser_class_mock.return_value))
        assert_that(config_parser_class_mock.mock_calls, has_item(call()))
        assert_that(config_parser.mock_calls, has_item(call.read('/etc/blitzortung.conf')))

    @patch('blitzortung.INJECTOR')
    def test_get_config(self, injector_class_mock):
        config = Mock()
        injector_class_mock.get.return_value = config

        assert_that(blitzortung.config.config(), is_(equal_to(config)))