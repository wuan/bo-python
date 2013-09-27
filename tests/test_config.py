# -*- coding: utf8 -*-
import ConfigParser

import unittest
import datetime
from hamcrest import assert_that, is_, equal_to, instance_of
from mockito import mock, when, verify
import nose
import pytz
import numpy as np
import pandas as pd

import blitzortung


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.config_parser = mock()
        self.config = blitzortung.config.Config(self.config_parser)

    def test_get_username(self):
        when(self.config_parser).get('auth', 'username').thenReturn('<username>')
        assert_that(self.config.get_username(), is_(equal_to('<username>')))
        verify(self.config_parser).get('auth', 'username')

    def test_get_password(self):
        when(self.config_parser).get('auth', 'password').thenReturn('<password>')
        assert_that(self.config.get_password(), is_(equal_to('<password>')))
        verify(self.config_parser).get('auth', 'password')

    def test_get_raw_path(self):
        when(self.config_parser).get('path', 'raw').thenReturn('<raw_path>')
        assert_that(self.config.get_raw_path(), is_(equal_to('<raw_path>')))
        verify(self.config_parser).get('path', 'raw')

    def test_get_archive_path(self):
        when(self.config_parser).get('path', 'archive').thenReturn('<archive_path>')
        assert_that(self.config.get_archive_path(), is_(equal_to('<archive_path>')))
        verify(self.config_parser).get('path', 'archive')

    def test_get_db_connection_string(self):
        when(self.config_parser).get('db', 'host').thenReturn('<host>')
        when(self.config_parser).get('db', 'dbname').thenReturn('<dbname>')
        when(self.config_parser).get('db', 'username').thenReturn('<username>')
        when(self.config_parser).get('db', 'password').thenReturn('<password>')

        assert_that(self.config.get_db_connection_string(),
                    is_(equal_to("host='<host>' dbname='<dbname>' user='<username>' password='<password>'")))

        verify(self.config_parser).get('db', 'host')
        verify(self.config_parser).get('db', 'dbname')
        verify(self.config_parser).get('db', 'username')
        verify(self.config_parser).get('db', 'password')

    def test_get_webservice_port(self):
        when(self.config_parser).get('webservice', 'port').thenReturn(1234)
        assert_that(self.config.get_webservice_port(), is_(equal_to(1234)))
        verify(self.config_parser).get('webservice', 'port')

    def test_string_representation(self):
        when(self.config_parser).get('auth', 'username').thenReturn('<username>')
        when(self.config_parser).get('auth', 'password').thenReturn('<password>')

        assert_that(str(self.config), is_(equal_to("Config(user: <username>, pass: **********)")))


class TestConfigModule(unittest.TestCase):

    def setUp(self):
        self.config_module = blitzortung.config.ConfigModule()

    def test_provide_config_parser(self):
        config_parser = self.config_module.provide_config_parser()

        assert_that(config_parser, is_(instance_of(ConfigParser.ConfigParser)))