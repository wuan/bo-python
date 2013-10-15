# -*- coding: utf8 -*-

import unittest
import datetime
import urllib2
from mock import Mock, patch, call
from hamcrest import assert_that, is_, equal_to, has_item, contains

import blitzortung


class BlitzortungDataTransformerTest(unittest.TestCase):

    def setUp(self):
        self.data_format = blitzortung.dataimport.BlitzortungDataTransformer()

    def test_parse_line(self):
        result = self.data_format.transform_entry(u'foo;1;2 bar;3;4 baz;"single value"')

        assert_that(result, is_(equal_to({u'baz': u'single value', u'foo': [u'1', u'2'], u'bar': [u'3', u'4']})))

    def test_parse_line_with_space(self):
        result = self.data_format.transform_entry(u'foo;1;2 "foo bar";3;4')

        assert_that(result, is_(equal_to({u'foo bar': [u'3', u'4'], u'foo': [u'1', u'2']})))

    def test_parse_line_with_html(self):
        result = self.data_format.transform_entry(u'foo;b&auml;z;&szlig; "f&ouml;o&nbsp;b&auml;r";3;4')

        assert_that(result, is_(equal_to({u'föo bär': [u'3', u'4'], u'foo': [u'bäz', u'ß']})))

    def test_parse_stroke_data_line(self):
        result = self.data_format.transform_entry(
            u"2013-08-08 10:30:03.644038642 pos;44.162701;8.931001;0 str;4.75 typ;0 dev;20146 sta;10;24;226,529,391,233,145,398,425,533,701,336,336,515,434,392,439,283,674,573,559,364,111,43,582,594")

        assert_that(result, is_(equal_to({u'sta': [u'10', u'24',
                                                   u'226,529,391,233,145,398,425,533,701,336,336,515,434,392,439,283,674,573,559,364,111,43,582,594'],
                                          u'pos': [u'44.162701', u'8.931001', u'0'], u'dev': u'20146', u'str': u'4.75',
                                          u'time': u'10:30:03.644038642', u'date': u'2013-08-08', u'typ': u'0'})))


class HttpDataTransportTest(unittest.TestCase):

    def setUp(self):
        self.config = Mock()
        self.config.get_username.return_value = '<username>'
        self.config.get_password.return_value = '<password>'

        self.data_transport = blitzortung.dataimport.HttpDataTransport(self.config)

    @patch('urllib2.HTTPPasswordMgrWithDefaultRealm')
    @patch('urllib2.HTTPBasicAuthHandler')
    @patch('urllib2.build_opener')
    def test_read_from_url(self, build_opener_mock, basic_auth_handler_class_mock, password_manager_class_mock):
        response = self.data_transport.read_from_url('http://foo.bar/baz')

        password_manager = password_manager_class_mock()
        assert_that(password_manager.mock_calls, has_item(call.add_password(None, 'http://foo.bar/', '<username>', '<password>')))
        assert_that(basic_auth_handler_class_mock.mock_calls, has_item(call(password_manager)))
        handler = basic_auth_handler_class_mock()
        assert_that(build_opener_mock.mock_calls, has_item(call(handler)))
        opener = build_opener_mock()
        url_connection = opener.open.return_value
        unstripped_response = url_connection.read.return_value
        stripped_response = unstripped_response.strip.return_value
        assert_that(response, is_(equal_to(stripped_response)))

        assert_that(url_connection.mock_calls, has_item(call.close()))

    @patch('urllib2.HTTPPasswordMgrWithDefaultRealm')
    @patch('urllib2.HTTPBasicAuthHandler')
    @patch('urllib2.build_opener')
    def test_read_from_url_with_exception(self, build_opener_mock, basic_auth_handler_class_mock, password_manager_class_mock):
        opener = build_opener_mock()
        opener.open.side_effect = urllib2.URLError("foo")

        response = self.data_transport.read_from_url('http://foo.bar/baz')

        assert_that(response, is_(None))


class StrokesUrlTest(unittest.TestCase):
    def create_strokes_url_generator(self, present_time):
        self.present_time = present_time
        self.start_time = present_time - datetime.timedelta(minutes=25)
        self.strokes_url = blitzortung.dataimport.BlitzortungStrokeUrlGenerator()

    def test_stroke_url_iterator(self):
        self.create_strokes_url_generator(datetime.datetime(2013, 8, 20, 12, 9, 0))

        urls = [url for url in self.strokes_url.get_url_paths(self.start_time, self.present_time)]

        assert_that(urls, contains(
            'Strokes/2013/08/20/11/40.log',
            'Strokes/2013/08/20/11/50.log',
            'Strokes/2013/08/20/12/00.log'
        ))
