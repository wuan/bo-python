# -*- coding: utf8 -*-

import unittest
import datetime
import urllib2
from nose.tools import raises
import pandas as pd

from hamcrest.library.collection.is_empty import empty
from mock import Mock, patch, call
from hamcrest import assert_that, is_, equal_to, has_item, contains

import blitzortung
from blitzortung.dataimport import split_quote_aware


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
        assert_that(password_manager.mock_calls,
                    has_item(call.add_password(None, 'http://foo.bar/', '<username>', '<password>')))
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
    def test_read_from_url_with_exception(self, build_opener_mock, basic_auth_handler_class_mock,
                                          password_manager_class_mock):
        opener = build_opener_mock()
        opener.open.side_effect = urllib2.URLError("foo")

        response = self.data_transport.read_from_url('http://foo.bar/baz')

        assert_that(response, is_(None))


class BlitzortungDataUrlTest(unittest.TestCase):

    def setUp(self):
        self.data_url = blitzortung.dataimport.BlitzortungDataUrl()

    def test_default_values(self):
        target_url = self.data_url.build_url('url_path')
        assert_that(target_url, is_(equal_to('http://data.blitzortung.org/Data_1/Protected/url_path')))

    def test_specific_values(self):
        target_url = self.data_url.build_url('url_path', host_name='foo', region=42)
        assert_that(target_url, is_(equal_to('http://foo.blitzortung.org/Data_42/Protected/url_path')))


class BlitzortungDataProviderTest(unittest.TestCase):
    def setUp(self):
        self.http_data_transport = Mock()
        self.data_transformer = Mock()

        self.provider = blitzortung.dataimport.BlitzortungDataProvider(self.http_data_transport, self.data_transformer)

    def test_read_data(self):
        response = u"line1 \nline2\näöü".encode('latin1')

        self.http_data_transport.read_from_url.return_value = response
        self.data_transformer.transform_entry.side_effect = ['foo', 'bar', 'baz']

        result = self.provider.read_data('target_url')

        self.http_data_transport.read_from_url.assert_called_with(
            'target_url')

        expected_transformer_calls = \
            [call(u'line1'), call(u'line2'), call(u'äöü')]

        assert_that(
            self.data_transformer.transform_entry.call_args_list,
            equal_to(expected_transformer_calls))
        assert_that(result, contains('foo', 'bar', 'baz'))

    def test_read_data_with_post_process(self):

        self.http_data_transport.read_from_url.return_value = "line\n"

        post_process = Mock()
        post_process.return_value = "processed line\n"

        self.data_transformer.transform_entry.return_value = "transformed"

        result = self.provider.read_data("url_path", pre_process=post_process)

        assert_that(post_process.call_args_list, is_(equal_to([call("line\n")])))
        assert_that(self.data_transformer.transform_entry.call_args_list, is_(equal_to([call("processed line")])))
        assert_that(result, contains("transformed"))

    def test_read_data_with_empty_response(self):
        self.http_data_transport.read_from_url.return_value = ''
        result = self.provider.read_data('foo')

        assert_that(result, equal_to([]))

    def test_read_data_with_encoding_exception(self):
        response = u"line".encode('latin1')

        self.http_data_transport.read_from_url.return_value = response
        self.data_transformer.transform_entry.side_effect = UnicodeDecodeError("foo", "bar", 10, 20, "baz")

        result = self.provider.read_data('foo')

        assert_that(result, is_(empty()))


class BlitzortungHistoryUrlGeneratorTest(unittest.TestCase):
    def create_history_url_generator(self, present_time):
        self.present_time = present_time
        self.start_time = present_time - datetime.timedelta(minutes=25)
        self.strokes_url = blitzortung.dataimport.BlitzortungHistoryUrlGenerator()

    def test_stroke_url_iterator(self):
        self.create_history_url_generator(datetime.datetime(2013, 8, 20, 12, 9, 0))

        urls = [url for url in self.strokes_url.get_url_paths(self.start_time, self.present_time)]

        assert_that(urls, contains(
            '2013/08/20/11/40.log',
            '2013/08/20/11/50.log',
            '2013/08/20/12/00.log'
        ))


class StrokesBlitzortungDataProviderTest(unittest.TestCase):
    def setUp(self):
        self.data_provider = Mock()
        self.data_url = Mock()
        self.url_generator = Mock()
        self.builder = Mock()

        self.provider = blitzortung.dataimport.StrokesBlitzortungDataProvider(
            self.data_provider,
            self.data_url,
            self.url_generator,
            self.builder
        )
        self.provider.read_data = Mock()

    def test_get_strokes_since(self):
        now = datetime.datetime.utcnow()
        latest_stroke_timestamp = now - datetime.timedelta(hours=1)
        self.url_generator.get_url_paths.return_value = ['path1', 'path2']
        stroke_data1 = {'one': 1}
        stroke_data2 = {'two': 2}
        self.data_provider.read_data.side_effect = [[stroke_data1, stroke_data2], []]
        stroke1 = Mock()
        stroke2 = Mock()
        stroke1.get_timestamp.return_value = pd.Timestamp(now - datetime.timedelta(hours=2))
        stroke2.get_timestamp.return_value = pd.Timestamp(now)
        self.builder.from_data.return_value = self.builder
        self.builder.build.side_effect = [stroke1, stroke2]

        strokes = self.provider.get_strokes_since(latest_stroke_timestamp)

        assert_that(strokes, contains(stroke2))

    def test_get_strokes_since_with_builder_error(self):
        now = datetime.datetime.utcnow()
        latest_stroke_timestamp = now - datetime.timedelta(hours=1)
        self.url_generator.get_url_paths.return_value = ['path']
        stroke_data = {'one': 1}
        self.data_provider.read_data.side_effect = [[stroke_data], []]
        self.builder.from_data.return_value = self.builder
        self.builder.build.side_effect = blitzortung.builder.BuilderError("foo")

        strokes = self.provider.get_strokes_since(latest_stroke_timestamp)

        assert_that(strokes, is_(empty()))

    @raises(Exception)
    def test_get_strokes_since_with_generic_exception(self):
        now = datetime.datetime.utcnow()
        latest_stroke_timestamp = now - datetime.timedelta(hours=1)
        self.url_generator.get_url_paths.return_value = ['path']
        stroke_data = {'one': 1}
        self.data_provider.read_data.side_effect = [[stroke_data], []]
        self.builder.from_data.return_value = self.builder
        self.builder.build.side_effect = Exception("foo")

        self.provider.get_strokes_since(latest_stroke_timestamp)


class StationsBlitzortungDataProviderTest(unittest.TestCase):
    def setUp(self):
        self.data_provider = Mock()
        self.data_url = Mock()
        self.builder = Mock()

        self.provider = blitzortung.dataimport.StationsBlitzortungDataProvider(
            self.data_provider,
            self.data_url,
            self.builder
        )
        self.provider.read_data = Mock()

    def test_get_stations(self):
        station_data1 = {'one': 1}
        station_data2 = {'two': 2}
        self.data_url.build_url.return_value = 'full_url'
        self.data_provider.read_data.side_effect = [[station_data1, station_data2], []]
        station1 = Mock()
        station2 = Mock()
        self.builder.from_data.return_value = self.builder
        self.builder.build.side_effect = [station1, station2]

        strokes = self.provider.get_stations()
        expected_args = [call('full_url', post_process=self.provider.post_process)]
        assert_that(self.data_provider.read_data.call_args_list, is_(equal_to(expected_args)))

        assert_that(strokes, contains(station1, station2))

    def test_get_stations_with_builder_error(self):
        station_data = {'one': 1}
        self.data_provider.read_data.side_effect = [[station_data], []]
        self.builder.from_data.return_value = self.builder
        self.builder.build.side_effect = blitzortung.builder.BuilderError("foo")

        strokes = self.provider.get_stations()

        assert_that(strokes, is_(empty()))


class RawSignalsBlitzortungDataProviderTest(unittest.TestCase):
    def setUp(self):
        self.data_provider = Mock()
        self.data_url = Mock()
        self.url_generator = Mock()
        self.builder = Mock()

        self.provider = blitzortung.dataimport.RawSignalsBlitzortungDataProvider(
            self.data_provider,
            self.data_url,
            self.url_generator,
            self.builder
        )
        self.provider.read_data = Mock()

    def test_get_raw_data_since(self):
        last_data = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

        raw_data1 = {'one': 1}
        raw_data2 = {'two': 2}
        self.data_url.build_url.return_value = 'full_url'
        self.data_provider.read_data.side_effect = [[raw_data1, raw_data2], []]
        raw1 = Mock()
        raw2 = Mock()
        self.builder.from_data.return_value = self.builder
        self.builder.build.side_effect = [raw1, raw2]

        region_id = 5
        station_id = 123
        strokes = self.provider.get_raw_data_since(last_data, region_id, station_id)

        assert_that(self.data_)
        expected_args = [call('full_url', post_process=self.provider.post_process)]
        assert_that(self.data_provider.read_data.call_args_list, is_(equal_to(expected_args)))

        assert_that(strokes, contains(raw1, raw2))


class TestStringOp(unittest.TestCase):

    def test_split_quote_aware(self):

        parts = [part for part in split_quote_aware('"eins" "zwei drei vier" "fuenf sechs" sieben')]
        assert_that(parts, is_(equal_to(['eins', 'zwei drei vier', 'fuenf sechs', 'sieben'])))