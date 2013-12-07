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

        self.provider = blitzortung.dataimport.BlitzortungDataProvider(self.http_data_transport)

    def test_read_data(self):
        response = u"line1 \nline2\näöü\n\n"

        self.http_data_transport.read_from_url.return_value = response

        result = self.provider.read_data('target_url')

        assert_that(list(result), contains(u'line1', u'line2', u'äöü'))

        self.http_data_transport.read_from_url.assert_called_with(
            'target_url')

    def test_read_data_with_post_process(self):
        self.http_data_transport.read_from_url.return_value = "line\n"

        pre_process = Mock()
        pre_process.return_value = "processed line\n"

        result = self.provider.read_data("url_path", pre_process=pre_process)

        assert_that(list(result), contains("processed line"))
        assert_that(pre_process.call_args_list, is_(equal_to([call("line\n")])))

    def test_read_data_with_empty_response(self):
        self.http_data_transport.read_from_url.return_value = ''
        result = self.provider.read_data('foo')

        assert_that(list(result), equal_to([]))


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
        self.builder.from_line.return_value = self.builder
        self.builder.build.side_effect = [stroke1, stroke2]

        strokes = self.provider.get_strokes_since(latest_stroke_timestamp)

        assert_that(list(strokes), contains(stroke2))

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
        self.data_provider.read_line.side_effect = [[stroke_data], []]
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
        station_data1 = "station one"
        station_data2 = "station two"
        self.data_url.build_url.return_value = 'full_url'
        self.data_provider.read_data.side_effect = [[station_data1, station_data2], []]
        station1 = Mock()
        station2 = Mock()
        self.builder.from_line.return_value = self.builder
        self.builder.build.side_effect = [station1, station2]

        stations = self.provider.get_stations()
        expected_args = [call('full_url', pre_process=self.provider.pre_process)]
        assert_that(self.data_provider.read_data.call_args_list, is_(equal_to(expected_args)))

        assert_that(stations, contains(station1, station2))

    def test_get_stations_with_builder_error(self):
        station_data = {'one': 1}
        self.data_provider.read_data.side_effect = [[station_data], []]
        self.builder.from_line.return_value = self.builder
        self.builder.build.side_effect = blitzortung.builder.BuilderError("foo")

        strokes = self.provider.get_stations()

        assert_that(list(strokes), is_(empty()))


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

        raw_data1 = Mock()
        raw_data2 = Mock()
        self.data_url.build_url.return_value = 'full_url'
        self.url_generator.get_url_paths.return_value = ['url_path1', 'url_path2']
        self.data_provider.read_data.side_effect = [raw_data1, raw_data2],
        raw11 = Mock()
        raw12 = Mock()
        raw21 = Mock()
        raw22 = Mock()
        self.builder.from_string.return_value = self.builder
        self.builder.build.side_effect = [raw11, raw12, raw21, raw22]

        region_id = 5
        station_id = 123
        strokes = self.provider.get_raw_data_since(last_data, region_id, station_id)

        assert_that(self.data_)
        expected_args = [call('full_url', post_process=self.provider.post_process)]
        assert_that(self.data_provider.read_data.call_args_list, is_(equal_to(expected_args)))

        assert_that(strokes, contains(raw1, raw2))


