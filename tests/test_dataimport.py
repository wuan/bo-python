# -*- coding: utf8 -*-

from __future__ import unicode_literals
import unittest
import datetime
from nose.tools import raises
import pandas as pd

from hamcrest.library.collection.is_empty import empty
from mock import Mock, patch, call
from hamcrest import assert_that, is_, equal_to, has_item, contains

import blitzortung
import blitzortung.dataimport
import blitzortung.builder


class HttpDataTransportTest(unittest.TestCase):
    def setUp(self):
        self.config = Mock(name='config')
        self.config.get_username.return_value = '<username>'
        self.config.get_password.return_value = '<password>'
        self.session = Mock(name='session')
        self.response = Mock(name='response')
        self.session.get.return_value = self.response

        self.data_transport = blitzortung.dataimport.HttpDataTransport(self.config, self.session)

    def test_read_lines_from_url(self):
        self.response.status_code = 200
        self.response.iter_lines.return_value = ['line1\n', 'line2\n']

        line_generator = self.data_transport.read_lines_from_url('http://foo.bar/baz')

        assert_that(list(line_generator), contains('line1', 'line2'))
        self.session.get.assert_called_with(
            'http://foo.bar/baz',
            auth=('<username>', '<password>'),
            timeout=60,
            stream=True)

    def test_read_lines_from_url_with_post_process(self):
        self.response.status_code = 200
        self.response.content = 'content'

        post_process = Mock(name='post_process')
        post_process.return_value = 'processed\nlines\n'

        line_generator = self.data_transport.read_lines_from_url('http://foo.bar/baz', post_process=post_process)

        assert_that(list(line_generator), contains('processed', 'lines'))

    def test_read_from_url_with_error(self):
        self.response.status_code = 404

        response = self.data_transport.read_lines_from_url('http://foo.bar/baz')

        assert_that(list(response), is_([]))


class BlitzortungDataUrlTest(unittest.TestCase):
    def setUp(self):
        self.data_url = blitzortung.dataimport.BlitzortungDataUrl()

    def test_default_values(self):
        target_url = self.data_url.build_url('url_path')
        assert_that(target_url, is_(equal_to('http://data.blitzortung.org/Data_1/url_path')))

    def test_specific_values(self):
        target_url = self.data_url.build_url('url_path', host_name='foo', region=42)
        assert_that(target_url, is_(equal_to('http://foo.blitzortung.org/Data_42/url_path')))


class BlitzortungDataProviderTest(unittest.TestCase):
    def setUp(self):
        self.http_data_transport = Mock(name="data_transport")

        self.provider = blitzortung.dataimport.BlitzortungDataProvider(self.http_data_transport)

    def test_read_data(self):
        response = [b"line1 ", b"line2", "äöü".encode('latin1')]

        self.http_data_transport.read_lines_from_url.return_value = response

        result = self.provider.read_data('target_url')

        assert_that(list(result), contains(u'line1 ', u'line2', u'äöü'))

        self.http_data_transport.read_lines_from_url.assert_called_with(
            'target_url', post_process=None)

    def test_read_data_with_empty_response(self):
        self.http_data_transport.read_lines_from_url.return_value = []
        result = self.provider.read_data('foo')

        assert_that(list(result), equal_to([]))


class BlitzortungHistoryUrlGeneratorTest(unittest.TestCase):
    def create_history_url_generator(self, present_time):
        self.present_time = present_time
        self.start_time = present_time - datetime.timedelta(minutes=25)
        self.strikes_url = blitzortung.dataimport.BlitzortungHistoryUrlGenerator()

    def test_strike_url_iterator(self):
        self.create_history_url_generator(datetime.datetime(2013, 8, 20, 12, 9, 0))

        urls = [url for url in self.strikes_url.get_url_paths(self.start_time, self.present_time)]

        assert_that(urls, contains(
            '2013/08/20/11/40.log',
            '2013/08/20/11/50.log',
            '2013/08/20/12/00.log'
        ))


class StrikesBlitzortungDataProviderTest(unittest.TestCase):
    def setUp(self):
        self.data_provider = Mock()
        self.data_url = Mock()
        self.url_generator = Mock()
        self.builder = Mock()

        self.provider = blitzortung.dataimport.StrikesBlitzortungDataProvider(
            self.data_provider,
            self.data_url,
            self.url_generator,
            self.builder
        )
        self.provider.read_data = Mock()

    def test_get_strikes_since(self):
        now = datetime.datetime.utcnow()
        latest_strike_timestamp = now - datetime.timedelta(hours=1)
        self.url_generator.get_url_paths.return_value = ['path1', 'path2']
        strike_data1 = {'one': 1}
        strike_data2 = {'two': 2}
        self.data_provider.read_data.side_effect = [[strike_data1, strike_data2], []]
        strike1 = Mock()
        strike2 = Mock()
        strike1.get_timestamp.return_value = pd.Timestamp(now - datetime.timedelta(hours=2))
        strike2.get_timestamp.return_value = pd.Timestamp(now)
        self.builder.from_line.return_value = self.builder
        self.builder.build.side_effect = [strike1, strike2]

        strikes = self.provider.get_strikes_since(latest_strike_timestamp)

        assert_that(list(strikes), contains(strike2))

    def test_get_strikes_since_with_builder_error(self):
        now = datetime.datetime.utcnow()
        latest_strike_timestamp = now - datetime.timedelta(hours=1)
        self.url_generator.get_url_paths.return_value = ['path']
        strike_data = {'one': 1}
        self.data_provider.read_data.side_effect = [[strike_data], []]
        self.builder.from_line.return_value = self.builder
        self.builder.build.side_effect = blitzortung.builder.BuilderError("foo")

        strikes = self.provider.get_strikes_since(latest_strike_timestamp)

        assert_that(strikes, is_(empty()))

    @raises(Exception)
    def test_get_strikes_since_with_generic_exception(self):
        now = datetime.datetime.utcnow()
        latest_strike_timestamp = now - datetime.timedelta(hours=1)
        self.url_generator.get_url_paths.return_value = ['path']
        strike_data = {'one': 1}
        self.data_provider.read_line.side_effect = [[strike_data], []]
        self.builder.from_data.return_value = self.builder
        self.builder.build.side_effect = Exception("foo")

        self.provider.get_strikes_since(latest_strike_timestamp)


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
        expected_args = [call('full_url', post_process=self.provider.pre_process)]
        assert_that(self.data_provider.read_data.call_args_list, is_(equal_to(expected_args)))

        assert_that(stations, contains(station1, station2))

    def test_get_stations_with_builder_error(self):
        station_data = {'one': 1}
        self.data_provider.read_data.side_effect = [[station_data], []]
        self.builder.from_line.return_value = self.builder
        self.builder.build.side_effect = blitzortung.builder.BuilderError("foo")

        strikes = self.provider.get_stations()

        assert_that(list(strikes), is_(empty()))


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

        self.data_url.build_url.side_effect = ['full_url1', 'full_url2']
        self.url_generator.get_url_paths.return_value = ['url_path1', 'url_path2']
        self.data_provider.read_data.side_effect = [["line11", "line12"], ["line21", "line22"]]
        raw11 = Mock(name='raw11')
        raw12 = Mock(name='raw12')
        raw21 = Mock(name='raw21')
        raw22 = Mock(name='raw22')
        self.builder.from_string.return_value = self.builder
        self.builder.build.side_effect = [raw11, raw12, raw21, raw22]

        region_id = 5
        station_id = 123
        strikes = self.provider.get_raw_data_since(last_data, region_id, station_id)

        expected_args = [call(last_data)]
        assert_that(self.url_generator.get_url_paths.call_args_list, is_(equal_to(expected_args)))

        expected_args = [
            call('123/url_path1', region=5, host_name='signals'),
            call('123/url_path2', region=5, host_name='signals')]
        assert_that(self.data_url.build_url.call_args_list, is_(equal_to(expected_args)))

        expected_args = [call('full_url1'), call('full_url2')]
        assert_that(self.data_provider.read_data.call_args_list, is_(equal_to(expected_args)))

        assert_that(strikes, contains("line11", "line12", "line21", "line22"))


