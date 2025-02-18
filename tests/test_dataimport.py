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

import datetime

import pytest
from assertpy import assert_that
from mock import Mock, call

import blitzortung
import blitzortung.builder
import blitzortung.dataimport


class HttpDataTransportTest(object):
    def setUp(self):
        self.config = Mock(name="config")
        self.config.get_username.return_value = "<username>"
        self.config.get_password.return_value = "<password>"
        self.session = Mock(name="session")
        self.response = Mock(name="response")
        self.response.status_code = 200
        self.session.get.return_value = self.response

        self.data_transport = blitzortung.dataimport.HttpFileTransport(
            self.config, self.session
        )

    def test_read_lines_from_url(self):
        self.response.iter_lines.return_value = [b"line1", b"line2"]

        line_generator = self.data_transport.read_lines("http://foo.bar/baz")

        assert_that(list(line_generator)).contains("line1", "line2")
        self.session.get.assert_called_with(
            "http://foo.bar/baz",
            auth=("<username>", "<password>"),
            timeout=60,
            stream=True,
        )

    def test_read_data(self):
        self.response.iter_lines.return_value = [
            b"line1 ",
            b"line2",
            "äöü".encode("utf8"),
        ]

        # self.http_data_transport.read_lines_from_url.return_value = response

        result = self.data_transport.read_lines("target_url")

        assert_that(list(result)).contains("line1 ", "line2", "äöü")

    def test_read_data_with_empty_response(self):
        self.response.iter_lines.return_value = []
        result = self.data_transport.read_lines("foo")

        assert_that(list(result)).is_empty()

    def test_read_lines_from_url_with_post_process(self):
        self.response.status_code = 200
        self.response.content = "content"

        post_process = Mock(name="post_process")
        post_process.return_value = b"processed\nlines\n"

        line_generator = self.data_transport.read_lines(
            "http://foo.bar/baz", post_process=post_process
        )

        assert_that(list(line_generator)).contains("processed", "lines")

    def test_read_from_url_with_error(self):
        self.response.status_code = 404

        response = self.data_transport.read_lines("http://foo.bar/baz")

        assert_that(list(response)).is_empty()


class BlitzortungDataUrlTest(object):
    def setUp(self):
        self.data_url = blitzortung.dataimport.BlitzortungDataPath()

    def test_default_values(self):
        target_url = self.data_url.build_path("url_path")
        assert_that(target_url).is_equal_to(
            "http://data.blitzortung.org/Data_1/url_path"
        )

    def test_specific_values(self):
        target_url = self.data_url.build_path("url_path", host_name="foo", region=42)
        assert_that(target_url).is_equal_to(
            "http://foo.blitzortung.org/Data_42/url_path"
        )

    def test_with_user_defined_base(self):
        self.data_url = blitzortung.dataimport.BlitzortungDataPath("base/path")
        target_url = self.data_url.build_path("url_path", host_name="bar", region=39)
        assert_that(target_url).is_equal_to("base/path/Data_39/url_path")


class BlitzortungHistoryUrlGeneratorTest(object):
    def create_history_url_generator(self, present_time):
        self.present_time = present_time
        self.start_time = present_time - datetime.timedelta(minutes=25)
        self.strikes_url = blitzortung.dataimport.BlitzortungDataPathGenerator()

    def test_strike_url_iterator(self):
        self.create_history_url_generator(datetime.datetime(2013, 8, 20, 12, 9, 0))

        urls = [
            url
            for url in self.strikes_url.get_paths(self.start_time, self.present_time)
        ]

        assert_that(urls).contains(
            "2013/08/20/11/40.log", "2013/08/20/11/50.log", "2013/08/20/12/00.log"
        )


class StrikesBlitzortungDataProviderTest(object):
    def setUp(self):
        self.data_provider = Mock()
        self.data_url = Mock()
        self.url_generator = Mock()
        self.builder = Mock()

        self.provider = blitzortung.dataimport.StrikesBlitzortungDataProvider(
            self.data_provider, self.data_url, self.url_generator, self.builder
        )
        self.provider.read_data = Mock()

    def test_get_strikes_since(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        latest_strike_timestamp = now - datetime.timedelta(hours=1)
        self.url_generator.get_paths.return_value = ["path1", "path2"]
        strike_data1 = {"one": 1}
        strike_data2 = {"two": 2}
        self.data_provider.read_lines.side_effect = [[strike_data1, strike_data2], []]
        strike1 = Mock()
        strike2 = Mock()
        strike1.timestamp = blitzortung.data.Timestamp(
            now - datetime.timedelta(hours=2)
        )
        strike2.timestamp = blitzortung.data.Timestamp(now)
        self.builder.from_line.return_value = self.builder
        self.builder.build.side_effect = [strike1, strike2]

        strikes = self.provider.get_strikes_since(latest_strike_timestamp)

        assert_that(list(strikes)).contains(strike2)

    def test_get_strikes_since_with_builder_error(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        latest_strike_timestamp = now - datetime.timedelta(hours=1)
        self.url_generator.get_paths.return_value = ["path"]
        strike_data = {"one": 1}
        self.data_provider.read_lines.side_effect = [[strike_data], []]
        self.builder.from_line.return_value = self.builder
        self.builder.build.side_effect = blitzortung.builder.BuilderError("foo")

        strikes = list(self.provider.get_strikes_since(latest_strike_timestamp))

        assert_that(strikes).is_empty()

    def test_get_strikes_since_with_generic_exception(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        latest_strike_timestamp = now - datetime.timedelta(hours=1)
        self.url_generator.get_url_paths.return_value = ["path"]
        strike_data = {"one": 1}
        self.data_provider.read_lines.side_effect = [[strike_data], []]
        self.builder.from_data.return_value = self.builder
        self.builder.build.side_effect = Exception("foo")

        with pytest.raises(Exception):
            list(self.provider.get_strikes_since(latest_strike_timestamp))


class StationsBlitzortungDataProviderTest(object):
    def setUp(self):
        self.data_provider = Mock()
        self.data_url = Mock()
        self.builder = Mock()

        self.provider = blitzortung.dataimport.StationsBlitzortungDataProvider(
            self.data_provider, self.data_url, self.builder
        )
        self.provider.read_data = Mock()

    def test_get_stations(self):
        station_data1 = "station one"
        station_data2 = "station two"
        self.data_url.build_path.return_value = "full_url"
        self.data_provider.read_lines.side_effect = [[station_data1, station_data2], []]
        station1 = Mock()
        station2 = Mock()
        self.builder.from_line.return_value = self.builder
        self.builder.build.side_effect = [station1, station2]

        stations = self.provider.get_stations()
        expected_args = [call("full_url", post_process=self.provider.pre_process)]
        assert_that(self.data_provider.read_lines.call_args_list).is_equal_to(
            expected_args
        )

        assert_that(stations).contains(station1, station2)

    def test_get_stations_with_builder_error(self):
        station_data = {"one": 1}
        self.data_provider.read_lines.side_effect = [[station_data], []]
        self.builder.from_line.return_value = self.builder
        self.builder.build.side_effect = blitzortung.builder.BuilderError("foo")

        strikes = self.provider.get_stations()

        assert_that(list(strikes)).is_empty()

    class RawSignalsBlitzortungDataProviderTest(object):
        def setUp(self):
            self.data_provider = Mock()
            self.data_url = Mock()
            self.url_path_generator = Mock()
            self.builder = Mock()

            self.provider = blitzortung.dataimport.RawSignalsBlitzortungDataProvider(
                self.data_provider, self.data_url, self.url_path_generator, self.builder
            )

        def test_get_raw_data_since(self):
            last_data = datetime.datetime.now(
                datetime.timezone.utc
            ) - datetime.timedelta(hours=1)

            self.data_url.build_path.side_effect = ["full_url1", "full_url2"]
            self.url_path_generator.get_paths.return_value = ["url_path1", "url_path2"]
            self.data_provider.read_lines.side_effect = [
                ["line11", "line12"],
                ["line21", "line22"],
            ]
            raw11 = Mock(name="raw11")
            raw12 = Mock(name="raw12")
            raw21 = Mock(name="raw21")
            raw22 = Mock(name="raw22")
            self.builder.from_string.return_value = self.builder
            self.builder.build.side_effect = [raw11, raw12, raw21, raw22]

            region_id = 5
            station_id = 123
            raw_signals = list(
                self.provider.get_raw_data_since(last_data, region_id, station_id)
            )

            expected_args = [call(last_data)]
            assert_that(self.url_path_generator.get_paths.call_args_list).is_equal_to(
                expected_args
            )

            expected_args = [
                call(
                    "{}/url_path1".format(station_id),
                    region=region_id,
                    host_name="signals",
                ),
                call(
                    "{}/url_path2".format(station_id),
                    region=region_id,
                    host_name="signals",
                ),
            ]
            assert_that(self.data_url.build_path.call_args_list).is_equal_to(
                expected_args
            )

            expected_args = [call("full_url1"), call("full_url2")]
            assert_that(self.data_provider.read_lines.call_args_list).is_equal_to(
                expected_args
            )

            assert_that(raw_signals).contains(raw11, raw12, raw21, raw22)
