# -*- coding: utf8 -*-

import unittest
import math
import datetime
from hamcrest import assert_that, is_, equal_to

import blitzortung


class DataFormatTest(unittest.TestCase):
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


class StrokesUrlTest(unittest.TestCase):
    def create_strokes_url_generator(self, end_time):
        self.strokes_url = blitzortung.dataimport.BlitzortungStrokeUrlGenerator(end_time - datetime.timedelta(minutes=25), end_time)

    def test_stroke_url_iterator(self):
        self.create_strokes_url_generator(datetime.datetime(2013, 8, 20, 12, 9, 0))

        urls = [url for url in self.strokes_url.get_url_paths()]

        assert_that(len(urls), is_(equal_to(3)))

        assert_that(urls[0], is_(equal_to('Strokes/2013/08/20/11/40.log')))
        assert_that(urls[1], is_(equal_to('Strokes/2013/08/20/11/50.log')))
        assert_that(urls[2], is_(equal_to('Strokes/2013/08/20/12/00.log')))

    def test_stroke_url_iterator_at_start_of_interval(self):
        self.create_strokes_url_generator(datetime.datetime(2013, 8, 20, 12, 5, 0))

        urls = [url for url in self.strokes_url.get_url_paths()]

        assert_that(len(urls), is_(equal_to(3)))

        assert_that(urls[0], is_(equal_to('Strokes/2013/08/20/11/40.log')))
        assert_that(urls[1], is_(equal_to('Strokes/2013/08/20/11/50.log')))
        assert_that(urls[2], is_(equal_to('Strokes/2013/08/20/12/00.log')))

    def test_stroke_url_iterator_at_start_of_interval(self):
        self.create_strokes_url_generator(datetime.datetime(2013, 8, 20, 12, 4, 59, 999999))

        urls = [url for url in self.strokes_url.get_url_paths()]

        assert_that(len(urls), is_(equal_to(4)))

        assert_that(urls[0], is_(equal_to('Strokes/2013/08/20/11/30.log')))
        assert_that(urls[1], is_(equal_to('Strokes/2013/08/20/11/40.log')))
        assert_that(urls[2], is_(equal_to('Strokes/2013/08/20/11/50.log')))
        assert_that(urls[3], is_(equal_to('Strokes/2013/08/20/12/00.log')))
