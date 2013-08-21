# -*- coding: utf8 -*-

import unittest
import math
from hamcrest import assert_that, is_, equal_to

import blitzortung


class DataFormatTest(unittest.TestCase):
    def setUp(self):
        self.data_format = blitzortung.web.DataFormat()

    def test_parse_line(self):
        result = self.data_format.parse_line('foo;1;2 bar;3;4')

        assert_that(result, is_(equal_to({'foo': ['1', '2'], 'bar': ['3', '4']})))

    def test_parse_line_with_space(self):
        result = self.data_format.parse_line('foo;1;2 "foo bar";3;4')

        assert_that(result, is_(equal_to({'foo bar': ['3', '4'], 'foo': ['1', '2']})))

    def test_parse_line_with_html(self):
        result = self.data_format.parse_line('foo;b&auml;z;&szlig; "f&ouml;o&nbsp;b&auml;r";3;4')

        assert_that(result, is_(equal_to({u'föo bär': ['3', '4'], 'foo': [u'bäz', u'ß']})))
