import unittest
import datetime
import pytz

from mock import Mock, PropertyMock, call
from hamcrest import assert_that, is_, equal_to, none

import psycopg2

import blitzortung
import blitzortung.db.table


class BaseTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_initialize(self):
        pass
