import unittest
import datetime

import blitzortung


class IdIntervalTest(unittest.TestCase):

    def test_nothing_set(self):
        id_interval = blitzortung.db.IdInterval()

        self.assertEqual(id_interval.get_start(), None)
        self.assertEqual(id_interval.get_end(), None)
        self.assertEqual(str(id_interval), "[None - None]")

    def test_start_set(self):
        id_interval = blitzortung.db.IdInterval(1234)

        self.assertEqual(id_interval.get_start(), 1234)
        self.assertEqual(id_interval.get_end(), None)
        self.assertEqual(str(id_interval), "[1234 - None]")

    def test_start_and_stop_set(self):
        id_interval = blitzortung.db.IdInterval(1234, 5678)

        self.assertEqual(id_interval.get_start(), 1234)
        self.assertEqual(id_interval.get_end(), 5678)
        self.assertEqual(str(id_interval), "[1234 - 5678]")

class TimeIntervalTest(unittest.TestCase):

    def test_nothing_set(self):
        id_interval = blitzortung.db.TimeInterval()

        self.assertEqual(id_interval.get_start(), None)
        self.assertEqual(id_interval.get_end(), None)
        self.assertEqual(str(id_interval), "[None - None]")

    def test_start_set(self):
        id_interval = blitzortung.db.TimeInterval(datetime.datetime(2010,11,20,11,30,15))

        self.assertEqual(id_interval.get_start(), datetime.datetime(2010,11,20,11,30,15))
        self.assertEqual(id_interval.get_end(), None)
        self.assertEqual(str(id_interval), "[2010-11-20 11:30:15 - None]")

    def test_start_and_stop_set(self):
        id_interval = blitzortung.db.TimeInterval(datetime.datetime(2010,11,20,11,30,15), datetime.datetime(2010,12,5,23,15,59))

        self.assertEqual(id_interval.get_start(), datetime.datetime(2010,11,20,11,30,15))
        self.assertEqual(id_interval.get_end(), datetime.datetime(2010,12,5,23,15,59))
        self.assertEqual(str(id_interval), "[2010-11-20 11:30:15 - 2010-12-05 23:15:59]")

class QueryTest(unittest.TestCase):
    
    def setUp(self):
        self.query = blitzortung.db.Query()
        self.query.set_table_name("foo")

    def test_initialization(self):
        self.assertEqual(str(self.query), "SELECT FROM foo")

    def test_set_table_name(self):
        self.assertEqual(str(self.query), "SELECT FROM foo")
        
    def test_set_columns(self):
        self.query.set_columns(['bar', 'baz'])
        self.assertEqual(str(self.query), "SELECT bar, baz FROM foo")

    def test_add_column(self):
        self.query.add_column('bar')
        self.assertEqual(str(self.query), "SELECT bar FROM foo")
        self.query.add_column('baz')
        self.assertEqual(str(self.query), "SELECT bar, baz FROM foo")

    def test_add_group_by(self):
        self.query.add_group_by('bar')
        self.assertEqual(str(self.query), "SELECT FROM foo GROUP BY bar")
        self.query.add_group_by('baz')
        self.assertEqual(str(self.query), "SELECT FROM foo GROUP BY bar, baz")

    def test_set_condition(self):
        self.query.add_condition("qux")
        self.assertEqual(str(self.query), "SELECT FROM foo WHERE qux")

        self.query.add_condition("quux")
        self.assertEqual(str(self.query), "SELECT FROM foo WHERE qux AND quux")

    def test_add_order(self):
        self.query.add_order(blitzortung.db.Order("bar"))
        self.assertEqual(str(self.query), "SELECT FROM foo ORDER BY bar")

        self.query.add_order(blitzortung.db.Order("baz", True))
        self.assertEqual(str(self.query), "SELECT FROM foo ORDER BY bar, baz DESC")
        
    def test_set_limit(self):
        self.query.set_limit(blitzortung.db.Limit(10))
        self.assertEqual(str(self.query), "SELECT FROM foo LIMIT 10")
        