import blitzortung
import unittest

class TestTimestamp(unittest.TestCase):

  def setUp(self):
    self.seq = range(10)

  def test_create_from_string(self):

    dir(blitzortung.data)
    timestamp = blitzortung.data.Timestamp("2012-02-10 12:56:18.096651423")

    print timestamp