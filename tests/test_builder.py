import unittest
import datetime
import pytz

import blitzortung


class BaseTest(unittest.TestCase):

  def setUp(self):
    self.builder = blitzortung.builder.Base()

  def assert_timestamp_base(self, timestamp):
    self.assertEqual(timestamp.day, 10)
    self.assertEqual(timestamp.month, 2)
    self.assertEqual(timestamp.year, 2012)
    self.assertEqual(timestamp.hour, 12)
    self.assertEqual(timestamp.minute, 56)
    self.assertEqual(timestamp.second, 18)

  def test_create_from_string(self):

    self.builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = self.builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.096651423")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 96651)
    self.assertEqual(timestamp_nanoseconds, 423)

  def test_create_from_millisecond_string(self):

    self.builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = self.builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.096")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 96000)
    self.assertEqual(timestamp_nanoseconds, 0)

  def test_create_from_string_wihtout_fractional_seconds(self):

    self.builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = self.builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 0)
    self.assertEqual(timestamp_nanoseconds, 0)

  def test_create_from_nanosecond_string(self):

    self.builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = self.builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.123456789")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_nanoseconds, 789)


    self.builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = self.builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.12345678")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_nanoseconds, 780)


    self.builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = self.builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.1234567")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_nanoseconds, 700)

class StrokeTest(unittest.TestCase):

  def setUp(self):
    self.builder = blitzortung.builder.Stroke()

  def test_default_values(self):
    self.assertEqual(self.builder.id_value, -1)
    self.assertEqual(self.builder.altitude, None)
    self.assertEqual(self.builder.participants, [])

  def test_set_id(self):
    self.builder.set_id(1234)
    self.assertEqual(self.builder.id_value, 1234)

    self.builder.set_x(0.0)
    self.builder.set_y(0.0)
    self.builder.set_timestamp(datetime.datetime.utcnow())
    self.builder.set_timestamp_nanoseconds(0)
    self.builder.set_amplitude(1.0)
    self.builder.set_lateral_error(5.0)
    self.builder.set_type(-1)
    self.builder.set_station_count(10)

    self.assertEqual(self.builder.build().get_id(), 1234)

  def test_set_timestamp(self):
    timestamp = datetime.datetime.utcnow()
    self.builder.set_timestamp(timestamp)
    self.assertEqual(self.builder.timestamp, timestamp)

    self.builder.set_x(0.0)
    self.builder.set_y(0.0)
    self.builder.set_timestamp_nanoseconds(0)
    self.builder.set_amplitude(1.0)
    self.builder.set_lateral_error(5.0)
    self.builder.set_type(-1)
    self.builder.set_station_count(10)

    self.assertEqual(self.builder.build().get_timestamp(), timestamp)


  def test_set_timestamp_nanoseconds(self):
    self.builder.set_timestamp_nanoseconds(567)
    self.assertEqual(self.builder.timestamp_nanoseconds, 567)

    self.builder.set_x(0.0)
    self.builder.set_y(0.0)
    self.builder.set_timestamp(datetime.datetime.utcnow())

    self.builder.set_amplitude(1.0)
    self.builder.set_lateral_error(5.0)
    self.builder.set_type(-1)
    self.builder.set_station_count(10)

    self.assertEqual(self.builder.build().get_timestamp_nanoseconds(), 567)

  def test_build_stroke_from_string(self):
    line = "2012-08-23 13:18:15.504862926 44.254116 17.583977 6.34kA -1 3406m 12"

    self.builder.from_string(line)

    stroke = self.builder.build()

    self.assertEqual(stroke.get_timestamp(), datetime.datetime(2012,8,23,13,18,15,504862).replace(tzinfo=pytz.UTC))
    self.assertEqual(stroke.get_timestamp_nanoseconds(), 926)
    self.assertEqual(stroke.get_x(), 17.583977)
    self.assertEqual(stroke.get_y(), 44.254116)
    self.assertEqual(stroke.get_amplitude(), 6340)
    self.assertEqual(stroke.get_type(), -1)
    self.assertEqual(stroke.get_lateral_error(), 3406)
    self.assertEqual(stroke.get_station_count(), 12)

  def test_build_stroke_from_participants_string(self):
    line = "2012-08-23 13:18:15.504862926 44.254116 17.583977 6.34kA -1 3406m 12 User1 User2 User3 User4 User5 User6 User7 User8 User9 User10 User11"

    self.builder.from_string(line)

    stroke = self.builder.build()

    self.assertEqual(len(stroke.get_participants()), 11)
    self.assertTrue(stroke.has_participant('User1'))

  def test_build_stroke_from_string_throws_exception_with_too_short_string(self):
    line = "2012-08-23 13:18:15.504862926 44.254116 17.583977 6.34kA -1 3406m"
    self.assertRaises(ValueError, self.builder.from_string, line)

class StationTest(unittest.TestCase):

  def setUp(self):
    self.builder = blitzortung.builder.Station()

  def test_default_values(self):
    self.assertEqual(self.builder.number, -1)
    self.assertEqual(self.builder.location_name, None)
    self.assertEqual(self.builder.last_data, None)
    self.assertEqual(self.builder.gps_status, None)
    self.assertEqual(self.builder.samples_per_hour, -1)
    self.assertEqual(self.builder.tracker_version, None)
    self.assertEqual(self.builder.offline_since, None)

  def test_build_station_from_string(self):
    line = "364 MustermK Karl&nbsp;Mustermann Neustadt Germany 49.5435 9.7314 2012-02-10&nbsp;14:39:47.410492569 A WT&#32;5.20.3 4"

    self.builder.from_string(line)
    station = self.builder.build()

    self.assertEqual(station.get_number(), 364)
    self.assertEqual(station.get_short_name(), 'MustermK')
    self.assertEqual(station.get_name(), 'Karl Mustermann')
    self.assertEqual(station.get_country(), 'Germany')
    self.assertEqual(station.get_x(), 9.7314)
    self.assertEqual(station.get_y(), 49.5435)
    self.assertEqual(station.get_timestamp(), datetime.datetime(2012,2,10,14,39,47,410492).replace(tzinfo=pytz.UTC))
    self.assertEqual(station.get_gps_status(), 'A')
    self.assertEqual(station.get_tracker_version(), 'WT 5.20.3')
    self.assertEqual(station.get_samples_per_hour(), 4)

  def test_build_station(self):

    self.builder.set_number(364)
    self.builder.set_short_name('MustermK')
    self.builder.set_name('Karl Mustermann')
    self.builder.set_country('Germany')
    self.builder.set_x(9.7314)
    self.builder.set_y(49.5435)
    self.builder.set_last_data("2012-02-10 14:39:47.410492123")
    self.builder.set_gps_status('A')
    self.builder.set_tracker_version('WT 5.20.3')
    self.builder.set_samples_per_hour(4)

    station = self.builder.build()

    self.assertEqual(station.get_number(), 364)
    self.assertEqual(station.get_short_name(), 'MustermK')
    self.assertEqual(station.get_name(), 'Karl Mustermann')
    self.assertEqual(station.get_country(), 'Germany')
    self.assertEqual(station.get_x(), 9.7314)
    self.assertEqual(station.get_y(), 49.5435)
    self.assertEqual(station.get_timestamp(), datetime.datetime(2012,2,10,14,39,47,410492).replace(tzinfo=pytz.UTC))
    self.assertEqual(station.get_gps_status(), 'A')
    self.assertEqual(station.get_tracker_version(), 'WT 5.20.3')
    self.assertEqual(station.get_samples_per_hour(), 4)
