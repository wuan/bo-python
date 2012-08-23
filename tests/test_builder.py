import unittest
import datetime
import pytz

import blitzortung

class StrokeTest(unittest.TestCase):

  def test_build_stroke_from_string(self):
    line = "2012-08-23 13:18:15.504862926 44.254116 17.583977 6.34kA -1 3406m 12"
    stroke_builder = blitzortung.builder.Stroke()
    stroke_builder.from_string(line)

    stroke = stroke_builder.build()

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

    stroke_builder = blitzortung.builder.Stroke()
    stroke_builder.from_string(line)

    stroke = stroke_builder.build()

    self.assertThat(stroke.get_participants().size(), 11)

class StationTest(unittest.TestCase):

  def test_build_station_from_string(self):
    line = "364 MustermK Karl&nbsp;Mustermann Neustadt Germany 49.5435 9.7314 2012-02-10&nbsp;14:39:47.410492569 A WT&#32;5.20.3 4"
    station_builder = blitzortung.builder.Station()
    station_builder.from_string(line)
    station = station_builder.build()

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

    station_builder = blitzortung.builder.Station()
    station_builder.set_number(364)
    station_builder.set_short_name('MustermK')
    station_builder.set_name('Karl Mustermann')
    station_builder.set_country('Germany')
    station_builder.set_x(9.7314)
    station_builder.set_y(49.5435)
    station_builder.set_last_data("2012-02-10 14:39:47.410492123")
    station_builder.set_gps_status('A')
    station_builder.set_tracker_version('WT 5.20.3')
    station_builder.set_samples_per_hour(4)

    station = station_builder.build()

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

class TimestampTest(unittest.TestCase):

  def setUp(self):
    pass

  def assert_timestamp_base(self, timestamp):
    self.assertEqual(timestamp.day, 10)
    self.assertEqual(timestamp.month, 2)
    self.assertEqual(timestamp.year, 2012)
    self.assertEqual(timestamp.hour, 12)
    self.assertEqual(timestamp.minute, 56)
    self.assertEqual(timestamp.second, 18)

  def test_create_from_string(self):

    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.096651423")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 96651)
    self.assertEqual(timestamp_nanoseconds, 423)

  def test_create_from_millisecond_string(self):

    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.096")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 96000)
    self.assertEqual(timestamp_nanoseconds, 0)

  def test_create_from_string_wihtout_fractional_seconds(self):

    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 0)
    self.assertEqual(timestamp_nanoseconds, 0)

  def test_create_from_nanosecond_string(self):

    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.123456789")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_nanoseconds, 789)


    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.12345678")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_nanoseconds, 780)


    base_builder = blitzortung.builder.Base()
    (timestamp, timestamp_nanoseconds) = base_builder.parse_timestamp_with_nanoseconds("2012-02-10 12:56:18.1234567")

    self.assert_timestamp_base(timestamp)
    self.assertEqual(timestamp.microsecond, 123456)
    self.assertEqual(timestamp_nanoseconds, 700)
