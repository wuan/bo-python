# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

import unittest
import datetime
from hamcrest import assert_that, is_, equal_to, none
import pytz
import numpy as np
import pandas as pd

import blitzortung.builder


class TestBase(unittest.TestCase):
    @staticmethod
    def get_timestamp(timestamp_string):
        return pd.Timestamp(np.datetime64(timestamp_string + 'Z', 'ns'), tz=pytz.UTC)


class TimestampTest(unittest.TestCase):
    def setUp(self):
        self.builder = blitzortung.builder.Timestamp()

    def test_initial_value(self):
        assert_that(self.builder.build(), is_(none()))

    def test_set_timestamp_from_none_value(self):
        self.builder.set_timestamp(None)
        assert_that(self.builder.build(), is_(none()))

    def test_set_timestamp_from_datetime(self):
        timestamp = self.builder.set_timestamp(datetime.datetime(2012, 2, 10, 12, 56, 18, 96651)).build()

        self.assert_timestamp(timestamp)

    def test_set_timestamp_from_pandas_timestamp(self):
        timestamp = pd.Timestamp(datetime.datetime(2012, 2, 10, 12, 56, 18, 96651))
        timestamp = pd.Timestamp(timestamp.value + 423)

        self.builder.set_timestamp(timestamp)

        timestamp = self.builder.build()
        self.assert_timestamp(timestamp)
        assert_that(timestamp.nanosecond, is_(equal_to(423)))

    def test_set_timestamp_from_pandas_timestamp_with_ns_offset(self):
        timestamp = pd.Timestamp(datetime.datetime(2012, 2, 10, 12, 56, 18, 96651), tz='CET')

        self.builder.set_timestamp(timestamp, 423)

        timestamp = self.builder.build()
        self.assert_timestamp(timestamp)

        assert_that(timestamp.tzinfo, is_(equal_to(pytz.timezone('CET'))))
        assert_that(timestamp.nanosecond, is_(equal_to(423)))

    def test_set_timestamp_from_bad_string(self):
        timestamp = self.builder.set_timestamp('0000-00-00').build()
        assert_that(timestamp.toordinal(), is_(pd.NaT.toordinal()))

    def test_set_timestamp_from_string(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.096651423").build()

        self.assert_timestamp(timestamp)
        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(96651)))
        assert_that(timestamp.nanosecond, is_(equal_to(423)))
        assert_that(timestamp.tzinfo, is_(equal_to(pytz.UTC)))

    def test_set_timestamp_from_millisecond_string(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.096").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(96000)))
        assert_that(timestamp.nanosecond, is_(equal_to(0)))

    def test_create_from_string_wihtout_fractional_seconds(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(0)))
        assert_that(timestamp.nanosecond, is_(equal_to(0)))

    def test_create_from_nanosecond_string(self):
        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.123456789").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(123456)))
        assert_that(timestamp.nanosecond, is_(equal_to(789)))

        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.12345678").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(123456)))
        assert_that(timestamp.nanosecond, is_(equal_to(780)))

        timestamp = self.builder.set_timestamp("2012-02-10 12:56:18.1234567").build()

        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(123456)))
        assert_that(timestamp.nanosecond, is_(equal_to(700)))

    def assert_timestamp_base(self, timestamp):
        assert_that(timestamp.day, is_(equal_to(10)))
        assert_that(timestamp.month, is_(equal_to(2)))
        assert_that(timestamp.year, is_(equal_to(2012)))
        assert_that(timestamp.hour, is_(equal_to(12)))
        assert_that(timestamp.minute, is_(equal_to(56)))
        assert_that(timestamp.second, is_(equal_to(18)))

    def assert_timestamp(self, timestamp):
        self.assert_timestamp_base(timestamp)
        assert_that(timestamp.microsecond, is_(equal_to(96651)))


class StrikeTest(TestBase):
    def setUp(self):
        self.builder = blitzortung.builder.Strike()

    def test_default_values(self):
        assert_that(self.builder.id_value, is_(equal_to(-1)))
        assert_that(self.builder.altitude, is_(none()))
        assert_that(self.builder.stations, is_(equal_to([])))

    def test_set_id(self):
        self.builder.set_id(1234)
        assert_that(self.builder.id_value, 1234)

        self.builder.set_x(0.0)
        self.builder.set_y(0.0)
        self.builder.set_timestamp(datetime.datetime.utcnow())
        self.builder.set_amplitude(1.0)
        self.builder.set_lateral_error(5.0)
        self.builder.set_station_count(10)

        assert_that(self.builder.build().get_id(), is_(equal_to(1234)))

    def test_set_timestamp(self):
        timestamp = datetime.datetime.utcnow()
        self.builder.set_timestamp(timestamp)
        assert_that(self.builder.timestamp, is_(equal_to(timestamp)))

        self.builder.set_x(0.0)
        self.builder.set_y(0.0)
        self.builder.set_amplitude(1.0)
        self.builder.set_altitude(0)
        self.builder.set_lateral_error(5.0)
        self.builder.set_station_count(10)

        assert_that(self.builder.build().get_timestamp(), is_(equal_to(pd.Timestamp(timestamp))))

    def test_set_lateral_error_lower_limit(self):
        self.builder.set_lateral_error(-1)
	assert_that(self.builder.build().get_lateral_error(), is_(0))
        self.builder.set_lateral_error(0)
	assert_that(self.builder.build().get_lateral_error(), is_(0))

    def test_set_lateral_error_upper_limit(self):
        self.builder.set_lateral_error(32767)
	assert_that(self.builder.build().get_lateral_error(), is_(32767))
        self.builder.set_lateral_error(32768)
	assert_that(self.builder.build().get_lateral_error(), is_(32767))

    def test_build_strike_from_line(self):
        strike_line = u"2013-08-08 10:30:03.644038642 pos;44.162701;8.931001;0 str;4.75 typ;0 dev;20146 sta;10;24;226,529,391,233,145,398,425,533,701,336,336,515,434,392,439,283,674,573,559,364,111,43,582,594"
        strike = self.builder.from_line(strike_line).build()

        assert_that(strike.get_timestamp(), is_(equal_to(self.get_timestamp("2013-08-08 10:30:03.644038642"))))
        assert_that(strike.get_x(), is_(equal_to(8.931001)))
        assert_that(strike.get_y(), is_(equal_to(44.162701)))
        assert_that(strike.get_altitude(), is_(equal_to(0)))
        assert_that(strike.get_amplitude(), is_(equal_to(4.75)))
        assert_that(strike.get_lateral_error(), is_(equal_to(20146)))
        assert_that(strike.get_station_count(), is_(equal_to(10)))
        assert_that(strike.get_stations(), is_(equal_to(
            [226, 529, 391, 233, 145, 398, 425, 533, 701, 336, 336, 515, 434, 392, 439, 283, 674, 573, 559,
             364, 111, 43, 582, 594])))

    def test_build_strike_from_bad_line(self):
        strike_line = u"2013-08-08 10:30:03.644038642 pos;44.162701;8.931001;0 str;4.75 typ;0 dev;20146 sta;10;24;226,529,391,233,145,398,425,533,701,336,336,515,434,392,439,283,674,573,559,364,111,43,582,594,"
        strike = self.builder.from_line(strike_line).build()

        assert_that(strike.get_timestamp(), is_(equal_to(self.get_timestamp("2013-08-08 10:30:03.644038642"))))
        assert_that(strike.get_x(), is_(equal_to(8.931001)))
        assert_that(strike.get_y(), is_(equal_to(44.162701)))
        assert_that(strike.get_altitude(), is_(equal_to(0)))
        assert_that(strike.get_amplitude(), is_(equal_to(4.75)))
        assert_that(strike.get_lateral_error(), is_(equal_to(20146)))
        assert_that(strike.get_station_count(), is_(equal_to(10)))
        assert_that(strike.get_stations(), is_(equal_to(
            [226, 529, 391, 233, 145, 398, 425, 533, 701, 336, 336, 515, 434, 392, 439, 283, 674, 573, 559,
             364, 111, 43, 582, 594])))


class StationTest(TestBase):
    def setUp(self):
        self.builder = blitzortung.builder.Station()

    def test_default_values(self):
        assert_that(self.builder.number, is_(equal_to(-1)))
        assert_that(self.builder.user, is_(equal_to(-1)))
        assert_that(self.builder.name, is_(none()))
        assert_that(self.builder.status, is_(none()))
        assert_that(self.builder.board, is_(none()))

    def test_build_station_from_line(self):
        line = u'station;364 user;1 city;"Musterdörfl" country;"Germany" pos;49.5435;9.7314;432 board;6.8 firmware;"WT 6.20.2 / 31e" status; 30 distance;71.474188743479 myblitz;N input_board;;;;;; input_firmware;"31e";"31e";"";"";"";"" input_gain;7.7;7.7;7.7;7.7;7.7;7.7 input_antenna;10;10;;;; last_signal;"2012-02-10 13:39:47" signals;3133 last_stroke;"2013-10-04 21:03:34" strokes;0;0;0;4;6;66.6667;752;3983;18.8802'

        station = self.builder.from_line(line).build()

        assert_that(station.get_number(), is_(equal_to(364)))
        assert_that(station.get_user(), is_(equal_to(1)))
        assert_that(station.get_name(), is_(equal_to(u'Musterdörfl')))
        assert_that(station.get_country(), is_(equal_to('Germany')))
        assert_that(station.get_x(), is_(equal_to(9.7314)))
        assert_that(station.get_y(), is_(equal_to(49.5435)))
        assert_that(station.get_timestamp(), is_(equal_to(self.get_timestamp("2012-02-10T13:39:47"))))
        assert_that(station.get_board(), is_(equal_to(u'6.8')))

    def test_build_station_offline(self):
        self.builder.set_number(364)
        self.builder.set_user(10)
        self.builder.set_name(u'Musterdörfl')
        self.builder.set_country(u'Germany')
        self.builder.set_x(9.7314)
        self.builder.set_y(49.5435)
        self.builder.set_timestamp("2012-02-10 14:39:47.410492123")
        self.builder.set_status('A')
        self.builder.set_board('0815')

        station = self.builder.build()

        assert_that(station.get_number(), is_(equal_to(364)))
        assert_that(station.get_name(), is_(equal_to(u'Musterdörfl')))
        assert_that(station.get_country(), is_(equal_to(u'Germany')))
        assert_that(station.get_x(), is_(equal_to(9.7314)))
        assert_that(station.get_y(), is_(equal_to(49.5435)))
        assert_that(station.get_timestamp(), is_(equal_to(self.get_timestamp("2012-02-10T14:39:47.410492123"))))
        assert_that(station.get_status(), is_(equal_to('A')))
        assert_that(station.get_board(), is_(equal_to('0815')))


class StationOffline(unittest.TestCase):
    def setUp(self):
        self.builder = blitzortung.builder.StationOffline()

    def test_default_values(self):
        assert_that(self.builder.id_value, is_(equal_to(-1)))
        assert_that(self.builder.number, is_(equal_to(-1)))
        assert_that(self.builder.begin, is_(none()))
        assert_that(self.builder.end, is_(none()))

    def test_build_station_offline(self):
        self.builder.set_id(364)
        self.builder.set_number(123)

        end = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
        begin = end - datetime.timedelta(hours=1)
        self.builder.set_begin(begin)
        self.builder.set_end(end)

        station_offline = self.builder.build()

        assert_that(station_offline.get_id(), is_(equal_to(364)))
        assert_that(station_offline.get_number(), is_(equal_to(123)))
        assert_that(station_offline.get_begin(), is_(equal_to(begin)))
        assert_that(station_offline.get_end(), is_(equal_to(end)))


class RawWaveformEventTest(unittest.TestCase):
    def setUp(self):
        self.builder = blitzortung.builder.RawWaveformEvent(blitzortung.builder.ChannelWaveform())

    def test_build_raw_green_event(self):
        line = "2013-09-28 20:00:54.490994382 48.000000 11.000000 500 0 GREEN 0 0.0 256 0 8 0 0 1950 69696A6B6D70727476787C80828483848484838382838384848587898A8A8A8B8C8C8D8D8D8D8C8B8987868482807D7B7A7979797A7B7C7D7D8082838484848382807F7E7C7B7978787878797B7D7F80828587898A8B8B8A88878583817E7D7B7A79797A7B7B7C7D7E7E8081818180807E7C7C7B7A797979797A7C7E8082848687898B8C8D8D8D8B8A8987858482807F7D7D7D7E80808283848586878786858381807E7D7C7B797878797A7B7D7F81828485878888888785848280807E7C7A7979797A7B7D7E8081828485868686858482807F7E7C7B79787878797B7D7F8082848688898A8A8A898785848382807F7D7D7D7D7E7F8081828283848585848381 1 GREEN 0 0.0 256 0 8 0 0 1950 8C8C8C8D8C8C8B8A89888685838281818080808080807F7F7E7D7D7C7B7A7A7A7A797979797979797A7B7C7D7E7F8082838384848584848484838281808080808080828182838384858584848483828281807F7D7D7C7C7C7C7D7E7F80818283848586868686868685858584838483838484858585868685868585838381807F7E7D7C7B7A7A797A7A7A7B7C7D7E7F8081828282828281818080807F7F7F7F7F80808181828283838383838382818180807F7E7D7D7D7D7D7E7E7F8080818283838484848383828282818180808080818182828383848585858585858483828281807E7D7C7C7C7C7C7C7D7D7E7E7F8080808180808080808080807F7F808081"
        self.builder.from_string(line)
        raw_event = self.builder.build()

        assert_that(raw_event.get_x(), is_(equal_to(11.0)))
        assert_that(raw_event.get_y(), is_(equal_to(48.0)))
        assert_that(raw_event.get_altitude(), is_(equal_to(500)))

        channels = raw_event.get_channels()
        assert_that(len(channels), is_(equal_to(2)))
        assert_that(channels[0].get_channel_number(), is_(equal_to(0)))
        assert_that(channels[1].get_channel_number(), is_(equal_to(1)))

    def test_build_raw_red_event(self):
        line = "2013-09-28 19:31:30.913939699 48.500000 11.500000 388 0 12.2 3 8.8 512 128 8 2 1618 1618 7374797B7F84898B8B898788838283838282817E7B7A7774767A7873859397938A847E7C7A7A7C7E7F7F7E7E7F7C7B78797B7F85888A8D8C8885817D7A79777A7E817276828688827F7F81878A8B8B86857F7A757575777B7D8081817F81808182848688888888848079666A757C7D7B7C82868A8A8986837F7F7E786D5D47362A2A34435468788997A0A6A8AAA8A6A2A199888E989A97918F90929597999A99999998938E8B878685878A8D93999FA7AFB4B9BEC8D0CEBFAA7F584537281B100F141E293747525C63696A6A6C6F73777B7D7E7F7F8084888E94999CA0A3A5A5A6938E94948F847B7777747476797E838B939CA2AAACADA8A39A948E8986827D7876726F6B6A696A6D5F6A7A81827B74706F6E6B6C72777D818689888988898D8F93979593918E89827F7C7B797D8188857C899396938B8989898A898A8C8A8A89878885868380807E7F7B78757271716F706F6D6B686563525361696C6A696E73787C78767574757373757578797979787877797E85898C8D8E8B888381827473818B8F8B858382838486888A8C8D8F8F8A86817C7572717174767A7C7D80818080808283868A7D818C8C877C7572767B80868B8F9292938F8D8C8A8B89898885847D7A78747575777B8185878B7E798487888079767475767575747373757B8184888C8D8C8888807C7777787B7D7D7F7E7D7D7D81747D8E95978F8986817E 1 12.2 3 8.8 512 128 8 4 1618 1618 8F89938C89888A80736E72747073797F808A9094969B9686858F715381A1A28F716C6B6B6B787F8A8D8C8A92908C8C827E7B7777777B7F7B7B7E797C8185898E989C63759FADA7886F63626A72787B7380818383898D929592938D8A7D7F756F6F6B6D72727B85888E8E637DACB8B099857C78746F6C6D6C7077848794A9C4D7DAD3CEBBA6957B76716A676158525357625D3E6EA3B2A1846F64625D5C5A5F676A707B7373716B73767A7E7A807E726C5F5048443F3D4356797478B9EFFCEDDED7D4CFC6B5B19E958A847D7B808587918A88899292887E6F625D5C585B59544E5E30407997A494898999969B9B908E878179726A665F5E5D5F5C5F656B77828A929BA0A59D989697985572A0ACAC8C76747D88858A8E94979492948D8D796E67616165646C7079777A7B7B877D82828D774E76989C89655C6067777C889390979894989CA19CA0A5A2A3948880737574747C8591A0A8AEB37D86ACB8AB916D686565696466676C7475848E959D9C948F8F948C8C8C878C84828584857E808A636598B2B8A4917D787773756A686E717575767A7D877F86868C958F98A09B9B9488827777727278476394AAA78C7564666663696C6A6A63615C60666B7574848A94979096978E918488868585828A5D54889CA692879494A2A6ACB0A8A4A29C958D8A858587847C89808984888C8F958C8E8E8D908480415C8D9594806D6D6C7A"
        self.builder.from_string(line)
        raw_event = self.builder.build()

        assert_that(raw_event.get_x(), is_(equal_to(11.5)))
        assert_that(raw_event.get_y(), is_(equal_to(48.5)))
        assert_that(raw_event.get_altitude(), is_(equal_to(388)))

        channels = raw_event.get_channels()
        assert_that(len(channels), is_(equal_to(2)))
        assert_that(channels[0].get_channel_number(), is_(equal_to(0)))
        assert_that(channels[1].get_channel_number(), is_(equal_to(1)))


class ChannelWaveformTest(unittest.TestCase):
    def setUp(self):
        self.builder = blitzortung.builder.ChannelWaveform()

    def test_build_raw_green_event(self):
        line = "0 GREEN 0 0.0 256 0 8 0 0 1950 81807F6B6D70727476787C80828483848484838382838384848587898A8A8A8B8C8C8D8D8D8D8C8B8987868482807D7B7A7979797A7B7C7D7D8082838484848382807F7E7C7B7978787878797B7D7F80828587898A8B8B8A88878583817E7D7B7A79797A7B7B7C7D7E7E8081818180807E7C7C7B7A797979797A7C7E8082848687898B8C8D8D8D8B8A8987858482807F7D7D7D7E80808283848586878786858381807E7D7C7B797878797A7B7D7F81828485878888888785848280807E7C7A7979797A7B7D7E8081828485868686858482807F7E7C7B79787878797B7D7F8082848688898A8A8A898785848382807F7D7D7D7D7E7F8081828283848585848381"
        self.builder.from_field_iterator(iter(line.split(" ")))
        channel_waveform = self.builder.build()

        assert_that(channel_waveform.get_channel_number(), is_(equal_to(0)))
        assert_that(channel_waveform.get_amplifier_version(), is_(equal_to("GREEN")))
        assert_that(channel_waveform.get_antenna(), is_(equal_to(0)))
        assert_that(channel_waveform.get_gain(), is_(equal_to("0.0")))
        assert_that(channel_waveform.get_values(), is_(equal_to(256)))
        assert_that(channel_waveform.get_start(), is_(equal_to(0)))
        assert_that(channel_waveform.get_bits(), is_(equal_to(8)))
        assert_that(channel_waveform.get_shift(), is_(equal_to(0)))
        assert_that(channel_waveform.get_conversion_gap(), is_(equal_to(0)))
        assert_that(channel_waveform.get_conversion_time(), is_(equal_to(1950)))

        waveform = channel_waveform.get_waveform()
        assert_that(len(waveform), is_(equal_to(256)))
        assert_that(waveform[0], is_(equal_to(1)))
        assert_that(waveform[1], is_(equal_to(0)))
        assert_that(waveform[2], is_(equal_to(-1)))

    def test_build_raw_red_event(self):
        line = "1 12.2 3 8.8 512 128 8 4 1618 1618 FF80008C89888A80736E72747073797F808A9094969B9686858F715381A1A28F716C6B6B6B787F8A8D8C8A92908C8C827E7B7777777B7F7B7B7E797C8185898E989C63759FADA7886F63626A72787B7380818383898D929592938D8A7D7F756F6F6B6D72727B85888E8E637DACB8B099857C78746F6C6D6C7077848794A9C4D7DAD3CEBBA6957B76716A676158525357625D3E6EA3B2A1846F64625D5C5A5F676A707B7373716B73767A7E7A807E726C5F5048443F3D4356797478B9EFFCEDDED7D4CFC6B5B19E958A847D7B808587918A88899292887E6F625D5C585B59544E5E30407997A494898999969B9B908E878179726A665F5E5D5F5C5F656B77828A929BA0A59D989697985572A0ACAC8C76747D88858A8E94979492948D8D796E67616165646C7079777A7B7B877D82828D774E76989C89655C6067777C889390979894989CA19CA0A5A2A3948880737574747C8591A0A8AEB37D86ACB8AB916D686565696466676C7475848E959D9C948F8F948C8C8C878C84828584857E808A636598B2B8A4917D787773756A686E717575767A7D877F86868C958F98A09B9B9488827777727278476394AAA78C7564666663696C6A6A63615C60666B7574848A94979096978E918488868585828A5D54889CA692879494A2A6ACB0A8A4A29C958D8A858587847C89808984888C8F958C8E8E8D908480415C8D9594806D6D6C7A"
        self.builder.from_field_iterator(iter(line.split(" ")))
        channel_waveform = self.builder.build()

        assert_that(channel_waveform.get_channel_number(), is_(equal_to(1)))
        assert_that(channel_waveform.get_amplifier_version(), is_(equal_to("12.2")))
        assert_that(channel_waveform.get_antenna(), is_(equal_to(3)))
        assert_that(channel_waveform.get_gain(), is_(equal_to("8.8")))
        assert_that(channel_waveform.get_values(), is_(equal_to(512)))
        assert_that(channel_waveform.get_start(), is_(equal_to(128)))
        assert_that(channel_waveform.get_bits(), is_(equal_to(8)))
        assert_that(channel_waveform.get_shift(), is_(equal_to(4)))
        assert_that(channel_waveform.get_conversion_gap(), is_(equal_to(1618)))
        assert_that(channel_waveform.get_conversion_time(), is_(equal_to(1618)))

        waveform = channel_waveform.get_waveform()
        assert_that(len(waveform), is_(equal_to(512)))
        assert_that(waveform[0], is_(equal_to(127)))
        assert_that(waveform[1], is_(equal_to(0)))
        assert_that(waveform[2], is_(equal_to(-128)))

