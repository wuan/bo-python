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

import unittest
import datetime
from hamcrest import assert_that, is_, equal_to, none
from nose.tools import raises
import pytz
import numpy as np
import pandas as pd
import shapely.geometry

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
        assert_that(timestamp.value, is_(pd.NaT.value))

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
        assert_that(self.builder.id_value, is_(1234))

        self.builder.set_x(0.0)
        self.builder.set_y(0.0)
        self.builder.set_timestamp(datetime.datetime.utcnow())
        self.builder.set_amplitude(1.0)
        self.builder.set_lateral_error(5.0)
        self.builder.set_station_count(10)

        assert_that(self.builder.build().id, is_(equal_to(1234)))

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

        assert_that(self.builder.build().timestamp, is_(equal_to(pd.Timestamp(timestamp))))

    def test_set_lateral_error_lower_limit(self):
        self.builder.set_lateral_error(-1)
        assert_that(self.builder.build().lateral_error, is_(0))
        self.builder.set_lateral_error(0)
        assert_that(self.builder.build().lateral_error, is_(0))

    def test_set_lateral_error_upper_limit(self):
        self.builder.set_lateral_error(32767)
        assert_that(self.builder.build().lateral_error, is_(32767))
        self.builder.set_lateral_error(32768)
        assert_that(self.builder.build().lateral_error, is_(32767))

    def test_build_strike_from_line(self):
        strike_line = u"2013-08-08 10:30:03.644038642 pos;44.162701;8.931001;0 str;4.75 typ;0 dev;20146 sta;10;24;226,529,391,233,145,398,425,533,701,336,336,515,434,392,439,283,674,573,559,364,111,43,582,594"
        strike = self.builder.from_line(strike_line).build()

        assert_that(strike.timestamp, is_(equal_to(self.get_timestamp("2013-08-08 10:30:03.644038642"))))
        assert_that(strike.x, is_(equal_to(8.931001)))
        assert_that(strike.y, is_(equal_to(44.162701)))
        assert_that(strike.altitude, is_(equal_to(0)))
        assert_that(strike.amplitude, is_(equal_to(4.75)))
        assert_that(strike.lateral_error, is_(equal_to(20146)))
        assert_that(strike.station_count, is_(equal_to(10)))
        assert_that(strike.stations, is_(equal_to(
            [226, 529, 391, 233, 145, 398, 425, 533, 701, 336, 336, 515, 434, 392, 439, 283, 674, 573, 559,
             364, 111, 43, 582, 594])))

    @raises(blitzortung.builder.BuilderError)
    def test_build_strike_from_bad_line(self):
        strike_line = u"2013-08-08 10:30:03.644038642"
        self.builder.from_line(strike_line)


class StrikeClusterTest(TestBase):
    def setUp(self):
        self.builder = blitzortung.builder.StrikeCluster()

        self.timestamp = datetime.datetime.utcnow()
        self.seconds_interval = 10 * 60
        self.shape = shapely.geometry.LinearRing()

    def test_default_values(self):
        assert_that(self.builder.cluster_id, is_(equal_to(-1)))
        assert_that(self.builder.interval_seconds, is_(0))
        assert_that(self.builder.timestamp, is_(none()))
        assert_that(self.builder.shape, is_(none()))
        assert_that(self.builder.strike_count, is_(equal_to(0)))

    def test_with_id(self):
        self.builder.with_id(1234)
        assert_that(self.builder.cluster_id, is_(equal_to(1234)))

        assert_that(self.builder.build().id, is_(equal_to(1234)))

    def test_with_timestamp(self):
        self.builder.with_timestamp(self.timestamp)
        assert_that(self.builder.timestamp, is_(self.timestamp))

        assert_that(self.builder.build().timestamp, is_(equal_to(self.timestamp)))

    def test_with_seconds_interval(self):
        self.builder.with_interval_seconds(self.seconds_interval)
        assert_that(self.builder.interval_seconds, is_(self.seconds_interval))

        assert_that(self.builder.build().interval_seconds, is_(equal_to(self.seconds_interval)))

    def test_with_shape(self):
        self.builder.with_shape(self.shape)
        assert_that(self.builder.shape, is_(self.shape))

        assert_that(self.builder.build().shape, is_(equal_to(self.shape)))

    def test_with_strike_count(self):
        self.builder.with_strike_count(42)
        assert_that(self.builder.cluster_id, 42)

        assert_that(self.builder.build().strike_count, is_(equal_to(42)))


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

        assert_that(station.number, is_(equal_to(364)))
        assert_that(station.user, is_(equal_to(1)))
        assert_that(station.name, is_(equal_to(u'Musterdörfl')))
        assert_that(station.country, is_(equal_to('Germany')))
        assert_that(station.x, is_(equal_to(9.7314)))
        assert_that(station.y, is_(equal_to(49.5435)))
        assert_that(station.timestamp, is_(equal_to(self.get_timestamp("2012-02-10T13:39:47"))))
        assert_that(station.board, is_(equal_to(u'6.8')))

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

        assert_that(station.number, is_(equal_to(364)))
        assert_that(station.name, is_(equal_to(u'Musterdörfl')))
        assert_that(station.country, is_(equal_to(u'Germany')))
        assert_that(station.x, is_(equal_to(9.7314)))
        assert_that(station.y, is_(equal_to(49.5435)))
        assert_that(station.timestamp, is_(equal_to(self.get_timestamp("2012-02-10T14:39:47.410492123"))))
        assert_that(station.status, is_(equal_to('A')))
        assert_that(station.board, is_(equal_to('0815')))


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

        assert_that(station_offline.id, is_(equal_to(364)))
        assert_that(station_offline.number, is_(equal_to(123)))
        assert_that(station_offline.begin, is_(equal_to(begin)))
        assert_that(station_offline.end, is_(equal_to(end)))


class RawWaveformEventTest(unittest.TestCase):
    def setUp(self):
        self.builder = blitzortung.builder.RawWaveformEvent(blitzortung.builder.ChannelWaveform())

    def test_build_raw_green_event(self):
        line = "2013-09-28 20:00:54.490994382 48.000000 11.000000 500 0 GREEN 0 0.0 256 0 8 0 0 1950 69696A6B6D70727476787C80828483848484838382838384848587898A8A8A8B8C8C8D8D8D8D8C8B8987868482807D7B7A7979797A7B7C7D7D8082838484848382807F7E7C7B7978787878797B7D7F80828587898A8B8B8A88878583817E7D7B7A79797A7B7B7C7D7E7E8081818180807E7C7C7B7A797979797A7C7E8082848687898B8C8D8D8D8B8A8987858482807F7D7D7D7E80808283848586878786858381807E7D7C7B797878797A7B7D7F81828485878888888785848280807E7C7A7979797A7B7D7E8081828485868686858482807F7E7C7B79787878797B7D7F8082848688898A8A8A898785848382807F7D7D7D7D7E7F8081828283848585848381 1 GREEN 0 0.0 256 0 8 0 0 1950 8C8C8C8D8C8C8B8A89888685838281818080808080807F7F7E7D7D7C7B7A7A7A7A797979797979797A7B7C7D7E7F8082838384848584848484838281808080808080828182838384858584848483828281807F7D7D7C7C7C7C7D7E7F80818283848586868686868685858584838483838484858585868685868585838381807F7E7D7C7B7A7A797A7A7A7B7C7D7E7F8081828282828281818080807F7F7F7F7F80808181828283838383838382818180807F7E7D7D7D7D7D7E7E7F8080818283838484848383828282818180808080818182828383848585858585858483828281807E7D7C7C7C7C7C7C7D7D7E7E7F8080808180808080808080807F7F808081"
        self.builder.from_string(line)
        raw_event = self.builder.build()

        assert_that(raw_event.x, is_(equal_to(11.0)))
        assert_that(raw_event.y, is_(equal_to(48.0)))
        assert_that(raw_event.altitude, is_(equal_to(500)))

        channels = raw_event.channels
        assert_that(len(channels), is_(equal_to(2)))
        assert_that(channels[0].channel_number, is_(equal_to(0)))
        assert_that(channels[1].channel_number, is_(equal_to(1)))

    def test_build_raw_red_event(self):
        line = "2013-09-28 19:31:30.913939699 48.500000 11.500000 388 0 12.2 3 8.8 512 128 8 2 1618 1618 7374797B7F84898B8B898788838283838282817E7B7A7774767A7873859397938A847E7C7A7A7C7E7F7F7E7E7F7C7B78797B7F85888A8D8C8885817D7A79777A7E817276828688827F7F81878A8B8B86857F7A757575777B7D8081817F81808182848688888888848079666A757C7D7B7C82868A8A8986837F7F7E786D5D47362A2A34435468788997A0A6A8AAA8A6A2A199888E989A97918F90929597999A99999998938E8B878685878A8D93999FA7AFB4B9BEC8D0CEBFAA7F584537281B100F141E293747525C63696A6A6C6F73777B7D7E7F7F8084888E94999CA0A3A5A5A6938E94948F847B7777747476797E838B939CA2AAACADA8A39A948E8986827D7876726F6B6A696A6D5F6A7A81827B74706F6E6B6C72777D818689888988898D8F93979593918E89827F7C7B797D8188857C899396938B8989898A898A8C8A8A89878885868380807E7F7B78757271716F706F6D6B686563525361696C6A696E73787C78767574757373757578797979787877797E85898C8D8E8B888381827473818B8F8B858382838486888A8C8D8F8F8A86817C7572717174767A7C7D80818080808283868A7D818C8C877C7572767B80868B8F9292938F8D8C8A8B89898885847D7A78747575777B8185878B7E798487888079767475767575747373757B8184888C8D8C8888807C7777787B7D7D7F7E7D7D7D81747D8E95978F8986817E 1 12.2 3 8.8 512 128 8 4 1618 1618 8F89938C89888A80736E72747073797F808A9094969B9686858F715381A1A28F716C6B6B6B787F8A8D8C8A92908C8C827E7B7777777B7F7B7B7E797C8185898E989C63759FADA7886F63626A72787B7380818383898D929592938D8A7D7F756F6F6B6D72727B85888E8E637DACB8B099857C78746F6C6D6C7077848794A9C4D7DAD3CEBBA6957B76716A676158525357625D3E6EA3B2A1846F64625D5C5A5F676A707B7373716B73767A7E7A807E726C5F5048443F3D4356797478B9EFFCEDDED7D4CFC6B5B19E958A847D7B808587918A88899292887E6F625D5C585B59544E5E30407997A494898999969B9B908E878179726A665F5E5D5F5C5F656B77828A929BA0A59D989697985572A0ACAC8C76747D88858A8E94979492948D8D796E67616165646C7079777A7B7B877D82828D774E76989C89655C6067777C889390979894989CA19CA0A5A2A3948880737574747C8591A0A8AEB37D86ACB8AB916D686565696466676C7475848E959D9C948F8F948C8C8C878C84828584857E808A636598B2B8A4917D787773756A686E717575767A7D877F86868C958F98A09B9B9488827777727278476394AAA78C7564666663696C6A6A63615C60666B7574848A94979096978E918488868585828A5D54889CA692879494A2A6ACB0A8A4A29C958D8A858587847C89808984888C8F958C8E8E8D908480415C8D9594806D6D6C7A"
        self.builder.from_string(line)
        raw_event = self.builder.build()

        assert_that(raw_event.x, is_(equal_to(11.5)))
        assert_that(raw_event.y, is_(equal_to(48.5)))
        assert_that(raw_event.altitude, is_(equal_to(388)))

        channels = raw_event.channels
        assert_that(len(channels), is_(equal_to(2)))
        assert_that(channels[0].channel_number, is_(equal_to(0)))
        assert_that(channels[1].channel_number, is_(equal_to(1)))

    def test_build_with_missing_bits_value(self):
        line = "2014-09-14 19:52:30.507001245 48.500000 11.500000 59 0 12.3 0 16.4 512 256 0 4 952 1904 908F898A838A8785827A7A808183858484888D8D958485868688897D7F79778384868E80737982858788838483858277827A7D7D7B76736B74777B82807B747C7C80828279776F706B6F757A7B827D887A7D85848A8E8F8F8D807F827E847B7778737B8081847C7379767E868C8E8A83877F7F7A7A71726F6E6A68636F6F727875737B7F86858A88847C7A736F71726F6B605F6B687772767A7873757779777A777A71716B6F6B6B6B706A656C646D76717675747C7A797C757073736766626C727684827B7D7B797F7F7E7B74717F767678716D6A686872737D7E7A7E7B7C827B7D837F7C79737775747377767B7E8588898E8E8982827A7C83848884797977767E828188878A90898F8A92978F8C838084868283787378787C8586878A80909395999C9CA49D94938E8C8D9490969593928B909A9DA49EA0999B9DA5A7A29C938D88828281828488888C949899A0A4A5A5A9A4A09E9990938E8A8881838D89909493959497A19E9EA298928D888D8A888C85858684848C8E8E9296999A93999C9B999692908C8A8E8D8E9392909999A1A9AAACA59D9A959395959693909291A09A9F9A9796999B9E9EA3ABA8B5A298979B9FA09C999493979698A0A1A4A09F9D9D97A1A1A29D9692909A9D9D9FA19E9C9D9AA1A5A4A9A9A29F9C90939999989694979A9A9B9CA1A59F9EA19C9E9B928F898586848B97989A968E8E8E8C8E9F 1 12.3 0 10.5 512 256 0 4 952 1904 83828181838288878A8B8F9093908F898C8B86837E807B79767A7E7D7D7D7B7E8388888786827E8179837271736E6D717376747A777A797E7A8178817F82887D767373726C727171716F6E74757D7F8B8C8F90909B8E8A867E797B7779776E706E6C7C768075757F82898B8C88857E7D847E7D796F726F6A6965686C6B697477727E82868587807B7A7E7B7975756866686D71706B7378787D7C7A827E84818080757A7D797B7168736966686F767A8085878882848784848B848683807D787874787A77767676727C787C7E81817E7F7C7469635A5756585E636773788283868593949FA0A0A4AAA7AAABA5A0A19F94827A716C6972727C7B78777875757A7980797D7D818890888485858681797B83848D8E8E8E8A8C9294979B98A09EA2A8A29D9A918F8C90999A9EA09C9CA09C999C9CA09E9E979295969795928B8A878685878A88898884898789919092919493909396908D8C8C919193959894949693949A95959A9B9D99959692939797999B9C9E9994958D8C87898E8E9595948F94939690908F9594928E8B918F908A8A87888E9495959A98989797999A97A2A5A1A1A09893939194908E8F9392969797989396989DA2A5A4A09D9A98989C989697949090838E8C8C90959E9DA0A1A6A7A5A29E9A99999596938F928F929797918F97999CA2A39EA09EA09E99938C898A8B8E908D9290888D8A94999C9F9896948B 3 13.1 0 10.8 512 256 0 4 952 1904 74797D7A7C868085828085817F80777E777580757E7A7882808087848A8986908C88928A858783837B7D757676756F6E6E7274727676767878797D7777786F7E7074726E6F6F6E6B636E706F70777781797B7F7A828285857D7E7B7E807D7B7B7D7D7B7C7F808189888C908C969294959394958C8F8B8A8A8E898D848789868B8D90929798929A99999D979B969197938B918B8B8D8B888E8A8D909194969797979C99A1999D96948F90888A8E8485827D8A82898A898D8F8B928B928F8C938E8788878A8683898382848287878893908C9596999A9BA2A6A0A19C9C9A9294878482776D695F60534F514D554E4F5E5C656F76868F9AA8ACB9CACBD5D4D9DBDCD9DAD2CCC4BFB2A5A095888276716B64605C555B575A5E5D676A6F7A7984889198A1A0A7AAABAEAEB5B4AFB5BEB7B7B6B6B9B7B9B8B6B8B3C0B2B8B2ACA8A19B9B918C878282807B787672787172736F6F6D6E6E6964626163595758555A55505C5B636969767781878C95999CA4A4A4ACACAAACA9B0AAAEAEAAACB0ACAFB1ADACAAACA8A2A09D96968E8A8A7C7C7777746F6B6E6C6F6E7372707875777B787C897E8182817F858486858B8B8C999193929094928E928E92929290878F8E8A898C858789868A88868682888382827A807D7B7D7D7B79797D797B8480828585878A8A918F8D918E918B8B888587887D7D7A7A7D777A7A7C81808286828A8A888C 4 13.1 0 10.4 512 256 0 4 952 1904 6F726D6B6D696F6E74797D7A8B888A8B868784817C82797976757A7A7C7C73787A7878797C7B7C807C867A7C7B7A7779787A77767576747E787C7678757276727173767677828084828282817B7B797C7B79807D8778787A7A757D7C83868A8D92909490998A8D8782858382807D7E7B7E8285858A908F9295959997939294978C8B8882827F808381868D9090979596959597969893989395908888848483848885888E919191949A9493908F92908C8D898B888A898487938D8D908D8F8F8D8E8E8A8F8C888E8D8D908E9394939799A09C9B9B9A999697928C8583767166625B615F6B6B6F818B91949B9A9EA09FA09FA2A5ADACB2B5B4B1B0ADA8A39E9E9B9A93928E88818479726E6B696B71757B858A92969A9FA2A0A09E9B9A969490928F8C9295979CA2A6A6ADAAADA9A9A7A7A5A4A49E9F9A958F8885838281878C92989A9E9E9D99948D88837D7B75716E6B6B676A6B71737883858A8D908B908C8D8C8E8A8F8C8A8B8A8A8A8F8C969B9BA2A2A6A8AAA6A7A499A19C9995908C88828280818285898C8E8F91908D8D868282827D7D7D7B7B79797B7A80827F898D8E91949493908F8D8E8A868682818483828284898A8A8F8A8C908B8C8B8C8A8A8B88898677827D7E7A7C7D7F817E83838480818284848989918D8F8F8C8A8F8881858481827C7E7C7A797C7D808586878A8A8C88898B8A8D8A8C86837E7B777271 5 13.1 0 10.5 512 256 0 4 952 1904 6E72696D71747E7E86857F7C837B87827979747A7A827B75747276777574797A7B7D7B7B7672727881877878767375737679747A757A75767578707175747875777B7F7F778278747B7E7D80797C797577747A7B807C7B7E8285878A8C8A8A8586848D8A8B82807D7A7A7B807B85827F8A878D8E8E919492958F8F918B837D7B7C7E86888889888E888A8D90909B959697908F908B9090887D787B86878D8F8C8B888B8D8D959595958B8D84868C888C8F87877F828D8E8F988A8C89848B8E92928D838788848A9098979B9895919499A09A9798908A84847E716C615D64656E737F828D8C888B939B9DA29B9A9B989CA1AEB4B0AAA69C9F9B999B999A94918982828577756F716F6B727D8188898B959A9B9C9A999B999C938C8B8B8D8E929895969B9DA3A8A3A09AA0A0A0A4A09C9F9995928988837D80848D8F9199979A978E8C8D8C8F92877C716E6B6B7575727370767F858683898C898B88888D8C8E9088888182858995948F9195969E9D9FA5A09D9B9B999598898D8C837D797881898B8D898C8A888886848B8581837E7E7B7C7B797B7A777C84858C8C8A86898B8C90908D8B8782847F7B8083868886878685898A8D89898782878B8C8B858383817D7C7A73827E8683817C777D7F8385828687878D8D8A868684888488868580787C7B7276787E848385837F827F848D898A8D868B8784807878717079767A7C7"
        self.builder.from_string(line)
        raw_event = self.builder.build()

        channels = raw_event.channels
        assert_that(len(channels), is_(equal_to(5)))
        assert_that(channels[0].bits, is_(equal_to(8)))
        assert_that(channels[1].bits, is_(equal_to(8)))


class ChannelWaveformTest(unittest.TestCase):
    def setUp(self):
        self.builder = blitzortung.builder.ChannelWaveform()

    def test_build_raw_green_event(self):
        line = "0 GREEN 0 0.0 256 0 8 0 0 1950 81807F6B6D70727476787C80828483848484838382838384848587898A8A8A8B8C8C8D8D8D8D8C8B8987868482807D7B7A7979797A7B7C7D7D8082838484848382807F7E7C7B7978787878797B7D7F80828587898A8B8B8A88878583817E7D7B7A79797A7B7B7C7D7E7E8081818180807E7C7C7B7A797979797A7C7E8082848687898B8C8D8D8D8B8A8987858482807F7D7D7D7E80808283848586878786858381807E7D7C7B797878797A7B7D7F81828485878888888785848280807E7C7A7979797A7B7D7E8081828485868686858482807F7E7C7B79787878797B7D7F8082848688898A8A8A898785848382807F7D7D7D7D7E7F8081828283848585848381"
        self.builder.from_field_iterator(iter(line.split(" ")))
        channel_waveform = self.builder.build()

        assert_that(channel_waveform.channel_number, is_(equal_to(0)))
        assert_that(channel_waveform.amplifier_version, is_(equal_to("GREEN")))
        assert_that(channel_waveform.antenna, is_(equal_to(0)))
        assert_that(channel_waveform.gain, is_(equal_to("0.0")))
        assert_that(channel_waveform.values, is_(equal_to(256)))
        assert_that(channel_waveform.start, is_(equal_to(0)))
        assert_that(channel_waveform.bits, is_(equal_to(8)))
        assert_that(channel_waveform.shift, is_(equal_to(0)))
        assert_that(channel_waveform.conversion_gap, is_(equal_to(0)))
        assert_that(channel_waveform.conversion_time, is_(equal_to(1950)))

        waveform = channel_waveform.waveform
        assert_that(len(waveform), is_(equal_to(256)))
        assert_that(waveform[0], is_(equal_to(1)))
        assert_that(waveform[1], is_(equal_to(0)))
        assert_that(waveform[2], is_(equal_to(-1)))

    def test_build_raw_red_event(self):
        line = "1 12.2 3 8.8 512 128 8 4 1618 1618 FF80008C89888A80736E72747073797F808A9094969B9686858F715381A1A28F716C6B6B6B787F8A8D8C8A92908C8C827E7B7777777B7F7B7B7E797C8185898E989C63759FADA7886F63626A72787B7380818383898D929592938D8A7D7F756F6F6B6D72727B85888E8E637DACB8B099857C78746F6C6D6C7077848794A9C4D7DAD3CEBBA6957B76716A676158525357625D3E6EA3B2A1846F64625D5C5A5F676A707B7373716B73767A7E7A807E726C5F5048443F3D4356797478B9EFFCEDDED7D4CFC6B5B19E958A847D7B808587918A88899292887E6F625D5C585B59544E5E30407997A494898999969B9B908E878179726A665F5E5D5F5C5F656B77828A929BA0A59D989697985572A0ACAC8C76747D88858A8E94979492948D8D796E67616165646C7079777A7B7B877D82828D774E76989C89655C6067777C889390979894989CA19CA0A5A2A3948880737574747C8591A0A8AEB37D86ACB8AB916D686565696466676C7475848E959D9C948F8F948C8C8C878C84828584857E808A636598B2B8A4917D787773756A686E717575767A7D877F86868C958F98A09B9B9488827777727278476394AAA78C7564666663696C6A6A63615C60666B7574848A94979096978E918488868585828A5D54889CA692879494A2A6ACB0A8A4A29C958D8A858587847C89808984888C8F958C8E8E8D908480415C8D9594806D6D6C7A"
        self.builder.from_field_iterator(iter(line.split(" ")))
        channel_waveform = self.builder.build()

        assert_that(channel_waveform.channel_number, is_(equal_to(1)))
        assert_that(channel_waveform.amplifier_version, is_(equal_to("12.2")))
        assert_that(channel_waveform.antenna, is_(equal_to(3)))
        assert_that(channel_waveform.gain, is_(equal_to("8.8")))
        assert_that(channel_waveform.values, is_(equal_to(512)))
        assert_that(channel_waveform.start, is_(equal_to(128)))
        assert_that(channel_waveform.bits, is_(equal_to(8)))
        assert_that(channel_waveform.shift, is_(equal_to(4)))
        assert_that(channel_waveform.conversion_gap, is_(equal_to(1618)))
        assert_that(channel_waveform.conversion_time, is_(equal_to(1618)))

        waveform = channel_waveform.waveform
        assert_that(len(waveform), is_(equal_to(512)))
        assert_that(waveform[0], is_(equal_to(127)))
        assert_that(waveform[1], is_(equal_to(0)))
        assert_that(waveform[2], is_(equal_to(-128)))

