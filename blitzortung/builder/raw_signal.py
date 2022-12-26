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

import itertools

from injector import inject

from .base import Event, BuilderError
from .. import data


class ChannelWaveform:
    """
    class for building of waveforms within raw signal objects
    """
    fields_per_channel = 11

    def __init__(self):
        self.channel_number = None
        self.amplifier_version = None
        self.antenna = None
        self.gain = None
        self.values = 0
        self.start = None
        self.bits = 0
        self.shift = None
        self.conversion_gap = None
        self.conversion_time = None
        self.waveform = None

    def from_field_iterator(self, field):
        self.channel_number = int(next(field))
        self.amplifier_version = next(field)
        self.antenna = int(next(field))
        self.gain = next(field)
        self.values = int(next(field))
        self.start = int(next(field))
        self.bits = int(next(field))
        self.shift = int(next(field))
        self.conversion_gap = int(next(field))
        self.conversion_time = int(next(field))
        self.__extract_waveform_from_hex_string(next(field))
        return self

    def __extract_waveform_from_hex_string(self, waveform_hex_string):
        hex_character = iter(waveform_hex_string)
        self.waveform = self.values * [0]
        bits_per_char = 4
        if self.bits == 0:
            self.bits = len(waveform_hex_string) // self.values * bits_per_char
        chars_per_sample = self.bits // bits_per_char
        value_offset = -(1 << (chars_per_sample * 4 - 1))

        for index in range(0, self.values):
            value_text = "".join(itertools.islice(hex_character, chars_per_sample))
            value = int(value_text, 16)
            self.waveform[index] = value + value_offset

    def build(self):
        return data.ChannelWaveform(
            self.channel_number,
            self.amplifier_version,
            self.antenna,
            self.gain,
            self.values,
            self.start,
            self.bits,
            self.shift,
            self.conversion_gap,
            self.conversion_time,
            self.waveform)


class RawWaveformEvent(Event):
    """
    class for building of raw signal objects
    """

    @inject
    def __init__(self, channel_builder: ChannelWaveform):
        super().__init__()
        self.altitude = 0
        self.channels = []

        self.channel_builder = channel_builder

    def build(self):
        return data.RawWaveformEvent(
            self.timestamp,
            self.x_coord,
            self.y_coord,
            self.altitude,
            self.channels
        )

    def set_altitude(self, altitude):
        self.altitude = altitude
        return self

    def from_json(self, json_object):
        self.set_timestamp(json_object[0])
        self.set_x(json_object[1])
        self.set_y(json_object[2])
        self.set_altitude(json_object[3])
        if len(json_object[9]) > 1:
            self.set_y(json_object[9][1])

        return self

    def from_string(self, string):
        """ Construct strike from blitzortung.org text format data line """
        if string:
            try:
                field = iter(string.split(' '))
                self.set_timestamp(next(field) + ' ' + next(field))
                self.timestamp.datetime += datetime.timedelta(seconds=1)
                self.set_y(float(next(field)))
                self.set_x(float(next(field)))
                self.set_altitude(int(next(field)))

                self.channels = []
                while True:
                    try:
                        self.channel_builder.from_field_iterator(field)
                    except StopIteration:
                        break
                    self.channels.append(self.channel_builder.build())
            except ValueError as e:
                raise BuilderError(e, string)

        return self
