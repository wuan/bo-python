# -*- coding: utf8 -*-

from __future__ import unicode_literals
from . import types


class Event(types.Point):
    time_format = '%Y-%m-%d %H:%M:%S'
    time_format_fractional_seconds = time_format + '.%f'

    def __init__(self, timestamp, x_coord_or_point, y_coord=None):
        super(Event, self).__init__(x_coord_or_point, y_coord)
        self.timestamp = timestamp

    def get_timestamp(self):
        return self.timestamp

    def difference_to(self, other):
        return self.timestamp - other.timestamp

    def ns_difference_to(self, other):
        return other.timestamp.value - self.timestamp.value

    def has_same_location(self, other):
        return super(Event, self).__eq__(other)

    def is_valid(self):
        return (self.get_x() != 0.0 or self.get_y() != 0.0) \
               and -180 <= self.get_x() <= 180 \
               and -90 < self.get_y() < 90 \
               and self.has_valid_timestamp()

    def has_valid_timestamp(self):
        return self.timestamp is not None and self.timestamp.year > 1900

    def __lt__(self, other):
        return self.timestamp.value < other.timestamp.value

    def __le__(self, other):
        return self.timestamp.value <= other.timestamp.value

    def __str__(self):
        if self.has_valid_timestamp():
            timestamp_string = "%s%03d%s" % (
                self.get_timestamp().strftime(self.time_format_fractional_seconds),
                self.get_timestamp().nanosecond,
                self.timestamp.strftime('%z')
            )
        else:
            timestamp_string = "NaT"

        return "%s %.4f %.4f" \
               % (timestamp_string, self.x_coord, self.y_coord)

    def __hash__(self):
        return super(Event, self).__hash__() ^ hash(self.timestamp)

    def get_uuid(self):
        return "%s-%05.0f-%05.0f" \
               % (str(self.get_timestamp().value), self.x_coord * 100, self.y_coord * 100)


class RawWaveformEvent(Event):
    def __init__(self, timestamp, x_coord, y_coord, altitude, channels):
        super(RawWaveformEvent, self).__init__(timestamp, x_coord, y_coord)
        self.altitude = altitude
        self.channels = channels

    def get_altitude(self):
        return self.altitude

    def get_channels(self):
        return self.channels

    def __str__(self):
        return super(RawWaveformEvent, self).__str__() + "%d %d chs" % (self.altitude, len(self.channels))

    def __repr__(self):
        return self.__str__()


class Station(Event):
    """
    class for station objects
    """

    def __init__(self, number, user, name, country, x_coord, y_coord, last_data, status, board):
        super(Station, self).__init__(last_data, x_coord, y_coord)
        self.number = number
        self.user = user
        self.name = name
        self.country = country
        self.status = status
        self.board = board

    def __str__(self):
        return "%3d/%3d '%s' '%s' %s" % (
            self.number, self.user, self.name, self.country, super(Station, self).__str__())

    def __eq__(self, other):
        return self.number == other.number and self.name == other.name and self.country == other.country

    def __ne__(self, other):
        return not self == other

    def get_number(self):
        """
        returns the station number
        """
        return self.number

    def get_user(self):
        """
        returns the user id for which the station is registered
        """
        return self.user

    def get_name(self):
        """
        returns the name of the station
        """
        return self.name

    def get_country(self):
        """
        returns the country in which the station resides
        """
        return self.country

    def get_status(self):
        """
        get the current station status
        """
        return self.status

    def get_board(self):
        return self.board

    def is_valid(self):
        return super(Station, self).is_valid() \
               and self.get_number() > 0


class StationOffline(object):
    """
    class for station offline information objects
    """

    def __init__(self, id_number, number, begin, end=None):
        self.id_number = id_number
        self.number = number
        self.begin = begin
        self.end = end

    def get_id(self):
        """
        return db id of offline info
        """
        return self.id_number

    def get_number(self):
        """
        return number of related station
        """
        return self.number

    def get_begin(self):
        """
        return start of offline time
        """
        return self.begin

    def get_end(self):
        """
        return end of offline time
        """
        return self.end

    def set_end(self, end):
        """
        set end of offline time
        """
        if not self.end:
            self.end = end
        else:
            raise ValueError('cannot overwrite end of StationOffline when already set')


class Strike(Event):
    """
    class for strike objects
    """

    def __init__(self, strike_id, timestamp, x_coord, y_coord, altitude, amplitude, lateral_error, station_count,
                 stations=None):
        super(Strike, self).__init__(timestamp, x_coord, y_coord)
        self.strike_id = strike_id
        self.altitude = altitude
        self.amplitude = amplitude
        self.lateral_error = lateral_error
        self.station_count = station_count
        self.stations = [] if stations is None else stations

    def get_location(self):
        """
        return location of the strike
        """
        return self

    def get_altitude(self):
        """
        return altitude of the strike
        """
        return self.altitude

    def get_amplitude(self):
        """
        return amplitude of the strike
        """
        return self.amplitude

    def get_id(self):
        """
        return database id of the strike (if applicable)
        """
        return self.strike_id

    def get_lateral_error(self):
        """
        return location error in meters
        """
        return self.lateral_error

    def get_station_count(self):
        """
        return count of participated stations
        """
        return self.station_count

    def has_participant(self, participant):
        """
        returns true if the given participant is contained in the stations list
        """
        return self.stations.count(participant) > 0

    def get_stations(self):
        """
        return list of participated stations
        """
        return self.stations

    def __str__(self):
        return super(Strike, self).__str__() + " %s %.1f %d %d" % (
            str(self.altitude) if self.altitude is not None else '-',
            self.amplitude if self.amplitude else 0.0,
            self.lateral_error,
            self.station_count if self.station_count else 0
        )


class ChannelWaveform(object):
    """
    class for raw data waveform channels
    """

    def __init__(self, channel_number, amplifier_version, antenna, gain, values, start, bits, shift, conversion_gap,
                 conversion_time, waveform):
        self.channel_number = channel_number
        self.amplifier_version = amplifier_version
        self.antenna = antenna
        self.gain = gain
        self.values = values
        self.start = start
        self.bits = bits
        self.shift = shift
        self.conversion_gap = conversion_gap
        self.conversion_time = conversion_time
        self.waveform = waveform

    def get_channel_number(self):
        """
        returns the channel number
        """
        return self.channel_number

    def get_amplifier_version(self):
        """
        returns the amplifier version used (GREEN is returned for old systems)
        """
        return self.amplifier_version

    def get_antenna(self):
        """
        get antenna type index
        """
        return self.antenna

    def get_gain(self):
        """
        get gain setting used
        the two gain values are dot separated
        """
        return self.gain

    def get_values(self):
        """
        get number of values recorded
        """
        return self.values

    def get_start(self):
        """
        get number of pretrigger samples
        """
        return self.start

    def get_bits(self):
        """
        get bits used per sample
        """
        return self.bits

    def get_shift(self):
        """
        no clue what this means
        """
        return self.shift

    def get_conversion_gap(self):
        """
        get the conversion gap
        """
        return self.conversion_gap

    def get_conversion_time(self):
        """
        get the conversion time which is the time between consecutive samples
        """
        return self.conversion_time

    def get_waveform(self):
        """
        get array of integer numbers representing the measured waveform
        """
        return self.waveform
