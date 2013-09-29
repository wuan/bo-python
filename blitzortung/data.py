# -*- coding: utf8 -*-

"""

@author Andreas Würl

"""

import datetime

import builder
import files
import types


class TimeRange(object):
    def __init__(self, end_time, time_delta=datetime.timedelta(hours=2)):
        self.end_time = end_time
        self.start_time = end_time - time_delta

    def __str__(self):
        return "['" + str(self.start_time) + "':'" + str(self.end_time) + "']"

    def get_start_time(self):
        return self.start_time

    def get_end_time(self):
        return self.end_time

    def get_end_minute(self):
        return self.get_end_time() - datetime.timedelta(minutes=1)

    def contains(self, time):
        return self.get_start_time() <= time < self.get_end_time()


class TimeInterval(TimeRange):
    def __init__(self, end_time, time_delta=datetime.timedelta(hours=1)):
        self.time_delta = time_delta
        TimeRange.__init__(self, self.round_time(end_time), time_delta)

    def __str__(self):
        return "['" + str(self.start_time) + "':'" + str(self.end_time) + "'," + str(self.time_delta) + "]"

    @staticmethod
    def total_seconds(time):
        """ return the total seconds of the given time or datetime (relative to midnight) """

        if isinstance(time, datetime.datetime):
            return time.hour * 3600 + time.minute * 60 + time.second
        elif isinstance(time, datetime.timedelta):
            return time.seconds + time.days * 24 * 3600
        else:
            raise Exception("unhandled type " + type(time))

    def round_time(self, time):
        delta_seconds = self.total_seconds(self.time_delta)

        seconds = self.total_seconds(time)
        seconds /= delta_seconds
        seconds *= delta_seconds

        if isinstance(time, datetime.datetime):
            return time.replace(hour=seconds // 3600, minute=seconds // 60 % 60, second=seconds % 60, microsecond=0)
        else:
            return datetime.timedelta(seconds=seconds)

    def has_next(self):
        return False

    def next(self):
        raise Exception(' no next interval ')

    def get_center_time(self):
        return self.start_time + self.time_delta / 2


class TimeIntervals(TimeInterval):
    def __init__(self, end_time, time_delta=datetime.timedelta(minutes=15), total_duration=datetime.timedelta(days=1)):
        TimeInterval.__init__(self, end_time, time_delta)

        self.total_duration = self.round_time(total_duration)

        self.start_time = self.end_time - self.total_duration

    def has_next(self):
        return self.start_time + self.time_delta < self.end_time

    def next(self):
        if self.has_next():
            self.start_time += self.time_delta
            return self.start_time
        else:
            raise Exception('no more time intervals')

    def get_end_time(self):
        return self.start_time + self.time_delta


class Event(types.Point):
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

    def __lt__(self, other):
        return self.timestamp.value < other.timestamp.value

    def __str__(self):
        timestamp = self.get_timestamp()
        if timestamp:
            timestamp_string = u"%s%03d%s" % (
                self.get_timestamp().strftime(builder.Timestamp.time_format_fractional_seconds),
                self.get_timestamp().nanosecond,
                self.timestamp.strftime('%z')
            )
        else:
            timestamp_string = u"NaT"

        return u"%s %.4f %.4f " \
               % (timestamp_string, self.x_coord, self.y_coord)


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
        return super(RawWaveformEvent, self).__str__() + "%d %d chs" \
               % (self.altitude, len(self.channels))


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
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        return u"%3d/%3d '%s' '%s' %s" % (
            self.number, self.user, self.name, self.country, super(Station, self).__str__())

    def __eq__(self, other):
        #return self.number == other.number and self.short_name == other.short_name and self.location_name == other.location_name and self.country == other.country and self.timestamp == other.timestamp
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
        return (self.get_x() != 0.0 or self.get_y() != 0.0) \
                   and -180 <= self.get_x() <= 180 \
                   and -90 < self.get_y() < 90 \
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


class Stroke(Event):
    """
    class for stroke objects
    """

    def __init__(self, stroke_id, timestamp, x_coord, y_coord, altitude, amplitude, lateral_error, station_count, stations=None):
        super(Stroke, self).__init__(timestamp, x_coord, y_coord)
        self.stroke_id = stroke_id
        self.altitude = altitude
        self.amplitude = amplitude
        self.lateral_error = lateral_error
        self.station_count = station_count
        self.stations = [] if stations is None else stations

    def get_location(self):
        """
        return location of the stroke
        """
        return self

    def get_altitude(self):
        """
        return altitude of the stroke
        """
        return self.altitude

    def get_amplitude(self):
        """
        return amplitude of the stroke
        """
        return self.amplitude

    def get_id(self):
        """
        return database id of the stroke (if applicable)
        """
        return self.stroke_id

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
        return super(Stroke, self).__str__() + "%d %.1f %d %d" % (
            self.altitude,
            self.amplitude,
            self.lateral_error,
            self.station_count
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
