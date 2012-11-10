# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

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
        return time >= self.get_start_time() and time < self.get_end_time()

class TimeInterval(TimeRange):

    def __init__(self, end_time, time_delta=datetime.timedelta(hours=1)):
        self.time_delta = time_delta
        TimeRange.__init__(self, self.round_time(end_time), time_delta)

    def __str__(self):
        return "['" + str(self.start_time) + "':'" + str(self.end_time) + "'," + str(self.time_delta) + "]"

    def total_seconds(self, time):
        ' return the total seconds of the given time or datetime (relative to midnight) '

        if isinstance(time, datetime.datetime):
            return time.hour * 3600 + time.minute * 60 + time.second
        elif isinstance(time, datetime.timedelta):
            return time.seconds + time.days * 24 * 3600
        else:
            raise Exception("unhandled type " + type(time))

    def round_time(self, time):
        delta_seconds = self.total_seconds(self.time_delta)

        seconds =  self.total_seconds(time)
        seconds /= delta_seconds
        seconds *= delta_seconds

        if isinstance(time, datetime.datetime):
            return time.replace(hour = seconds/3600, minute= seconds / 60 % 60, second= seconds % 60, microsecond=0)
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

    def __init__(self, timestamp, x_coord_or_point, y_coord = None):
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
        return "%s%03d%s %.4f %.4f " \
               % (self.get_timestamp().strftime(builder.Base.timeformat_fractional_seconds),
                  self.get_timestamp().nanosecond,
                  self.timestamp.strftime('%z'), self.x_coord, self.y_coord)

class RawEvent(Event):
    def __init__(self, timestamp, x_coord, y_coord, altitude, amplitude, angle):
        super(RawEvent, self).__init__(timestamp, x_coord, y_coord)
        self.altitude = altitude
        self.amplitude = amplitude
        self.angle = angle

    def get_altitude(self):
        return self.altitude
    
    def get_amplitude(self):
        return self.amplitude

    def get_angle(self):
        return self.angle

    def __str__(self):
        return super(RawEvent, self).__str__() + "%d %.2f %.2f" %(self.altitude, self.amplitude, self.angle)


class RawWaveformEvent(Event):
    def __init__(self, timestamp, x_coord, y_coord, altitude, sample_period, amplitude_x, amplitude_y, angle_offset):
        super(RawWaveformEvent, self).__init__(timestamp, x_coord, y_coord)
        self.altitude = altitude
        self.sample_period = sample_period
        self.amplitude_x = amplitude_x
        self.amplitude_y = amplitude_y
        self.angle_offset = angle_offset

    def get_altitude(self):
        return self.altitude
    
    def get_sample_period(self):
        return self.sample_period
    
    def get_x_amplitude(self, index):
        return self.amplitude_x[index]

    def get_y_amplitude(self, index):
        return self.amplitude_y[index]
    
    def get_angle_offset(self):
        return self.angle_offset

    def __str__(self):
        return super(RawWaveformEvent, self).__str__() + "%d %d %s %s %.2f" %(self.altitude, self.sample_period, str(self.amplitude_x), str(self.amplitude_y), self.angle_offset)


class ExtEvent(RawEvent):
    def __init__(self, x_coord, y_coord, timestamp, timestamp_nanoseconds, height, number_of_satellites, sample_period, amplitude_x, amplitude_y, station_number):
        super(ExtEvent, self).__init__(x_coord, y_coord, timestamp, timestamp_nanoseconds, height, number_of_satellites, sample_period, amplitude_x, amplitude_y)

        self.station_number = station_number

    def __str__(self):
        return "%03d %s" %(self.station_number, super(ExtEvent, self).__str__() )

    def get_station_number(self):
        return self.station_number



class Station(Event):

    def __init__(self, number, short_name, name, location_name, country, x_coord, y_coord, last_data, gps_status, tracker_version, samples_per_hour):
        super(Station, self).__init__(last_data, x_coord, y_coord)
        self.number = number
        self.short_name = short_name
        self.name = name
        self.location_name = location_name
        self.country = country
        self.gps_status = gps_status
        self.tracker_version = tracker_version
        self.samples_per_hour = samples_per_hour

    def __str__(self):
        return u"%d %s %s %s %s %s" %(self.number, self.short_name, self.location_name, self.country, super(Station, self).__str__(), self.get_timestamp().strftime(builder.Base.timeformat))

    def __eq__(self, other):
        #return self.number == other.number and self.short_name == other.short_name and self.location_name == other.location_name and self.country == other.country and self.timestamp == other.timestamp
        return self.number == other.number and self.short_name == other.short_name and self.location_name == other.location_name and self.country == other.country

    def __ne__(self, other):
        return not self == other

    def get_number(self):
        return self.number

    def get_short_name(self):
        return self.short_name

    def get_name(self):
        return self.name

    def get_location_name(self):
        return self.location_name

    def get_country(self):
        return self.country

    def get_gps_status(self):
        return self.gps_status

    def get_tracker_version(self):
        return self.tracker_version

    def get_samples_per_hour(self):
        return self.samples_per_hour

class StationOffline(object):

    def __init__(self, id_number, number, begin, end=None):
        self.id_number = id_number
        self.number = number
        self.begin = begin
        self.end = end

    def get_id(self):
        return self.id_number

    def get_number(self):
        return self.number

    def get_begin(self):
        return self.begin

    def get_end(self):
        return self.end

    def set_end(self, end):
        if not self.end:
            self.end = end
        else:
            raise ValueError('cannot overwrite end of StationOffline when already set')


class Stroke(Event):
    '''
    classdocs
    '''

    def __init__(self, stroke_id, x_coord, y_coord, timestamp, amplitude, height, lateral_error, type_val, station_count, participants = None):
        super(Stroke, self).__init__(x_coord, y_coord, timestamp)
        self.stroke_id = stroke_id
        self.amplitude = amplitude
        self.height = height
        self.lateral_error = lateral_error
        self.type_val = type_val
        self.station_count = station_count
        self.participants = [] if participants == None else participants

    def get_location(self):
        return self

    def get_height(self):
        return self.height

    def get_amplitude(self):
        return self.amplitude

    def get_type(self):
        return self.type_val

    def get_id(self):
        return self.stroke_id

    def get_lateral_error(self):
        return self.lateral_error

    def get_station_count(self):
        return self.station_count

    def has_participant(self, participant):
        return self.participants.count(participant) > 0

    def get_participants(self):
        return self.participants

    def is_detected_by_user(self):
        return False

    def __str__(self):
        return super(Stroke, self).__str__() + "%s %d %d %.1f %d" \
               % (str(self.height) if self.height else '-', self.amplitude, self.type_val, self.lateral_error, self.station_count)

class Histogram(object):

    def __init__(self, file_names, time):
        data = files.StatisticsData(file_names, time)

        self.histogram = []
        while True:

            data.get()

            entry = {}

            entry['center_time'] = time.get_center_time()
            entry['count'] = data.getCount()
            entry['mean'] = data.getMean()
            entry['variance'] = data.getVariance()

            self.histogram.append(entry)

            if not time.has_next():
                break

            time.next()

    def get(self):
        return self.histogram

class AmplitudeHistogram(object):

    def __init__(self, file_names, time):
        data = files.HistogramData(file_names, time)

        data.list()
