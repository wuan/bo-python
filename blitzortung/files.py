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

import datetime
import glob
import json
import os
import subprocess

from blitzortung.data import Timestamp
from . import builder


class Raw:
    BO_DATA_EXECUTABLE = 'bo-data'

    def __init__(self, file_path):
        self.file_path = file_path

    def get_file_path(self):
        return self.file_path

    def get_file_name(self):
        return os.path.basename(self.file_path)

    def get_data(self, start_time=None, end_time=None):
        return [builder.RawEvent().from_json(element).build()
                for element in self.__execute(start_time, end_time)]

    def get_waveform_data(self, start_time=None, end_time=None):
        return [builder.RawWaveformEvent().from_json(element).build()
                for element in self.__execute(start_time, end_time, '--long-data')]

    def get_info(self, start_time=None, end_time=None):
        return self.__execute(start_time, end_time, '--mode', 'info')

    def get_histogram(self, start_time=None, end_time=None):
        return self.__execute(start_time, end_time, '--mode', 'histogram')

    def __repr__(self):
        return "files.Raw(%s)" % (os.path.basename(self.file_path))

    def __execute(self, start_time, end_time, *additional_args):
        args = [self.BO_DATA_EXECUTABLE, '-j', '-i', self.file_path]
        if start_time:
            args += ['-s', start_time]
        if end_time:
            args += ['-e', end_time]
        data_pipe = subprocess.Popen(args + list(additional_args), stdout=subprocess.PIPE)
        (output, _) = data_pipe.communicate()
        return json.loads(output)


class RawFile:
    def __init__(self, config):
        raw_file_names = glob.glob(os.path.join(config.get_raw_path(), '*.bor'))

        raw_file_names.sort()

        self.raw_files = {}

        for raw_file_name in raw_file_names:
            try:
                date = datetime.datetime.strptime(raw_file_name[-12:-4], '%Y%m%d').date()
            except ValueError:
                continue
            if date not in self.raw_files:
                self.raw_files[date] = raw_file_name
            else:
                raise ValueError("ERROR: double date! " + raw_file_name + " vs. " + self.raw_files[date])

    def get(self, date):
        if date in self.raw_files:
            return self.raw_files[date]
        else:
            raise KeyError("no file for date " + date.strftime('%Y-%m-%d'))

    def get_dates(self):
        dates = self.raw_files.keys()
        dates.sort()
        return dates


class Archive:
    def __init__(self, config):
        self.dates_filecount = {}

        self.root_path = config.get_archive_path()
        root_depth = self.__get_path_depth(self.root_path)

        for current_path, dirs, files in os.walk(self.root_path):
            depth = self.__get_path_depth(current_path) - root_depth

            if depth == 3:
                date_string = "-".join(self.__split_path_into_components(current_path)[-depth:])
                self.dates_filecount[Timestamp(date_string)] = len(files)

    def get_dates_filecount(self):
        return self.dates_filecount

    def get_files_for_date(self, date_string):
        result = []
        date = Timestamp(date_string)
        if date in self.dates_filecount:

            for file_path in glob.glob(os.path.join(self.__get_path_for_date(date), '*')):
                result.append(Raw(file_path))

        return result

    def __get_path_for_date(self, date):
        path = self.root_path

        for format_string in ['%Y', '%m', '%d']:
            path = os.path.join(path, date.strftime(format_string))

        return path

    def __get_path_depth(self, path):
        return len(self.__split_path_into_components(path))

    def __split_path_into_components(self, path):
        (rest, last) = os.path.split(path)
        if last == "":
            return []
        else:
            components = self.__split_path_into_components(rest)
            components.append(last)
            return components


class Data:
    def __init__(self, raw_file_path, time):
        self.raw_file_path = raw_file_path
        self.time = time
        self.error = False

    def get(self, long_format=False):
        start = self.time.get_start_time()
        start_time = start.strftime("%H%M")

        end = self.time.get_end_minute()
        end_time = end.strftime("%H%M")

        self.error = False

        raw_file = self.raw_file_path.get_paths(start.date())

        if long_format:
            return self.get_output(raw_file, start_time, end_time, True)
        else:
            return self.get_data(raw_file, start_time, end_time)

    @staticmethod
    def get_output(raw_file, start_time, end_time, long_format=False):
        cmd = ['bo-data', '-i', raw_file, '-s', start_time, '-e', end_time]
        if long_format:
            cmd.append('--long-data')
        data_pipe = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        (output, _) = data_pipe.communicate()

        return output.splitlines(keepends=False)

    def get_data(self, raw_file, start_time, end_time):
        raw_events = []

        for line in self.get_output(raw_file, start_time, end_time):
            raw_event_builder = builder.RawEvent()
            raw_event_builder.from_string(line)
            raw_events.append(raw_event_builder.build())

        return raw_events

    def list(self):
        for event in self.get():
            print(event)

    def list_long(self):
        for line in self.get(True):
            print(line)
