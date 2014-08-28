# -*- coding: utf8 -*-

"""
Copyright (C) 2010-2014 Andreas Würl

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.

"""

from __future__ import print_function
import os
import subprocess
import glob
import datetime
import json
import pandas as pd

import blitzortung.builder
import blitzortung


class Raw(object):
    BO_DATA_EXECUTABLE = 'bo-data'

    def __init__(self, file_path):
        self.file_path = file_path

    def get_file_path(self):
        return self.file_path

    def get_file_name(self):
        return os.path.basename(self.file_path)

    def get_data(self, start_time=None, end_time=None):
        return [blitzortung.builder.RawEvent().from_json(element).build()
                for element in self.__execute(start_time, end_time)]

    def get_waveform_data(self, start_time=None, end_time=None):
        return [blitzortung.builder.RawWaveformEvent().from_json(element).build()
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


class RawFile(object):
    def __init__(self, config):
        raw_file_names = glob.glob(os.path.join(config.get_raw_path(), '*.bor'))

        raw_file_names.sort()

        self.raw_files = {}

        for raw_file_name in raw_file_names:
            try:
                date = datetime.datetime.strptime(raw_file_name[-12:-4], '%Y%m%d').date()
            except ValueError:
                continue
            if not date in self.raw_files:
                self.raw_files[date] = raw_file_name
            else:
                raise Exception("ERROR: double date! " + raw_file_name + " vs. " + self.raw_files[date])

    def get(self, date):
        if date in self.raw_files:
            return self.raw_files[date]
        else:
            raise Exception("no file for date " + date.strftime('%Y-%m-%d'))

    def get_dates(self):
        dates = self.raw_files.keys()
        dates.sort()
        return dates


class Archive(object):
    def __init__(self, config):
        self.dates_filecount = {}

        self.root_path = config.get_archive_path()
        root_depth = self.__get_path_depth(self.root_path)

        for current_path, dirs, files in os.walk(self.root_path):
            depth = self.__get_path_depth(current_path) - root_depth

            if depth == 3:
                date_string = "-".join(self.__split_path_into_components(current_path)[-depth:])
                self.dates_filecount[pd.Timestamp(date_string)] = len(files)

    def get_dates_filecount(self):
        return self.dates_filecount

    def get_files_for_date(self, date_string):
        result = []
        date = pd.Timestamp(date_string)
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


class Data(object):
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

        raw_file = self.raw_file_path.get_url_paths(start.date())

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
            raw_event_builder = blitzortung.builder.RawEvent()
            raw_event_builder.from_string(line)
            raw_events.append(raw_event_builder.build())

        return raw_events

    def list(self):
        for event in self.get():
            print(event)

    def list_long(self):
        for line in self.get(True):
            print(line)

