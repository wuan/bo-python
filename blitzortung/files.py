# -*- coding: utf8 -*-

'''

@author Andreas Würl

'''

import os, subprocess
import glob
import datetime
import json
import pandas as pd

import builder

class Raw(object):

    BO_DATA_EXECUTABLE = 'bo-data'
    
    def __init__(self, file_path):
        self.file_path = file_path
        
    def get_file_path(self):
        return self.file_path
    
    def get_file_name(self):
        return os.path.basename(self.file_path)
    
    def get_data(self, starttime=None, endtime=None):
        return [ builder.RawEvent().from_json(element).build()
                 for element in self.__execute(starttime, endtime) ]

    def get_waveform_data(self, starttime=None, endtime=None):
        return [ builder.RawWaveformEvent().from_json(element).build()
                 for element in self.__execute(starttime, endtime, '--long-data') ]
    
    def get_info(self, starttime=None, endtime=None):
        return self.__execute(starttime, endtime, '--mode', 'info')

    def get_histogram(self, starttime=None, endtime=None):
        return self.__execute(starttime, endtime, '--mode', 'histogram')
        
    def __repr__(self):
        return "files.Raw(%s)" % (os.path.basename(self.file_path))
    
    def __execute(self, starttime, endtime, *additional_args):
        args = [self.BO_DATA_EXECUTABLE, '-j', '-i', self.file_path]
        if starttime:
            args += ['-s', starttime]
        if endtime:
            args += ['-e', endtime]
        dataPipe = subprocess.Popen(args + list(additional_args), stdout=subprocess.PIPE)
        (output, _) = dataPipe.communicate()
        return json.loads(output)
        
class RawFile(object):

    def __init__(self, config):        
        raw_file_names = glob.glob(os.path.join(rawPath, '*.bor'))

        raw_file_names.sort()

        self.raw_files = {}

        for raw_file_name in raw_file_names:
            try:
                date = datetime.datetime.strptime(raw_file_name[-12:-4], '%Y%m%d').date()
            except ValueError:
                continue
            if not self.raw_files.has_key(date):
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
            
            for file_path in glob.glob(os.path.join(self.__get_path_for_date(date),'*')):
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
        start_date = start.strftime("%Y%m%d")

        end = self.time.get_end_minute()
        end_time = end.strftime("%H%M")

        self.error = False

        raw_file = self.raw_file_path.get(start.date())

        if long_format:
            return self.get_output(raw_file, start_time, end_time, True)
        else:
            return self.get_data(raw_file, start_time, end_time)

    def get_output(self, raw_file, starttime, endtime, long_format=False):
        cmd = ['bo-data','-i', raw_file, '-s', starttime, '-e', endtime]
        if long_format:
            cmd.append('--long-data')
        dataPipe = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        (output, _) = dataPipe.communicate()

        return output.splitlines()

    def get_data(self, raw_file, starttime, endtime):
        rawEvents = []

        for line in self.get_output(raw_file, starttime, endtime):
            raw_event_builder = builder.RawEvent()
            raw_event_builder.from_string(line)
            rawEvents.append(raw_event_builder.build())

        return rawEvents

    def list(self):
        for event in self.get():
            print event

    def list_long(self):
        for line in self.get(True):
            print line

class StatisticsData(Data):

    def get_data(self, raw_file, starttime, endtime):
        results = raw_file.get_statistical_data()
        self.count = int(results[0])
        self.mean = float(results[1])
        self.variance = float(results[2])

    def getCount(self):
        if not self.error:
            return self.count
        return 0

    def getMean(self):
        if not self.error:
            return self.mean
        return float('nan')

    def getVariance(self):
        if not self.error:
            return self.variance
        return float('nan')

class HistogramData(Data):

    def get_data(self, raw_file, starttime, endtime):
        return raw_file.get_histogram(starttime, endtime)
        dataPipe = subprocess.Popen(['bo-data','-i', raw_file, '-s', starttime, '-e', endtime, '--mode', 'histogram'], stdout=subprocess.PIPE)
        (output, _) = dataPipe.communicate()

        return output.splitlines()
