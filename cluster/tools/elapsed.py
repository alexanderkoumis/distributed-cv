#!/usr/bin/env python

import gzip
import os
import re
import sys


def seconds_since_midnight(time):
    h_m_s, ms = time.split(',')
    h, m, s = h_m_s.split(':')
    s_total = int(h) * 60 * 60
    s_total += int(m) * 60
    s_total += int(s)
    s_total += float(ms) * 0.001
    return s_total

def elapsed(time_start, time_end):
    time_start_sec = seconds_since_midnight(time_start)
    time_end_sec = seconds_since_midnight(time_end)
    return float('%.6g' % (time_end_sec - time_start_sec))

def read_dir(path):

    def parse_syslog(syslog):
        re_start = r'.*(\d+:\d+:\d+,\d+)\sINFO.*Job.*running.*'
        re_end = r'.*(\d+:\d+:\d+,\d+)\sINFO.*Job.*completed.*'
        time_start = None
        time_end = None

        def set_time(regexp, line):
            match = re.match(regexp, line)
            return match.group(1) if match else None

        for line in syslog.split('\n'):
            if time_start:
                time_end = set_time(re_end, line)
                if time_end:
                    return elapsed(time_start, time_end)
            else:
                time_start = set_time(re_start, line)

        return None

    for dirpath, dirnames, filenames in os.walk(path):
        filenames_full = [os.path.join(dirpath, f) for f in filenames]
        for filename in filenames_full:
            if 'syslog.gz' in filename:
                with gzip.open(filename, 'rb') as syslog:
                    seconds = parse_syslog(syslog.read())
                    if seconds:
                        print filename, seconds

if os.path.isdir(sys.argv[1]):
    read_dir(sys.argv[1])
else:
    print elapsed(sys.argv[1], sys.argv[2])

