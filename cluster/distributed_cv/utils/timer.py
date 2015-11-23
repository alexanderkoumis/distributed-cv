import sys
import time
import timeit


class Timer(object):

    def start(self):
        self.start_time = timeit.default_timer()
        self.start_time_str = time.strftime('%H:%M:%S')

    def end(self):
        elapsed = timeit.default_timer() - self.start_time
        end_time_str = time.strftime('%H:%M:%S')
        time_format = '\nStart time: {0}\tEnd time: {1}\tElapsed time: {2} seconds\n'
        sys.stderr.write(time_format.format(self.start_time_str, end_time_str, elapsed))
