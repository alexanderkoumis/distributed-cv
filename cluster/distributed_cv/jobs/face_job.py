"""A mrjob mapreduce wordcount example using EMR HDFS.
"""

import logging
import os
import re
import sys

import cv2
import numpy

from distributed_cv.datasets.colorferet import labels_int
from distributed_cv.utils.features import create_detector, create_recognizer
from distributed_cv.utils.timer import Timer

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


timer = Timer()

class MRFaceTask(MRJob):
    """A HDFS face/race detection interface.
    """

    def mapper_init(self):

        global timer

        self.dataset_dir = 'dataset_dir'
        self.output_dir = os.path.join(jobconf_from_env('mapreduce.task.output.dir'), 'faces')
        self.opencv_version = int(cv2.__version__.split('.')[0])

        cascade_cpu = jobconf_from_env('job.settings.cascade_cpu')
        cascade_gpu = jobconf_from_env('job.settings.cascade_gpu')
        colorferet = jobconf_from_env('job.settings.colorferet')
        gpu_or_cpu = jobconf_from_env('job.settings.gpu_or_cpu')

        self.detector = create_detector(gpu_or_cpu, cascade_cpu, cascade_gpu)
        self.recognizer = create_recognizer(colorferet)

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        self.write_results = False

        timer.start()


    def mapper(self, _, line):

        race_count = {
            'Black-or-African-American': 0,
            'Asian': 0,
            'Asian-Middle-Eastern': 0,
            'Hispanic': 0,
            'Native-American': 0,
            'Other': 0,
            'Pacific-Islander': 0,
            'White': 0
        }

        frame_path = os.path.join(self.dataset_dir, line)
        frame = cv2.imread(frame_path)
        frame_bgr = None
        if len(frame.shape) == 3:
            if frame.shape[2] > 1:
                frame_bgr = frame
                frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        for (x, y, w, h) in self.detector.detect(frame_path):
            cutout = cv2.resize(frame[y:y+h, x:x+w], (256, 256))
            race_predicted_num, conf = self.recognizer.predict(cutout)
            race_predicted_str = labels_int[int(race_predicted_num)]
            race_count[race_predicted_str] += 1
            if self.write_results:
                if frame_bgr:
                    cutout = cv2.resize(frame_bgr[y:y+h, x:x+w], (256, 256))
                cv2.putText(cutout, race_predicted_str, (0, 200), cv2.FONT_HERSHEY_SIMPLEX, .7, (255, 255, 255), 1)
                cv2.imwrite(os.path.join(self.output_dir, '{0}_{1}_{2}_{3}.jpg'.format(x, y, w, h)), cutout)
        for race in race_count:
            yield race, race_count[race]

    def combiner(self, race, count):
        yield race, sum(count)

    def reducer(self, race, count):
        yield race, sum(count)

    def reducer_final(self):
        global timer
        timer.end()

if __name__ == '__main__':
    MRFaceTask.run()

