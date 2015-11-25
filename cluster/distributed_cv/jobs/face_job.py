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


from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


class MRFaceTask(MRJob):
    """A HDFS face/race detection interface.
    """

    def mapper_init(self):

        self.dataset_dir = 'dataset_dir'
        self.output_dir = os.path.join(jobconf_from_env('mapreduce.task.output.dir'), 'faces')

        cascade_cpu = jobconf_from_env('job.settings.cascade_cpu')
        cascade_gpu = jobconf_from_env('job.settings.cascade_gpu')
        colorferet = jobconf_from_env('job.settings.colorferet')
        gpu_or_cpu = jobconf_from_env('job.settings.gpu_or_cpu')

        self.detector = create_detector(gpu_or_cpu, cascade_cpu, cascade_gpu)
        self.recognizer = create_recognizer(colorferet)

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        self.write_results = False

    def mapper(self, _, line):

        def bgr2bw(frame):
            if len(frame.shape) == 3:
                if frame.shape[2] > 1:
                    frame_bgr = frame
                    return cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            return frame

        frame_path = os.path.join(self.dataset_dir, line)
        faces = self.detector.detect(frame_path)
        if len(faces) > 0:
            for (x, y, w, h) in faces:
                frame = bgr2bw(cv2.imread(frame_path))
                cutout = cv2.resize(frame[y:y+h, x:x+w], (256, 256))
                race_predicted_num, conf = self.recognizer.predict(cutout)
                race_predicted_str = labels_int[int(race_predicted_num)]
                yield race_predicted_str, 1
            # race_count[race_predicted_str] += 1
            # if self.write_results:
            #     if frame_bgr:
            #         cutout = cv2.resize(frame_bgr[y:y+h, x:x+w], (256, 256))
            #     cv2.putText(cutout, race_predicted_str, (0, 200), cv2.FONT_HERSHEY_SIMPLEX, .7, (255, 255, 255), 1)
            #     cv2.imwrite(os.path.join(self.output_dir, '{0}_{1}_{2}_{3}.jpg'.format(x, y, w, h)), cutout)
        # for race in race_count:
        #     yield race, race_count[race]

    def combiner(self, race, count):
        yield race, sum(count)

    def reducer(self, race, count):
        yield race, sum(count)



if __name__ == '__main__':
    MRFaceTask.run()

