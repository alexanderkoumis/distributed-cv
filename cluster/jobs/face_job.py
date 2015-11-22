"""A mrjob mapreduce wordcount example using EMR HDFS.
"""

import logging
import os
import re
import sys
import time
import timeit

import cv2
import numpy

import cv2gpu

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

class Timer(object):

    def __init__(self):
        self.start_time = timeit.default_timer()
        self.start_time_str = time.strftime('%H:%M:%S')

    def time(self):
        elapsed = timeit.default_timer() - self.start_time
        end_time_str = time.strftime('%H:%M:%S')
        time_format = '\nStart time: {0}\tEnd time: {1}\tElapsed time: {2} seconds\n'
        sys.stderr.write(time_format.format(self.start_time_str, end_time_str, elapsed))


timer = Timer()

class MRFaceTask(MRJob):
    """A HDFS face/race detection interface.
    """


    def mapper_init(self):

        def load_from_small_dataset(colorferet_small_dir):

            face_labels_str = {
                'Black-or-African-American': 0,
                'Asian': 1,
                'Asian-Middle-Eastern': 2,
                'Hispanic': 3,
                'Native-American': 4,
                'Other': 5,
                'Pacific-Islander': 6,
                'White': 7
            }

            images = []
            labels = []

            for face_label_str in face_labels_str:
                face_label_num = face_labels_str[face_label_str]
                image_dir = os.path.join(colorferet_small_dir, face_label_str)
                image_files = [os.path.join(image_dir, image_file) for image_file in os.listdir(image_dir)]
                images_tmp = [cv2.resize(cv2.imread(image_file, 0), (256, 256)) for image_file in image_files if image_file.split('.')[-1] == 'png']
                labels_tmp = [face_label_num] * len(images_tmp)
                images.extend(images_tmp)
                labels.extend(labels_tmp)

            return images, labels

        self.dataset_dir = 'dataset_dir'
        self.output_dir = os.path.join(jobconf_from_env('mapreduce.task.output.dir'), 'faces')
        self.opencv_version = int(cv2.__version__.split('.')[0])

        if self.opencv_version == 2:
            self.recognizer = cv2.createLBPHFaceRecognizer()
            # self.recognizer = cv2.createFisherFaceRecognizer()
            # self.recognizer = cv2.createEigenFaceRecognizer()
        elif self.opencv_version == 3:
            self.recognizer = cv2.face.createLBPHFaceRecognizer()
            # self.recognizer = cv2.face.createFisherFaceRecognizer()
            # self.recognizer = cv2.face.createEigenFaceRecognizer()

        gpu_or_cpu = jobconf_from_env('job.settings.gpu_or_cpu')

        if cv2gpu.is_cuda_compatible() and gpu_or_cpu == 'gpu':
            sys.stderr.write('Using GPU CascadeClassifier')
            cv2gpu.init_gpu_detector(jobconf_from_env('job.settings.cascade_gpu'))
        else:
            sys.stderr.write('Using CPU CascadeClassifier')
            cv2gpu.init_cpu_detector(jobconf_from_env('job.settings.cascade_cpu'))

        images, labels = load_from_small_dataset(jobconf_from_env('job.settings.colorferet'))

        self.recognizer.train(images, numpy.array(labels))
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        self.face_labels_num = {
            0: 'Black-or-African-American',
            1: 'Asian',
            2: 'Asian-Middle-Eastern',
            3: 'Hispanic',
            4: 'Native-American',
            5: 'Other',
            6: 'Pacific-Islander',
            7: 'White'
        }

        self.write_results = False


    def mapper(self, _, line):

        race_predicted = {
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
        for (x, y, w, h) in cv2gpu.find_faces(frame_path):
            cutout = cv2.resize(frame[y:y+h, x:x+w], (256, 256))
            race_predicted_num, conf = self.recognizer.predict(cutout)
            race_predicted_str = self.face_labels_num[int(race_predicted_num)]
            race_predicted[race_predicted_str] += 1
            if self.write_results:
                if frame_bgr:
                    cutout = cv2.resize(frame_bgr[y:y+h, x:x+w], (256, 256))
                cv2.putText(cutout, race_predicted_str, (0, 200), cv2.FONT_HERSHEY_SIMPLEX, .7, (255, 255, 255), 1)
                cv2.imwrite(os.path.join(self.output_dir, '{0}_{1}_{2}_{3}.jpg'.format(x, y, w, h)), cutout)
        for race in race_predicted:
            yield race, race_predicted[race]

    def combiner(self, race, count):
        yield race, sum(count)

    def reducer(self, race, count):
        yield race, sum(count)

    def reducer_final(self):
        timer.time()

if __name__ == '__main__':
    MRFaceTask.run()

