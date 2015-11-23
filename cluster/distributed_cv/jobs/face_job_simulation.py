import os
import shutil
import tarfile

import cv2

from distributed_cv.datasets.colorferet import labels_int
from distributed_cv.utils.features import create_detector, create_recognizer
from distributed_cv.utils.printing import print_debug
from distributed_cv.utils.timer import Timer


class MRFaceTaskSimulation(object):

    def __init__(self, file_list, **kwargs):

        self.file_list = file_list
        gpu_or_cpu = kwargs['hardware_type']
        resources_dir = kwargs['resources_dir']
        colorferet_tar = os.path.join(resources_dir, kwargs['colorferet'])
        cascade_cpu = os.path.join(resources_dir, kwargs['cascade_cpu'])
        cascade_gpu = os.path.join(resources_dir, kwargs['cascade_gpu'])

        tar_stem = kwargs['colorferet'].split('.')[0]
        colorferet_dir = os.path.join(resources_dir, tar_stem)

        with tarfile.open(colorferet_tar) as tar:
            tar.extractall(path=colorferet_dir)

        self.detector = create_detector(gpu_or_cpu, cascade_cpu, cascade_gpu)
        self.recognizer = create_recognizer(colorferet_dir)
        shutil.rmtree(colorferet_dir, ignore_errors=True)

        self.timer = Timer()
        self.timer.start()

    def run(self):
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
        for file_path in self.file_list:
            frame = cv2.imread(file_path)
            frame_bgr = None
            if len(frame.shape) == 3:
                if frame.shape[2] > 1:
                    frame_bgr = frame
                    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            for (x, y, w, h) in self.detector.detect(file_path):
                cutout = cv2.resize(frame[y:y+h, x:x+w], (256, 256))
                race_predicted_num, conf = self.recognizer.predict(cutout)
                race_predicted_str = labels_int[int(race_predicted_num)]
                race_count[race_predicted_str] += 1
        results = []
        for race in race_count:
            count = race_count[race]
            results.append((race, count))
        return results
