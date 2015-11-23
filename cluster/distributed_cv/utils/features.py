import os
import sys

import cv2
import cv2gpu
import numpy

from distributed_cv.datasets.colorferet import load_from_small_dataset


def create_detector(gpu_or_cpu, cascade_cpu, cascade_gpu):
    if not os.path.isfile(cascade_cpu) or not os.path.isfile(cascade_gpu):
        sys.exit('{} or {} is not a file'.format(cascade_cpu, cascade_gpu))
    if cv2gpu.is_cuda_compatible() and gpu_or_cpu == 'gpu':
        sys.stdout.write('Using GPU CascadeClassifier\n')
        cv2gpu.init_gpu_detector(cascade_gpu)
    else:
        sys.stdout.write('Using CPU CascadeClassifier\n')
        cv2gpu.init_cpu_detector(cascade_cpu)

    class Detector(object):
        def detect(self, image_path):
            return cv2gpu.find_faces(image_path)

    return Detector()

def create_recognizer(dataset_dir):
    recognizer = None
    opencv_version = int(cv2.__version__.split('.')[0])
    cv2face = None
    if opencv_version == 2:
        recognizer = cv2.createLBPHFaceRecognizer()
        # recognizer = cv2.createFisherFaceRecognizer()
        # recognizer = cv2.createEigenFaceRecognizer()
    elif opencv_version == 3:
        recognizer = cv2.face.createLBPHFaceRecognizer()
        # recognizer = cv2.face.createFisherFaceRecognizer()
        # recognizer = cv2.face.createEigenFaceRecognizer()
    else:
        sys.exit('Unknown OpenCV version:', opencv_version)

    images, labels = load_from_small_dataset(dataset_dir)
    recognizer.train(images, numpy.array(labels))
    return recognizer
