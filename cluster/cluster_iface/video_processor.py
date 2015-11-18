import abc
import os
import sys

import cv2

face_cascade_file = './resources/haarcascades/haarcascade_frontalface_default.xml'
colorferet_dir = './resources/colorferet'


class _VideoProcessor(object):

    __metaclass__ = abc.ABCMeta
    kill = False

    def __init__(self, video_path):
        self.video_path = video_path

    @abc.abstractmethod
    def _process(self, frame_bgr, num_curr):
        raise NotImplementedError

    def run(self):

        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            print self.video_path, 'doesn\'t exist'
            sys.exit(1)

        frame_num_curr = int(cap.get(cv2.cv.CV_CAP_PROP_POS_FRAMES))
        frame_num_total = int(cap.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))

        while True:
            flag, frame_bgr = cap.read()
            if flag:
                frame_num_curr = int(cap.get(cv2.cv.CV_CAP_PROP_POS_FRAMES))
		self._process(frame_bgr, frame_num_curr, frame_num_total)
                if frame_num_curr == frame_num_total or self.kill:
                    break
            else:
                print 'Error reading frame from file:', self.video_path

        print 'Read all images'


class FaceProcessorStandalone(_VideoProcessor):

    def __init__(self, video_path):
        super(FaceProcessorStandalone, self).__init__(video_path)
        self.face_cascade = cv2.CascadeClassifier(os.path.abspath(face_cascade_file))

    def _process(self, frame_bgr, num_curr, num_total):

        frame_gray = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2GRAY)
        faces = self.face_cascade.detectMultiScale(frame_gray, 2, 8)

        for (x, y, w, h) in faces:
            cv2.rectangle(frame_bgr, (x, y), (x + w, y + h), (255,0,0), 2)

        cv2.imshow('face_capture', frame_bgr)
        if cv2.waitKey(10) == 27:
            print 'Exiting: User entered esc'
            sys.exit()


class SplitProcessor(_VideoProcessor):

    file_list = []
    list_path = ""
    verbose = True

    def __init__(self, video_path, out_dir, out_ext):
        super(SplitProcessor, self).__init__(video_path)
        self.ext = out_ext
        self.stem = os.path.splitext(os.path.basename(video_path))[0]
        self.base = os.path.join(out_dir, self.stem)
        self.first = True
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        self.run()
        self._write_list(out_dir)

    def _process(self, frame_bgr, num_curr, num_total):

        def _filename(frame_num):
            return '{}_{}.{}'.format(self.base, frame_num, self.ext)

        if self.first:
            self.first = False
            filename_last = _filename(num_total)
            for frame_num in range(num_curr, num_total):
                filename_curr = _filename(frame_num)
                self.file_list.append(filename_curr)
            if os.path.isfile(filename_last):
                self.kill = True
                print 'All frames already processed'
                return

        filename_curr = _filename(num_curr)
        if not os.path.isfile(filename_curr):
            cv2.imwrite(filename_curr, frame_bgr)
            if self.verbose:
                print 'wrote frame :: {}/{}'.format(num_curr, num_total)    
        elif self.verbose:
            print 'skipping frame (exists) :: {}/{}'.format(num_curr, num_total)
    
    def _write_list(self, out_dir):
        self.list_path = os.path.join(out_dir, 'list.txt')
        list_file = open(self.list_path, 'w')
        for frame_path in self.file_list:
            list_file.write("%s\n" % os.path.basename(frame_path))
        list_file.close()

    def remove_files(self):
        for frame_path in self.frame_paths:
            os.remove(frame_path)
        os.remove(self.list_path)
