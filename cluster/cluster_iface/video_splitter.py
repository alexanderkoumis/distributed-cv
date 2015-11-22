"""A video splitter module.
This library module splits a video file into a set of images using OpenCV's 
VideoCapture interface.
"""

import abc
import os
import sys

import cv2

class VideoSplitter(object):

    debug = True
    file_list = []
    list_path = ''

    def __init__(self, video_path, out_dir, out_ext):
        self.video_path = video_path
        self.out_dir = out_dir
        self.ext = out_ext
        self.stem = os.path.splitext(os.path.basename(video_path))[0]
        self.base = os.path.join(out_dir, self.stem)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

    def split(self):
        cap = self._init_cap()
        self._run(cap)
        self._write_list(self.out_dir)
        return self.file_list, self.list_path

    def _debug_print(self, message):
        if self.debug:
            print message

    def _init_cap(self):
        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            print self.video_path, 'doesn\'t exist'
            sys.exit(1)
        return cap

    def _append_filename(self, filename_curr):
        self.file_list.append(filename_curr)

    def _run(self, cap):

        frame_num_total = int(cap.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
        frame_num_last = 0

        if self._already_split(frame_num_total):
            [self._append_filename(os.path.join(self.out_dir, filename)) for filename in os.listdir(self.out_dir) if filename.endswith(self.ext)]
            print 'All frames already processed'
            return

        while True:
            frame_valid, frame_bgr = cap.read()
            if frame_valid:
                frame_num_curr = int(cap.get(cv2.cv.CV_CAP_PROP_POS_FRAMES))
                if frame_num_curr == frame_num_last:
                    continue
                frame_num_last = frame_num_curr
                filename_curr = self._filename(frame_num_curr)
                self._append_filename(filename_curr)
                if os.path.isfile(filename_curr):
                    self._debug_print('skipping frame (exists) :: {}/{}'.format(frame_num_curr, frame_num_total))
                else:
                    cv2.imwrite(filename_curr, frame_bgr)
                    self._debug_print('wrote frame :: {}/{}'.format(frame_num_curr, frame_num_total))

                if frame_num_curr == frame_num_total:
                    break
            else:
                print 'Error reading frame from file:', self.video_path
        print 'Read all images'

    def _already_split(self, frame_num_total):
        filename_last = self._filename(frame_num_total)
        if os.path.isfile(filename_last):
            return True
        return False

    def _filename(self, frame_num):
         return '{}_{}.{}'.format(self.base, frame_num, self.ext)

    def _write_list(self, out_dir):
        self.list_path = os.path.join(out_dir, 'list.txt')
        list_file = open(self.list_path, 'w')
        for frame_path in self.file_list:
            list_file.write('{}\n'.format(os.path.basename(frame_path)))
        list_file.close()

    def remove_files(self):
        for frame_path in self.frame_paths:
            os.remove(frame_path)
        os.remove(self.list_path)
