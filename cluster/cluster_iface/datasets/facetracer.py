# http://www.cs.columbia.edu/CAVE/databases/facetracer/

# Download dataset at:
# http://www.cs.columbia.edu/CAVE/databases/facetracer/files/facetracer.zip

# Usage
# ./facetracer.py <unzipped facetracer path>

import os
import re
import sys
from cStringIO import StringIO
from httplib import BadStatusLine
from math import cos, sin, pow, sqrt
from urllib2 import urlopen, HTTPError, URLError
from socket import error as SocketError

import cv2
import numpy


facetracer_dir = sys.argv[1]

faceindex_file = os.path.join(facetracer_dir, 'faceindex.txt')
facelabels_file = os.path.join(facetracer_dir, 'facelabels.txt')
facestats_file = os.path.join(facetracer_dir, 'facestats.txt')
imgs_dir = os.path.join(facetracer_dir, 'images')

face_objs = {}

with open(facelabels_file, 'r') as f:
    for line in f.read().split('\n'):
        split = line.split('\t')
        if len(split) == 3:
            id = split[0]
            attribute = split[1]
            label = split[2]
            if attribute == 'race':
                face_objs[id] = { 'race': label }

with open(faceindex_file, 'r') as f:
     for line in f.read().split('\n'):
        split = line.split('\t')
        if len(split) == 3:
            id = split[0]
            if id in face_objs:
                face_objs[id]['url'] = split[1]

with open(facestats_file, 'r') as f:
    cnt = 0
    for line in f.read().split('\n'):
        # Skip first two lines
        if cnt > 1:
            split = line.split('\t')
            if len(split) == 20:
                id = split[0]
                # Only proceed with imgs for which a URL exists
                if id in face_objs and face_objs[id]['url']:
                    face_objs[id]['dims'] = {
                        'crop_w': int(split[1]),
                        'crop_h': int(split[2]),
                        'crop_x0': int(split[3]),
                        'crop_y0': int(split[4]),
                        'yaw': int(split[5]),
                        'pitch': int(split[6]),
                        'roll': int(split[7]),
                        'left_eye_left_x': int(split[8]),
                        'left_eye_left_y': int(split[9]),
                        'left_eye_right_x': int(split[10]),
                        'left_eye_right_y': int(split[11]),
                        'right_eye_left_x': int(split[12]),
                        'right_eye_left_y': int(split[13]),
                        'right_eye_right_x': int(split[14]),
                        'right_eye_right_y': int(split[15]),
                        'mouth_left_x': int(split[16]),
                        'mouth_left_y': int(split[17]),
                        'mouth_right_x': int(split[18]),
                        'mouth_right_y': int(split[19])
                    }
        cnt += 1

bounds_ratio = 2
img_size = { 'w': 256, 'h': 256 }
w_scaled = img_size['w'] * bounds_ratio
h_scaled = img_size['h'] * bounds_ratio
pics_read = 0
pics_error = 0

if not os.path.isdir(imgs_dir):
    os.makedirs(imgs_dir)

for id in face_objs:
    face_obj = face_objs[id]
    if 'dims' in face_obj:
        buffer = None
        try:
            buffer = bytearray(urlopen(face_obj['url']).read())
        except (BadStatusLine, HTTPError, SocketError, URLError) as e:
            pics_error += 1
            print '{} / {} :: skipped ({})'.format(pics_read + pics_error, len(face_objs), e)
        else:
            array = numpy.asarray(buffer, dtype=numpy.uint8)
            img_orig = cv2.imdecode(array, -1)

            if img_orig is None:
                continue

            h_img = img_orig.shape[0]
            w_img = img_orig.shape[1]

            d = face_obj['dims']
            eye_left_x = d['crop_x0'] + (d['left_eye_right_x'] + d['left_eye_left_x']) / 2
            eye_left_y = d['crop_y0'] + (d['left_eye_right_y'] + d['left_eye_left_y']) / 2
            eye_right_x = d['crop_x0'] + (d['right_eye_right_x'] + d['right_eye_left_x']) / 2
            eye_right_y = d['crop_y0'] + (d['right_eye_right_y'] + d['right_eye_left_y']) / 2
            mouth_x = d['crop_x0'] + (d['mouth_right_x'] + d['mouth_left_x']) / 2
            mouth_y = d['crop_y0'] + (d['mouth_right_y'] + d['mouth_left_y']) / 2

            forehead_x = (eye_left_x + eye_right_x) / 2
            forehead_y = (eye_left_y + eye_right_y) / 2

            w_face = forehead_x - mouth_x
            h_face = forehead_y - mouth_y

            bl_x = eye_left_x - w_face
            bl_y = eye_left_y - h_face

            br_x = eye_right_x - w_face
            br_y = eye_right_y - h_face

            pts1 = numpy.float32([
                [ eye_left_x, eye_left_y ],
                [ eye_right_x, eye_right_y ],
                [ bl_x, bl_y ],
                [ br_x, br_y ]
            ])
            
            pts2 = numpy.float32([
                [ 0            , 0             ],
                [ img_size['w'], 0             ],
                [ 0            , img_size['h'] ],
                [ img_size['w'], img_size['h'] ]
            ])

            pts3 = numpy.float32([
                [ 0             - img_size['w'] / 2, 0             - img_size['h'] / 2, 1 ],
                [ img_size['w'] + img_size['w'] / 2, 0             - img_size['h'] / 2, 1 ],
                [ 0             - img_size['w'] / 2, img_size['h'] + img_size['h'] / 2, 1 ],
                [ img_size['w'] + img_size['w'] / 2, img_size['h'] + img_size['h'] / 2, 1 ]
            ])

            M = cv2.getPerspectiveTransform(pts1, pts2)

            pts4 = numpy.float32([numpy.linalg.inv(M).dot(pts3[i]) for i in range(0, 4)])

            pts5 = numpy.float32([
                [ pts4[0][0], pts4[0][1] ],
                [ pts4[1][0], pts4[1][1] ],
                [ pts4[2][0], pts4[2][1] ],
                [ pts4[3][0], pts4[3][1] ]
            ])

            # tl = (pts1[0][0], pts1[0][1])
            # tr = (pts1[1][0], pts1[1][1])
            # bl = (pts1[2][0], pts1[2][1])
            # br = (pts1[3][0], pts1[3][1])
            # cv2.line(img_orig, tl, tr, (0, 255, 0))
            # cv2.line(img_orig, tl, bl, (0, 255, 0))
            # cv2.line(img_orig, tr, br, (0, 255, 0))
            # cv2.line(img_orig, bl, br, (0, 255, 0))
            # tl = (pts5[0][0], pts5[0][1])
            # tr = (pts5[1][0], pts5[1][1])
            # bl = (pts5[2][0], pts5[2][1])
            # br = (pts5[3][0], pts5[3][1])
            # cv2.line(img_orig, tl, tr, (0, 255, 0))
            # cv2.line(img_orig, tl, bl, (0, 255, 0))
            # cv2.line(img_orig, tr, br, (0, 255, 0))
            # cv2.line(img_orig, bl, br, (0, 255, 0))
            # cv2.imshow('face', img_orig)
            # cv2.waitKey(33)

            M = cv2.getPerspectiveTransform(pts5, pts2)
            img_resized = cv2.warpPerspective(img_orig, M, (img_size['w'], img_size['h']))

            filename = '{}_{}.jpg'.format(pics_read, face_obj['race'])
            filename_full = os.path.join(imgs_dir, filename)
            pics_read += 1

            cv2.imwrite(filename_full, img_resized)

            print '{} / {} :: read'.format(pics_read + pics_error, len(face_objs))
