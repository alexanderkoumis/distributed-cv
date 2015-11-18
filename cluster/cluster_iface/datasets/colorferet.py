import os
import re
import sys

import cv2
import numpy


class ColorFeret(object):

    bounds_ratio = 2

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

    face_labels_num = {
        0: 'Black-or-African-American',
        1: 'Asian',
        2: 'Asian-Middle-Eastern',
        3: 'Hispanic',
        4: 'Native-American',
        5: 'Other',
        6: 'Pacific-Islander',
        7: 'White'
    }

    @classmethod
    def load_from_full_dataset(ColorFeret, colorferet_full_dir):

        images = []
        labels = []
        label = 0

        image_dir_root = os.path.join(colorferet_full_dir, 'dvd1', 'data', 'images')
        txt_root = os.path.join(colorferet_full_dir, 'dvd1', 'data', 'ground_truths', 'name_value')

        for image_dir, image_subdirs, files in os.walk(image_dir_root):

            if len(files) == 0:
                print 'Directory', image_dir, ' has no images, skipping'
                continue

            if len(image_subdirs):
                print 'Skipping root directory'
                continue

            # Only processing ppms
            if files[0].split('.')[-1] != 'ppm':
                continue

            image_dir_root = os.path.join(image_dir, files[0])

            if not os.path.isfile(image_dir_root):
                print 'Error:', image_dir_root, 'is not an image, skipping'
                continue

            stem_coords = files[0].split('.')[0]
            stem = os.path.basename(image_dir)
            txt_dir = os.path.join(txt_root, stem)

            txt_file_coords = os.path.join(txt_dir, stem_coords + '.txt')
            txt_file_race = os.path.join(txt_dir, stem + '.txt')

            # Open file
            face_traits = {}
            file_coords = open(txt_file_coords, 'r')
            file_race = open(txt_file_race, 'r')

            matches_coords = re.findall(r'left_eye_coordinates=(\d+) (\d+)\nright_eye_coordinates=(\d+) (\d+)\nnose_coordinates=\d+ \d+\nmouth_coordinates=(\d+) (\d+)', file_coords.read())
            matches_race = re.findall(r'race=(.*)', file_race.read())

            if len(matches_coords) and len(matches_race):

                image_orig = cv2.imread(image_dir_root, 0)

                if image_orig is not None:

                    eye_left_x = int(matches_coords[0][0])
                    eye_left_y = int(matches_coords[0][1])
                    eye_right_x = int(matches_coords[0][2])
                    eye_right_y = int(matches_coords[0][3])
                    mouth_x = int(matches_coords[0][4])
                    mouth_y = int(matches_coords[0][5])
                    race = matches_race[0]

                    w_orig = eye_left_x - eye_right_x
                    h_orig = mouth_y - eye_right_y
                    w_scaled = int(w_orig * ColorFeret.bounds_ratio)
                    h_scaled = int(h_orig * ColorFeret.bounds_ratio)
                    tl_x = eye_right_x - (w_scaled - w_orig) / 2
                    tl_y = eye_right_y - (h_scaled - h_orig) / 2

                    image_tmp = image_orig[tl_y:tl_y+h_scaled, tl_x:tl_x+w_scaled]
                    image = cv2.resize(image_tmp, (256, 256))

                    images.append(image)
                    labels.append(ColorFeret.face_labels_str[race])

            file_coords.close()
            file_race.close()

        return images, labels

    @classmethod
    def load_from_small_dataset(ColorFeret, colorferet_small_dir):

        images = []
        labels = []

        for face_label_str in ColorFeret.face_labels_str:
            face_label_num = ColorFeret.face_labels_str[face_label_str]
            image_dir = os.path.join(colorferet_small_dir, face_label_str)
            image_files = [os.path.join(image_dir, image_file) for image_file in os.listdir(image_dir)]
            images_tmp = [cv2.resize(cv2.imread(image_file, 0), (256, 256)) for image_file in image_files if image_file.split('.')[-1] == 'png']
            labels_tmp = [face_label_num] * len(images_tmp)
            images.extend(images_tmp)
            labels.extend(labels_tmp)        

        return images, labels


if __name__ == "__main__":
    sys.exit()