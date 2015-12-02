import os
import subprocess
import sys

from distributed_cv.utils.printing import print_debug
from distributed_cv.utils.video_splitter import VideoSplitter

def process_archive(run_type, archive_file, archive_list):
    return archive_file, archive_list

def process_directory(directory):

    if os.path.isdir(directory):
        directory = os.path.normpath(directory)
        exts = ('.jpeg', '.jpg', '.png')
        dir_stem = os.path.basename(directory)
        file_list = []
        for filename in os.listdir(directory):
            if filename.endswith(exts):
                file_list.append(os.path.join(directory, filename))
        list_txt = os.path.join(directory, '{}.txt'.format(dir_stem))

        if not os.path.isfile(list_txt):
            with open(list_txt, 'w') as f:
                [f.write(filename + '\n') for filename in file_list]

        return file_list, list_txt
    else:
        sys.exit('{} is not a directory'.format(directory))

def process_youtube(youtube_url):
    """Downloads and splits a YouTube video. Returns title and file list."""

    def get_youtube_filename(youtube_url):
        """Gets the name of a youtube video."""

        filename = ''
        try:
            cmd = ['youtube-dl', '--get-filename', youtube_url]
            filename = subprocess.check_output(cmd).rstrip()
            filename = filename.replace(' ', '')
        except OSError as e:
            if e.errno == os.errno.ENOENT:
                sys.exit('Must install youtube-dl: pip install youtube-dl')
            else:
                raise
        return ''.join(c for c in filename if c.isalnum() or c == '.')

    video_file = get_youtube_filename(youtube_url)
    video_stem = video_file.split('.')[0]
    video_dir_full = os.path.join('input', 'videos', video_stem)
    video_file_full = os.path.join('input', 'videos', video_file)
    if not os.path.isdir(video_dir_full):
        os.makedirs(video_dir_full)
    if os.path.isfile(video_file_full):
        print_debug('Video {} already downloaded'.format(video_file_full))
    else:
        print_debug('Downloading video {} to {}'.format(video_stem,
                                                        video_file_full))
        subprocess.call(['youtube-dl', '-q', '-o', video_file_full,
                                                   youtube_url])
    print_debug('Splitting frames to {}'.format(video_dir_full))
    splitter = VideoSplitter(video_file_full, video_dir_full, 'jpg')
    file_list, list_txt = splitter.split()

    return file_list, list_txt
