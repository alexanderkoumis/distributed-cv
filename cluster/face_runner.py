#!/usr/bin/python

""" Face detection cluster job runner.
"""

import argparse
import os
import sys
import subprocess
import tarfile
import boto3

from boto.s3.key import Key
from boto.emr.step import StreamingStep
from boto.emr.instance_group import InstanceGroup
from mrjob.job import MRJob

from cluster_iface.configuration import AwsConfiguration
from cluster_iface.video_splitter import VideoSplitter
from jobs.face_job import MRFaceTask


# Default values
verbose = True
num_instances = 2
gpu_or_cpu = 'cpu'
out_dir = 'output'
out_subdir = 'result'
cascade_cpu = 'haarcascade_frontalface_default.xml'
cascade_gpu = 'haarcascade_frontalface_default_cuda.xml'


def print_debug(message):
    if verbose:
        print '[DEBUG]: {}'.format(message)


def get_youtube_filename(youtube_url):
    filename = ''
    try:
        filename = subprocess.check_output(['youtube-dl', '--get-filename', youtube_url]).rstrip().replace(' ', '')
    except OSError as e:
        if e.errno == os.errno.ENOENT:
            sys.exit('youtube-dl not found, install with: pip install youtube-dl')
        else:
            raise
    return ''.join(c for c in filename if c.isalnum() or c == '.')


def make_archive(file_list, file_name):
    print_debug('Creating archive of images as {}'.format(file_name))
    print_debug('file_list: {}'.format(file_list))
    if os.path.isfile(file_name):
        print '{} is already an archive'.format(file_name)
        return
    with tarfile.open(file_name, 'w:gz') as tar:
        print_debug('Creating tar: {}'.format(file_name))
        for file in file_list:
            print_debug('Archiving: {}'.format(file))
            tar.add(file, arcname=os.path.basename(file))


def run_simulation(gpu_or_cpu, file_list, verbose):
    print 'haha'


def process_dataset(directory, youtube, usage):

    def process_video(youtube_url):
        """Downloads and splits a YouTube video. Returns title and file list."""
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

        return file_list, video_stem, list_txt

    def process_directory(directory):

        if os.path.isdir(directory):
            directory = os.path.normpath(directory)
            exts = ('.jpeg', '.jpg', '.png')
            file_list = []
            for filename in os.listdir(directory):
                if filename.endswith(exts):
                    file_list.append(filename)
            dir_stem = os.path.basename(directory)
            list_txt = os.path.join(directory, '{}.txt'.format(dir_stem))

            if not os.path.isfile(list_txt):
                with open(list_txt, 'w') as f:
                    [f.write(filename + '\n') for filename in file_list]

            return file_list, dir_stem, list_txt
        else:
            sys.exit('{} is not a directory'.format(directory))

    if directory:
        return process_directory(directory)
    elif youtube:
        return process_video(youtube)
    else:
        print 'No YouTube URL or directory supplied.'
        sys.exit(usage)


class MRJobRunner(object):

    def __init__(self, verbose, file_list, list_txt, tar_full):

        self.verbose = verbose
        self.job_args = self._get_job_args(tar_full, list_txt, verbose)

    def __call__(self):

        results = []
        word_count = None

        try:
            word_count = MRFaceTask(args=self.job_args)
        except IOError, excp:
            sys.exit('Exception message:', excp.message)

        word_count.set_up_logging(verbose=verbose, stream=sys.stdout)
        with word_count.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                results.append(word_count.parse_output_line(line))
        return results


    def _get_job_args(self, tar_full, list_txt, verbose):
        args = ['--verbose'] if verbose else []
        args.extend([
            '--file={}'.format(os.path.join('resources', cascade_cpu)),
            '--file={}'.format(os.path.join('resources', cascade_gpu)),
            '--jobconf=job.settings.cascade_cpu={}'.format(cascade_cpu),
            '--jobconf=job.settings.cascade_gpu={}'.format(cascade_gpu),
            '--jobconf=job.settings.gpu_or_cpu={}'.format(gpu_or_cpu),
            '--jobconf=job.settings.colorferet=colorferet',
            '--archive=resources/colorferet.tar.gz#colorferet',
            '--archive={}#dataset_dir'.format(tar_full),
            list_txt
        ])
        return args


class MRJobRunnerLocal(MRJobRunner):

    def __init__(self, *args):
        super(MRJobRunnerLocal, self).__init__(*args)

    def _get_job_args(self, *args):
        job_args = super(MRJobRunnerLocal, self)._get_job_args(*args)

        dir_num = 0
        if not os.path.isdir(out_dir):
            os.makedirs(out_dir)
        else:
            subdirs = next(os.walk(out_dir))[1]
            if len(subdirs) > 0:
                nums = [int(i.split('_')[1]) for i in subdirs]
                nums.sort()
                dir_num = nums[-1] + 1
        out_path = os.path.join(out_dir, '{}_{}'.format(out_subdir, dir_num))
        if not os.path.isdir(out_path):
            os.makedirs(out_path)

        job_args.extend(['-rinline', '--output-dir={}'.format(out_path)])

        return job_args

class MRJobRunnerEMR(MRJobRunner):

    bucket = 'facedata'

    def __init__(self, *args):
        super(MRJobRunnerEMR, self).__init__(*args)

    def _get_job_args(self, *args):
        job_args = super(MRJobRunnerEMR, self)._get_job_args(*args)

        bucket_obj = boto3.resource('s3').Bucket(self.bucket)
        existing_out_dirs = list(bucket_obj.objects.filter(Prefix=out_dir+'/'))
        dir_num = 0
        if len(existing_out_dirs) > 0:
            dir_num = int(existing_out_dirs[-1].key.split('/')[1].split('_')[1]) + 1
        out_path = 's3://{}/{}/{}_{}'.format(self.bucket, out_dir, out_subdir, dir_num)

        job_args.extend(['-remr', '--output-dir={}'.format(out_path)])

        return job_args


def main():

    global verbose
    global num_instances
    global gpu_or_cpu

    max_wlen = 8
    out_path = None
    results = []

    video_tar_full = None
    video_txt_full = None

    parser = argparse.ArgumentParser()
    group_run = parser.add_mutually_exclusive_group()
    group_run.add_argument('--local', action='store_true', help='Run on local mrjob cluster')
    group_run.add_argument('--emr', action='store_true', help='Run on Amazon EMR cluster')
    group_run.add_argument('--simulate', action='store_true', help='Simulate run (no cluster)')

    group_dataset = parser.add_mutually_exclusive_group()
    group_dataset.add_argument('--youtube', type=str, help='Process YouTube video (requires youtube-dl)')
    group_dataset.add_argument('--directory', type=str, help='Process directory of images')

    parser.add_argument('--gpu', help='Enable GPU on AWS (Warning: will cost more)', action='store_true')
    parser.add_argument('--verbose', action='store_true', help='Print out debug information')
    parser.add_argument('--num_instances', type=int, help='Number of EC2 instances (defaults to {})'.format(num_instances))

    args = parser.parse_args()

    verbose = args.verbose if args.verbose else verbose
    gpu_or_cpu = 'gpu' if args.gpu else gpu_or_cpu
    num_instances = args.num_instances if args.num_instances else num_instances

    file_list, stem, list_txt = process_dataset(args.directory,
                                                args.youtube,
                                                parser.format_usage())

    if args.simulate:

        results = run_simulation(gpu_or_cpu, file_list, verbose)

    else:

        tar = os.path.join('input', '{}.tar.gz'.format(stem))
        make_archive(file_list, tar)

        if args.local:
            runner = MRJobRunnerLocal(verbose, file_list, list_txt, tar)
        elif args.emr:
            runner = MRJobRunnerEMR(verbose, file_list, list_txt, tar,
                                    num_instances)
        else:
            print 'Missing ( --local | --emr | --simulate )'
            sys.exit(args.format_help())

        results = runner()


    for result in results:
        key, value = result
        klen = len(key)
        if klen > max_wlen:
            max_wlen = klen

    pad = '{:' + '{}'.format(max_wlen) + '}'
    fmat = '{}:\t{}'.format(pad, '{}')
    print fmat.format('--WORD--', '--COUNT--')
    for key, value in results:
        print '  {}'.format(fmat.format(key, value))






if __name__ == '__main__':
    main()
