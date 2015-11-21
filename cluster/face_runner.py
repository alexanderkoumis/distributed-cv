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
from cluster_iface.video_processor import SplitProcessor
from jobs.face_job import MRFaceTask


verbose = False

def print_debug(message):
    if verbose:
        print '[DEBUG]', message

def make_archive(files, out):
    if os.path.isfile(out):
        print '{} is already an archive'.format(out)
        return
    with tarfile.open(out, 'w:gz') as tar:
        print_debug('Creating tar: {}'.format(out))
        for file in files:
            print file
            tar.add(file, arcname=os.path.basename(file))

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

def main():

    run_type = 'local' # Currently supports local and emr
    max_wlen = 8
    out_path = None
    results = []

    out_dir = 'output'
    out_subdir = 'result'

    video_tar_full = None
    video_txt_full = None
    cascade_cpu = 'haarcascade_frontalface_default.xml'
    cascade_gpu = 'haarcascade_frontalface_default_cuda.xml'
    cascade_cpu_full = os.path.join('resources', cascade_cpu)
    cascade_gpu_full = os.path.join('resources', cascade_gpu)

    parser = argparse.ArgumentParser()
    parser.add_argument('--run', help='Run type (same as MRJob) (defaults to local)')
    parser.add_argument('--youtube', help='Process YouTube video (requires youtube-dl)')
    parser.add_argument('--gpu', help='Enable GPU on AWS (Warning: will cost more)', action='store_true', dest='gpu')
    args = parser.parse_args()

    gpu_or_cpu = 'gpu' if args.gpu else 'cpu'

    config = AwsConfiguration(gpu_or_cpu)

    if args.run:
        run_type = args.run

    if args.youtube:
        video_file = get_youtube_filename(args.youtube)
        video_stem = video_file.split('.')[0]
        video_tar = '{}.tar.gz'.format(video_stem)
        video_dir_full = os.path.join('input', 'videos', video_stem)
        video_file_full = os.path.join('input', 'videos', video_file)
        video_tar_full = os.path.join('input', 'videos', video_tar)
        video_txt_full = os.path.join(video_dir_full, 'list.txt')

        if not os.path.isdir(video_dir_full):
            os.makedirs(video_dir_full)

        print_debug('Downloading video {} to {}'.format(video_stem, video_file_full))
        subprocess.call(['youtube-dl', '-q', '-o', video_file_full, args.youtube])

        print_debug('Splitting frames to {}'.format(video_dir_full))
        splitter = SplitProcessor(video_file_full, video_dir_full, 'jpg')
        splitter.run()

        print_debug('Creating archive of frames as {}'.format(video_tar_full))
        print_debug('file_list: {}'.format(splitter.file_list))
        make_archive(splitter.file_list, video_tar_full)

    else:
        parser.print_help()
        sys.exit(2)

    if not video_tar_full or not video_txt_full:
        sys.exit('Something went wrong. video_tar_full is None')

    try:
        arguments = [
            '--file={}'.format(cascade_cpu_full),
            '--file={}'.format(cascade_gpu_full),
            '--jobconf=job.settings.video_dir=video_dir',
            '--jobconf=job.settings.cascade_cpu={}'.format(cascade_cpu),
            '--jobconf=job.settings.cascade_gpu={}'.format(cascade_gpu),
            '--jobconf=job.settings.gpu_or_cpu={}'.format(gpu_or_cpu),
            '--jobconf=job.settings.colorferet=colorferet',
            '--archive=resources/colorferet.tar.gz#colorferet',
            '--archive={}#video_dir'.format(video_tar_full),
            video_txt_full
        ]
        if verbose:
            arguments.extend(['--verbose'])
        if run_type == 'local':
            dir_num = 0
	    print 'os.path.isdir(', out_dir, '):', os.path.isdir(out_dir)
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
            arguments.extend([
                '-rinline',
                '--output-dir={}'.format(out_path)
            ])
        elif run_type == 'emr':
            bucket_dir = 'facedata'
            bucket_obj = boto3.resource('s3').Bucket(bucket_dir)
            existing_out_dirs = list(bucket_obj.objects.filter(Prefix=out_dir+'/'))
            dir_num = 0
            if len(existing_out_dirs) > 0:
                dir_num = int(existing_out_dirs[-1].key.split('/')[1].split('_')[1]) + 1
        
            out_path = 's3://{}/{}/{}_{}'.format(bucket_dir, out_dir, out_subdir, dir_num)
            arguments.extend([
                '-remr',
                '--output-dir={}'.format(out_path)
            ])
        else:
            sys.exit('Unrecognized run type: {}'.format(run_type))
        word_count = MRFaceTask(args=arguments)
        word_count.set_up_logging(verbose=True, stream=sys.stdout)
        with word_count.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                key, value = word_count.parse_output_line(line)
                klen = len(key)
                if klen > max_wlen:
                    max_wlen = klen
                results.append((key, value))
    except IOError, excp:
        print 'Exception message:', excp.message
        raise

    pad = '{:' + '{}'.format(max_wlen) + '}'
    fmat = '{}:\t{}'.format(pad, '{}')

    print 'Output In: {}'.format(out_path)
    print fmat.format('--WORD--', '--COUNT--')
    for key, value in results:
        print '  {}'.format(fmat.format(key, value))



if __name__ == '__main__':
    main()
