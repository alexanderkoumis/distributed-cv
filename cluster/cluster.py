#!/usr/bin/env python

""" Face detection cluster job runner.
"""

import argparse
import os
import sys
import boto3

from boto.s3.key import Key
from boto.emr.step import StreamingStep
from boto.emr.instance_group import InstanceGroup
from mrjob.job import MRJob


from distributed_cv.runners.mrjob_local import MRJobRunnerLocal
from distributed_cv.runners.mrjob_emr import MRJobRunnerEMR
from distributed_cv.runners.simulation import SimulationRunner
from distributed_cv.utils.printing import print_debug, print_results
from distributed_cv.utils.processing import process_archive, process_directory, process_youtube


# Default values
opts = {
    'num_instances': 2,
    'verbose': '1',
    'run_type': 'local',
    'hardware_type': 'cpu',
    'out_dir': 'output',
    'out_subdir': 'result',
    'resources_dir': 'resources',
    'colorferet': 'colorferet.tar.gz',
    'cascade_cpu': 'haarcascade_frontalface_default.xml',
    'cascade_gpu': 'haarcascade_frontalface_default_cuda.xml'
}

os.environ['VERBOSE'] = opts['verbose']

def process_args():

    global opts

    parser = argparse.ArgumentParser()

    group_gpu = parser.add_mutually_exclusive_group()
    group_gpu.add_argument('--cpu', action='store_true', default=True,
                           help='use CPU face detector')
    group_gpu.add_argument('--gpu', action='store_true', default=False,
                           help='enable GPU (Costs more with AWS)')
    
    group_dataset = parser.add_mutually_exclusive_group()
    group_dataset.add_argument('--youtube', type=str,
                               help='input YouTube video (requires youtube-dl)')
    group_dataset.add_argument('--directory', type=str,
                               help='input directory of images')
    group_dataset.add_argument('--archive', type=str,
                               help='input archive (must also supply list)')

    parser.add_argument('--list', type=str,
                        help='input list')
    parser.add_argument('--run', type=str, dest='run_type', default=opts['run_type'],
                        help='( simulate | local | emr )')
    parser.add_argument('--num_instances', type=int, default=opts['num_instances'],
                        help='number of EC2 instances')
    parser.add_argument('--verbose', action='store_true', default=bool(opts['verbose']),
                        help='print out debug information')

    args = parser.parse_args()

    os.environ['VERBOSE'] = str(args.verbose)
    os.environ['USAGE'] = parser.format_help()
    opts['num_instances'] = args.num_instances
    opts['run_type'] = args.run_type
    opts['hardware_type'] = 'gpu' if args.gpu else 'cpu'
    if args.directory:
        opts['input_type'] = 'directory'
        opts['input_path'] = args.directory
    elif args.youtube:
        opts['input_type'] = 'youtube'
        opts['input_path'] = args.youtube
    elif args.archive:
        if args.list:
            opts['input_type'] = 'archive'
            opts['archive_file'] = args.archive
            opts['archive_list'] = args.list
        else:
            print 'Must provide list file with --list'
            sys.exit(parser.format_help())
    else:
        print 'Need input ( directory | youtube | archive )'
        sys.exit(parser.format_help())

    return opts

def main(opts):
    
    runners = {
        'simulate': SimulationRunner,
        'local': MRJobRunnerLocal,
        'emr': MRJobRunnerEMR
    }

    def process_input():
        if opts['input_type'] == 'archive':
            return process_archive(opts['run_type'], opts['archive_file'], opts['archive_list'])
        elif opts['input_type'] == 'directory':
            return process_directory(opts['input_path'])
        elif opts['input_type'] == 'youtube':
            return process_youtube(opts['input_path'])
        else:
            print 'Unrecognized input_type'
            sys.exit(os.environ['USAGE'])

    if not opts['run_type'] in runners.keys():
        sys.exit('Unrecognized run_type. Choose from local, emr, or simulate')

    file_list, list_txt = process_input()
    runner = runners[opts['run_type']](file_list, list_txt, **opts)
    results = runner()
    print_results(results)

    return results

if __name__ == '__main__':
    opts = process_args()
    main(opts)
