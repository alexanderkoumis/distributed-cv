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
from distributed_cv.utils.processing import process_directory, process_youtube


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

    group_dataset = parser.add_mutually_exclusive_group()
    group_dataset.add_argument('--youtube', type=str,
                               help='input YouTube video (requires youtube-dl)')
    group_dataset.add_argument('--directory', type=str,
                               help='input directory of images')

    group_gpu = parser.add_mutually_exclusive_group()
    group_gpu.add_argument('--cpu', action='store_true', default=True,
                           help='use CPU face detector')
    group_gpu.add_argument('--gpu', action='store_true', default=False,
                           help='enable GPU (Costs more with AWS)')
    
    parser.add_argument('--verbose', action='store_true', default=bool(opts['verbose']),
                        help='print out debug information')
    parser.add_argument('--run', type=str, dest='run_type', default=opts['run_type'],
                        help='( simulate | local | emr )')
    parser.add_argument('--num_instances', type=int, default=opts['num_instances'],
                        help='number of EC2 instances')

    args = parser.parse_args()

    os.environ['VERBOSE'] = str(args.verbose)
    opts['num_instances'] = args.num_instances
    opts['run_type'] = args.run_type
    opts['hardware_type'] = 'gpu' if args.gpu else 'cpu'
    if args.directory:
        opts['input_type'] = 'directory'
        opts['input_path'] = args.directory
    elif args.youtube:
        opts['input_type'] = 'youtube'
        opts['input_path'] = args.youtube
    else:
        print 'Need input ( directory | youtube )'
        sys.exit(parser.format_help())

    return opts

def main(opts):
    
    runners = {
        'simulate': SimulationRunner,
        'local': MRJobRunnerLocal,
        'emr': MRJobRunnerEMR
    }

    processors = {
        'directory': process_directory,
        'youtube': process_youtube
    }

    if not opts['run_type'] in runners.keys():
        sys.exit('Unrecognized run_type. Choose from {}', runners.keys)

    if not opts['input_type'] in processors.keys():
        sys.exit('Unrecognized input_type. Choose from {}', processors.keys)        

    print_debug(opts)

    file_list, list_txt = processors[opts['input_type']](opts['input_path'])
    runner = runners[opts['run_type']](file_list, list_txt, **opts)
    results = runner()
    print_results(results)

    return results

if __name__ == '__main__':
    opts = process_args()
    main(opts)
