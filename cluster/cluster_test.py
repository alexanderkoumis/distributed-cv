#!/usr/bin/env python
import argparse
import os
import subprocess


test_bin = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cluster.py')

youtube_id = 'mP2MpZhst_g'
youtube_url = 'https://www.youtube.com/watch?v={}'.format(youtube_id)

hardware_types = ['cpu']
run_types = ['emr']
# run_types = ['simulate', 'local']
num_instances = [2, 5, 10]

parser = argparse.ArgumentParser()
parser.add_argument('--cmds', action='store_true', help='Print commands, dont run')
args = parser.parse_args()

def flag(arg):
    return '--{}'.format(arg)

def gen_args():
    arg_dict = {}
    log_format = 'youtube{}-{}-{}.txt'
    for run_type in run_types:
        for hardware_type in hardware_types:
            args = ('python', test_bin, flag('verbose'), flag('run'), run_type, flag(hardware_type), flag('youtube'), youtube_url)
            if run_type == 'emr':
                for num in num_instances:
                    log_file = log_format.format(youtube_id, 'emr{}'.format(num), hardware_type)
                    arg_dict[log_file] = list(args + (flag('num_instances'), str(num)))
            else:
                log_file = log_format.format(youtube_id, run_type, hardware_type)
                arg_dict[log_file] = list(args)
    return arg_dict

arg_dict = gen_args()

for log_file in arg_dict:
    test_args = ' '.join(arg_dict[log_file])
    test_args = '{} | tee {}'.format(test_args, os.path.join('logs', log_file))
    if args.cmds:
        print test_args
    else:
        subprocess.call(test_args, shell=True, stderr=subprocess.STDOUT)
