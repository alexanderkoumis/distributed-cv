#!/usr/bin/env python

import gzip
import os
import sys

def get_filenames(path):
    print os.path.abspath(path)
    files = []
    for dirpath, dirnames, filenames in os.walk(path):
        filenames_full = [os.path.join(dirpath, filename) for filename in filenames]
        if len(filenames_full) > 0:
            files.extend(filenames_full)
    return files

def print_files(filenames):
    for filename in filenames:
        filename_split = filename.split('.')
        if len(filename_split) > 1:
            if filename_split[1] == 'gz':
                with gzip.open(filename, 'rb') as f:
                    print f.read()

def main():
    
    path = os.path.abspath(sys.argv[1])
    if not os.path.isdir(path):
        print 'Error: {} is not a valid path'.format(sys.argv[1])
        sys.exit(1)
    filenames = get_filenames(path)
    print_files(filenames)

if __name__ == '__main__':
    main()
 
