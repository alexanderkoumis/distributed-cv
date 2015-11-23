import os
import sys

def print_debug(message):
    if 'VERBOSE' in os.environ:
        if os.environ['VERBOSE']:
            sys.stdout.write('[DEBUG]: {}\n'.format(message))

def print_results(results):
    max_wlen = 8
    for key, value in results:
        klen = len(key)
        if klen > max_wlen:
            max_wlen = klen
    pad = '{:' + '{}'.format(max_wlen + 1) + '}'
    fmat = '{}: {}'.format(pad, '{}')
    print fmat.format('WORD', 'COUNT')
    for key, value in results:
        print '{}'.format(fmat.format(key, value))
