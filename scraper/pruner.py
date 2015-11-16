#!/usr/bin/env python

import json
import sys
from os.path import isfile, join
from StringIO import StringIO
from tld import get_tld
from tld.utils import update_tld_names


scraped_dir = ''
json_path = join(scraped_dir, '')


def load_json(path):
    with open(path, 'r') as f:
        return json.load(f)

def prune_paths(image_list):
    image_list_ret = []    
    for image_info in image_list:
        del image_info['image_urls']
        if len(image_info['images']) > 0:
            image_list_ret.extend(image_info['images'])
    return image_list_ret

def prune_nonexisting(image_list):
    image_obj = {}
    for image_info in image_list:
        image_path = join(scraped_dir, image_info['path'])
        if isfile(image_path):
            image_obj[image_path] = image_info['url']
    return image_obj

def sort_domains(image_obj):
    update_tld_names()
    image_obj_ret = {}
    for path in image_obj:
        url = image_obj[path]
        try:
            tld = get_tld(url)
            if tld not in image_obj_ret:
                image_obj_ret[tld] = {}
            image_obj_ret[tld][path] = url
        except:
            pass
    return image_obj_ret

def write_json(image_obj):
    pruned_path = '{}_pruned.json'.format(json_path.split('.')[0])
    with open(pruned_path, 'w') as outfile:
        outfile.write(json.dumps(image_obj, sort_keys = True, indent = 4, ensure_ascii=False))

def main():
    images_list = load_json(json_path)
    images_list = prune_paths(images_list)    
    image_obj = prune_nonexisting(images_list)
    image_obj = sort_domains(image_obj)
    write_json(image_obj)

if __name__ == '__main__':
    main()
