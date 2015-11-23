import pytest

from itertools import product

import distributed_cv.cluster as cluster


test_bin = 'cluster.py'
youtube_id = 'mP2MpZhst_g'
youtube_url = 'https://www.youtube.com/watch?v={}'.format(youtube_id)

num_instances = [2, 5, 10]
hardware_types = ['cpu', 'gpu']
run_types = {
	'local': ['simulate', 'local'],
	'remote': ['emr']
}
input_types = {
	'youtube': youtube_url
	# 'directory': []
}

def flag(arg):
	return '--{}'.format(arg)

@pytest.mark.parametrize('hardware_type, run_type, input_type',
						 product(hardware_types, run_types['local'], input_types.keys()))
def test_local(hardware_type, run_type, input_type):
	cluster.opts['hardware_type'] = hardware_type
	cluster.opts['run_type'] = run_type
	cluster.opts['input_type'] = input_type
	cluster.opts['input_path'] = input_types[input_type]
	assert cluster.main(cluster.opts)

# @pytest.mark.parametrize('hardware_type, run_type, input_type, num_instances',
# 						 product(hardware_types, run_types['remote'], input_types.keys(), num_instances))
# def test_remote(hardware_type, run_type, input_type, num_instances):
# 	cluster.opts['hardware_type'] = hardware_type
# 	cluster.opts['run_type'] = run_type
# 	cluster.opts['num_instances'] = num_instances
# 	cluster.opts['input_type'] = input_type
# 	cluster.opts['input_path'] = input_types[input_type]
# 	assert cluster.main(cluster.opts)
