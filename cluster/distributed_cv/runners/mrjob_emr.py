import os
import re
import sys

import boto3

from distributed_cv.config.aws import AwsConfiguration
from distributed_cv.runners.runner import MRJobRunner

class MRJobRunnerEMR(MRJobRunner):

    bucket = 'facedata'

    def __init__(self, *args, **kwargs):
        AwsConfiguration(kwargs['hardware_type'], kwargs['num_instances'])
        super(MRJobRunnerEMR, self).__init__(*args, **kwargs)

    def _get_job_args(self, tar, txt, **kwargs):
        job_args = super(MRJobRunnerEMR, self)._get_job_args(tar, txt, **kwargs)

        out_dir = kwargs['out_dir']
        out_subdir = kwargs['out_subdir']

        bucket_obj = boto3.resource('s3').Bucket(self.bucket)
        existing_out_dirs = list(bucket_obj.objects.filter(Prefix=out_dir+'/'))
        dir_num = 0

        for b in existing_out_dirs:
            matches = re.match(r'.*result_(\d+).*', b.key)
            if matches:
                num = int(matches.group(1))
                if num > dir_num:
                    dir_num = num
        dir_num += 1
        out_path = 's3://{}/{}/{}_{}'.format(self.bucket, out_dir, out_subdir, dir_num)

        job_args.extend(['-remr', '--output-dir={}'.format(out_path)])

        return job_args

