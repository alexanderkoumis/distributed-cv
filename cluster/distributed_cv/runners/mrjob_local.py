import os

from distributed_cv.runners.runner import MRJobRunner


class MRJobRunnerLocal(MRJobRunner):

    def __init__(self, *args, **kwargs):
        super(MRJobRunnerLocal, self).__init__(*args, **kwargs)

    def _get_job_args(self, tar, txt, **kwargs):
        job_args = super(MRJobRunnerLocal, self)._get_job_args(tar, txt, **kwargs)
        dir_num = 0
        out_dir = kwargs['out_dir']
        out_subdir = kwargs['out_subdir']
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
