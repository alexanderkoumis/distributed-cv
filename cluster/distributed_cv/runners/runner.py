import os
import sys
import tarfile

from distributed_cv.jobs.face_job import MRFaceTask
from distributed_cv.jobs.face_job_simulation import MRFaceTaskSimulation
from distributed_cv.utils.printing import print_debug


class MRJobRunner(object):

    def __init__(self, file_list, list_txt, **kwargs):

        stem = os.path.basename(list_txt).split('.')[0]
        tar_full = os.path.join('input', '{}.tar.gz'.format(stem))

        self._make_archive(file_list, tar_full)
        self.verbose = os.environ['VERBOSE']

        self.job_args = self._get_job_args(tar_full, list_txt, **kwargs)

    def __call__(self):

        results = []
        face_count = None

        try:
            face_count = MRFaceTask(args=self.job_args)
        except IOError, excp:
            sys.exit('Exception message:', excp.message)

        face_count.set_up_logging(verbose=self.verbose, stream=sys.stdout)
        with face_count.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                results.append(face_count.parse_output_line(line))
        return results


    def _get_job_args(self, tar, txt, **kwargs):
        cascade_cpu = kwargs['cascade_cpu']
        cascade_gpu = kwargs['cascade_gpu']
        hardware_type = kwargs['hardware_type']
        args = ['--verbose'] if self.verbose else []
        args.extend([
            '--file={}'.format(os.path.join('resources', cascade_cpu)),
            '--file={}'.format(os.path.join('resources', cascade_gpu)),
            '--jobconf=job.settings.cascade_cpu={}'.format(cascade_cpu),
            '--jobconf=job.settings.cascade_gpu={}'.format(cascade_gpu),
            '--jobconf=job.settings.gpu_or_cpu={}'.format(hardware_type),
            '--jobconf=job.settings.colorferet=colorferet',
            '--archive=resources/colorferet.tar.gz#colorferet',
            '--archive={}#dataset_dir'.format(tar),
            txt
        ])
        return args

    @staticmethod
    def _make_archive(file_list, file_name):
        print_debug('Creating archive of images as {}'.format(file_name))
        if os.path.isfile(file_name):
            print '{} is already an archive'.format(file_name)
            return
        with tarfile.open(file_name, 'w:gz') as tar:
            print_debug('Creating tar: {}'.format(file_name))
            for file in file_list:
                tar.add(file, arcname=os.path.basename(file))