import os
import sys
import tarfile

import distributed_cv
from distributed_cv.jobs.face_job import MRFaceTask
from distributed_cv.jobs.face_job_simulation import MRFaceTaskSimulation
from distributed_cv.utils.printing import print_debug


module_dir = os.path.dirname(distributed_cv.__file__)

class MRJobRunner(object):

    def __init__(self, file_list, list_txt, **kwargs):

        stem = os.path.basename(list_txt).split('.')[0]
        tar_full = os.path.join('input', '{}.tar.gz'.format(stem))

        self._make_archive(file_list, tar_full)
        self.verbose = os.environ['VERBOSE']

        module_files = list(self._module_files())
        module_tar = os.path.join('resources', 'distributed_cv.tar.gz')
        self._make_archive(module_files, module_tar, keep_existing=False, recursive=True)

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
            '--python-archive=resources/distributed_cv.tar.gz#distributed_cv',
            '--archive=resources/colorferet.tar.gz#colorferet',
            '--archive={}#dataset_dir'.format(tar),
            txt
        ])
        return args

    @staticmethod
    def _module_files():
        for root, dirs, files in os.walk(module_dir):
            for file in files:
                if file.endswith('.py'):
                    yield os.path.join(root, file)

    @staticmethod
    def _make_archive(file_list, file_name, keep_existing=True, recursive=False):
        print_debug('Creating archive: {}'.format(file_name))
        if os.path.isfile(file_name):
            if keep_existing:
                print '{} is already an archive'.format(file_name)
                return
            else:
                os.remove(file_name)
        with tarfile.open(file_name, 'w:gz') as tar:
            print_debug('Creating tar: {}'.format(file_name))
            if recursive:
                [tar.add(f, arcname=os.path.join('distributed_cv', os.path.relpath(f, module_dir))) for f in file_list]
            else:
                [tar.add(f, arcname=os.path.basename(f)) for f in file_list]
