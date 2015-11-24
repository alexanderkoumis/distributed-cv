from distributed_cv.jobs.face_job_simulation import MRFaceTaskSimulation


class SimulationRunner(object):

    def __init__(self, file_list, list_txt, **kwargs):
        self.face_count = MRFaceTaskSimulation(file_list, **kwargs)
        
    def __call__(self):
        return self.face_count.run()
