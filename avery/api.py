class AveryJob:
    IDLE = 'idle'
    LOCKED = 'locked'
    CANCELLED = 'cancelled'
    COMPLETED = 'completed'

    def __init__(self, id_, tags, args, version, *,
                 status=None,
                 worker_id=None,
                 worker_heartbeat=None,
                 worker_exception=None):
        self.id = id_
        self.tags = tags
        self.args = args
        self.version = version
        self.status = status or AveryJob.IDLE
        self.worker_id = worker_id
        self.worker_heartbeat = worker_heartbeat
        self.worker_exception = worker_exception


class IAveryJobController:
    def get_job(self, job_id):
        pass

    def create_job(self, job_id, tags, args=None):
        pass

    def cancel_job(self, job_id, version):
        pass

    def delete_job(self, job_id, version):
        pass


class IAveryWorkerController:
    def acquire_job(self, tags, worker_id):
        pass

    def heartbeat_job(self, job_id, version):
        pass

    def finalize_job(self, job_id, version, status, worker_exception=None):
        pass
