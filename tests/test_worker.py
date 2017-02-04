from threading import Event

import pytest

from terry.api import Job


@pytest.mark.timeout(10)
def test_worker_job_success(controller, worker):
    job_started = Event()

    def work_func(channel):
        job_started.set()

    worker._worker_func = work_func

    job_id = controller.create_job_id()
    controller.create_job(job_id, 'test-tag')

    job_started.wait()  # wait until worker start the job
    worker.stop()

    job = controller.get_job(job_id)

    assert job.status == Job.COMPLETED
    assert job.worker_exception is None
    assert job.worker_id == worker.id


@pytest.mark.timeout(10)
def test_worker_job_exception(controller, worker):
    job_started = Event()

    def work_func(channel):
        job_started.set()
        raise Exception('exception from job')

    worker._worker_func = work_func

    job_id = controller.create_job_id()
    controller.create_job(job_id, 'test-tag')

    job_started.wait()  # wait until worker start the job
    worker.stop()

    job = controller.get_job(job_id)

    assert job.status == Job.COMPLETED
    assert job.worker_exception['reason'] == 'exception from job'
    assert job.worker_id == worker.id
