from terry.api import Job


def test_create_job(controller):
    job_id = controller.create_job_id()
    controller.create_job(job_id, 'test-tag', {'payload': 42})
    job = controller.get_job(job_id)

    assert job.id == job_id
    assert job.status == Job.IDLE
    assert job.args == {'payload': 42}
    assert job.worker_id is None


def test_acquire_job(controller):
    job_id = controller.create_job_id()
    controller.create_job(job_id, 'test-tag')

    job_1 = controller.acquire_job(['test-tag'], 'test-worker')
    assert job_1.id == job_id
    assert job_1.worker_id == 'test-worker'

    job_2 = controller.acquire_job(['test-tag'], 'test-worker')
    assert job_2 is None


def test_reacquire_job(controller):
    job_id = controller.create_job_id()
    controller.create_job(job_id, 'test-tag')

    job_1 = controller.acquire_job(['test-tag'], 'test-worker')
    assert job_1.id == job_id

    from datetime import datetime, timedelta
    fake_heartbeat = datetime.utcnow() - controller.HEARTBEAT_TIMEOUT - timedelta(minutes=1)
    controller._update_job(job_1.id, job_1.version, worker_heartbeat=fake_heartbeat)

    job_2 = controller.acquire_job(['test-tag'], 'test-worker')
    assert job_2.id == job_id
