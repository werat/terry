#!/usr/bin/env python3

import time
import logging

from datetime import datetime, timedelta

from avery.controller import Controller
from avery.worker import Worker, JobChannel  # noqa


def work_func(channel: JobChannel):
    print('========= GOT JOB =========')
    print('id: ', channel.job.id)
    print('locked_at: ', channel.job.locked_at)
    print('args: ', channel.job.args)
    print('===========================')
    channel.requeue_job(datetime.utcnow() + timedelta(seconds=3))


def setup_backend(db_uri):
    import pymongo
    client = pymongo.MongoClient(db_uri)
    client.drop_database(client.get_default_database().name)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s\t%(levelname)s:\t%(message)s', level=logging.INFO)

    db_uri = 'mongodb://localhost/avery-example'

    setup_backend(db_uri)

    controller = Controller(db_uri)

    worker = Worker('example-worker', ['example-tag'], work_func, controller)
    worker.start()

    job_id = controller.create_job_id()
    controller.create_job(job_id, 'example-tag', {'payload': 42})

    try:
        while worker.is_running:
            time.sleep(1)

    except KeyboardInterrupt:
        worker.request_stop()
        worker.join()
