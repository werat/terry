import ctypes
import logging
import math
import random
import sys
import threading
import time
import traceback

from .api import Job, ConcurrencyError, RetriableError


__all__ = ['InterruptTask', 'Worker', 'JobChannel']


class InterruptTask(Exception):
    pass


class _RequeueRequested(Exception):
    pass


class JobContext:
    def __init__(self, worker_id, job):
        self.worker_id = worker_id
        self.job = job
        self.outdated = False
        self.requeue_requested = False
        self.requeue_for = None

    def update(self, job):
        self.job = job
        self.outdated = False

    @property
    def cancelled(self):
        return self.job.status == Job.CANCELLED

    @property
    def revoked(self):
        return self.job.worker_id != self.worker_id

    def requeue_job(self, run_at):
        self.requeue_requested = True
        self.requeue_for = run_at


class JobChannel:
    def __init__(self, ctx):
        self.__ctx = ctx

    @property
    def job(self):
        return self.__ctx.job

    @property
    def cancelled(self):
        return self.__ctx.cancelled

    @property
    def revoked(self):
        return self.__ctx.revoked

    def interrupt_if_requested(self):
        if self.cancelled or self.revoked:
            raise InterruptTask

    def requeue_job(self, run_at):
        self.__ctx.requeue_job(run_at)
        raise _RequeueRequested


class WorkerThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(WorkerThread, self).__init__(*args, **kwargs)
        self.interrupt_requested = False
        self.interrupted = False
        self.exc_info = None

    @property
    def has_failed(self):
        return self.exc_info is not None

    def run(self):
        try:
            super(WorkerThread, self).run()
        except InterruptTask:
            self.interrupted = True
        except _RequeueRequested:
            pass
        except:
            self.exc_info = sys.exc_info()


class Worker:
    def __init__(self, id_, job_tags, worker_func, controller, interrupt_via_exception=False):
        self._id = id_
        self._job_tags = job_tags
        self._worker_func = worker_func
        self._controller = controller
        self._interrupt_via_exception = interrupt_via_exception

        # current job and worker thread
        self._job_ctx = None
        self._worker_thread = None

        self._stop = threading.Event()
        self._force_stop = threading.Event()

        # TODO: handle exception in _main_loop
        self._main_loop_thread = threading.Thread(target=self._loop)

        self.logger = logging.getLogger(__name__)

    @property
    def id(self):
        return self._id

    @property
    def is_running(self):
        return self._main_loop_thread.is_alive()

    def start(self):
        self.logger.info('[%s] Starting...', self._id)
        self._main_loop_thread.start()

    def request_stop(self):
        self.logger.info('[%s] Got request to stop...', self._id)
        self._stop.set()

    def join(self):
        self._main_loop_thread.join()

    def stop(self):
        self.request_stop()
        self.join()

    def _loop(self):
        #
        retry_delay = 0
        #
        while True:
            if self._stop.is_set() and self._job_ctx is None:
                break

            if self._force_stop.is_set():
                break

            if retry_delay:
                # aka exponential backoff for retriable errors
                time.sleep(retry_delay)

            try:
                if self._job_ctx is None:
                    self._try_acquire_job()

                elif self._job_ctx.outdated:
                    self._try_update_current_task()

                elif self._job_ctx.cancelled or self._job_ctx.revoked:
                    self._wait_for_worker_thread_and_cleanup()

                elif self._worker_thread.is_alive():
                    self._try_heartbeat_current_task()

                elif self._job_ctx.requeue_requested:
                    self._try_requeue_current_task()

                else:  # worker has finished processing the task
                    self._try_finalize_current_task()

            except RetriableError:
                retry_delay = 1 if retry_delay == 0 else min(10, retry_delay * 2)
            else:
                retry_delay = 0

    def _try_acquire_job(self):
        try:
            job = self._controller.acquire_job(self._job_tags, self._id)
        except ConcurrencyError:
            job = None

        if job:
            self._job_ctx = JobContext(self.id, job)
            self.logger.info('[%s] Acquired job %s', self._id, self._job_ctx.job.id)
            self._worker_thread = WorkerThread(target=self._worker_func, args=(self._job_ctx,))
            self._worker_thread.start()
        else:
            time.sleep(math.e - random.random())

    def _try_update_current_task(self):
        job = self._controller.get_job(self._job_ctx.job.id)

        self._job_ctx.update(job)

        if self._job_ctx.cancelled:
            self.logger.info('[%s] It seems job %s was cancelled', self._id, self._job_ctx.job.id)

        if self._job_ctx.revoked:
            self.logger.info('[%s] It seems job %s was taken from us', self._id, self._job_ctx.job.id)

    def _wait_for_worker_thread_and_cleanup(self):
        if self._worker_thread.is_alive():
            if self._interrupt_via_exception and not self._worker_thread.interrupt_requested:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(self._worker_thread.ident),
                                                           ctypes.py_object(InterruptTask))
                self._worker_thread.interrupt_requested = True
                self.logger.info('[%s] Worker thread with job %s was interrupted via exception',
                                 self._id, self._job_ctx.job.id)

            self._worker_thread.join(math.pi - random.random())
        else:
            self.logger.info('[%s] Processing of job %s was terminated', self._id, self._job_ctx.job.id)
            self._job_ctx = None
            self._worker_thread = None

    def _try_heartbeat_current_task(self):
        try:
            job = self._controller.heartbeat_job(self._job_ctx.job.id, self._job_ctx.job.version)
        except ConcurrencyError:
            job = None

        if job:
            self._job_ctx.update(job)
            self._worker_thread.join(math.pi - random.random())
        else:
            self._job_ctx.outdated = True
            self.logger.info('[%s] Failed to heartbeat job %s due to version mismatch',
                             self._id, self._job_ctx.job.id)

    def _try_requeue_current_task(self):
        assert not self._worker_thread.is_alive()
        # requeue current job with new run_at time
        try:
            self._controller.requeue_job(self._job_ctx.job.id, self._job_ctx.job.version,
                                         run_at=self._job_ctx.requeue_for)
        except ConcurrencyError:
            self._job_ctx.outdated = True
            self.logger.info('[%s] Failed to mark job %s as completed due to version mismatch',
                             self._id, self._job_ctx.job.id)
        else:
            self._job_ctx = None
            self._worker_thread = None

    def _try_finalize_current_task(self):
        assert not self._worker_thread.is_alive()
        # worker thread has finished, we should mark job as COMPLETED
        if self._worker_thread.has_failed:
            reason = str(self._worker_thread.exc_info[1])
            tback = ''.join(traceback.format_exception(*self._worker_thread.exc_info))
            worker_exception = {'reason': reason, 'traceback': tback}
        else:
            worker_exception = None

        try:
            job = self._controller.finalize_job(self._job_ctx.job.id, self._job_ctx.job.version,
                                                worker_exception=worker_exception)
        except ConcurrencyError:
            job = None

        if job:
            self.logger.info('[%s] Job %s has been processed', self._id, self._job_ctx.job.id)
            self._job_ctx = None
            self._worker_thread = None
        else:
            self._job_ctx.outdated = True
            self.logger.info('[%s] Failed to mark job %s as completed due to version mismatch',
                             self._id, self._job_ctx.job.id)
