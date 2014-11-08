import multiprocessing
import time

from airflow.executors.base_executor import BaseExecutor
from airflow.configuration import getconf
from airflow.utils import State
from celery_worker import execute_command


class CeleryExecutor(BaseExecutor):
    """ Submits the task to RabbitMQ, which is picked up and executed by a bunch
        of worker processes """
    def __init__(self, parallelism=1):
        super(CeleryExecutor, self).__init__()
        self.parallelism = parallelism

    def start(self):
        self.queue = multiprocessing.JoinableQueue()
        self.result_queue = multiprocessing.Queue()
        self.workers = [
            CelerySubmitter(self.queue, self.result_queue)
            for i in xrange(self.parallelism)]

        for w in self.workers:
            w.start()

    def execute_async(self, key, command):
        self.queue.put((key, command))

    def heartbeat(self):
        while not self.result_queue.empty():
            results = self.result_queue.get()
            self.change_state(*results)

    def end(self):
        # Sending poison pill to all worker
        [self.queue.put(None) for w in self.workers]
        self.queue.join()


class CelerySubmitter(multiprocessing.Process):

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        while True:
            key, command = self.task_queue.get()
            if command is None:
                # Received poison pill, no more tasks to run
                self.task_queue.task_done()
                break
            BASE_FOLDER = getconf().get('core', 'BASE_FOLDER')
            command = (
                "exec bash -c '"
                "cd $AIRFLOW_HOME;\n" +
                "source init.sh;\n" +
                command +
                "'"
            ).format(**locals())

            try:
                res = execute_command.delay(command)
                result = res.get()
            except Exception as e:
                self.result_queue.put((key, State.FAILED))
                raise e
            self.result_queue.put((key, State.SUCCESS))
            self.task_queue.task_done()
            time.sleep(1)

