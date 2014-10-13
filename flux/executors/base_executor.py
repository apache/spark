import logging
import time

from flux import settings
from flux.utils import State


class BaseExecutor(object):

    def __init__(self):
        self.commands = {}
        self.event_buffer = {}

    def start(self):
        """
        Executors may need to get things started. For example LocalExecutor
        starts N workers.
        """
        pass

    def queue_command(self, key, command):
        """
        """
        logging.info("Adding to queue: " + command)
        self.commands[key] = "running"
        self.execute_async(key, command)

    def change_state(self, key, state):
        self.commands[key] = state
        self.event_buffer[key] = state

    def get_event_buffer(self):
        """
        Returns and flush the event buffer
        """
        d = self.event_buffer
        self.event_buffer = {}
        return d

    def execute_async(self, key, command):
        """
        This method will execute the command asynchronously.
        """
        raise NotImplementedError()

    def end(self):
        """
        This method is called when the caller is done submitting job and is
        wants to wait synchronously for the job submitted previously to be
        all done.
        """
        raise NotImplementedError()

import multiprocessing
import subprocess


class LocalWorker(multiprocessing.Process):

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        proc_name = self.name
        while True:
            key, command = self.task_queue.get()
            if key is None:
                # Received poison pill, no more tasks to run
                self.task_queue.task_done()
                break
            BASE_FOLDER = settings.BASE_FOLDER
            print command
            command = (
                "exec bash -c '"
                "cd $FLUX_HOME;\n" +
                "source init.sh;\n" +
                command +
                "'"
            ).format(**locals())
            try:
                sp = subprocess.Popen(command, shell=True).wait()
            except Exception as e:
                self.result_queue.put((key, State.FAILED))
                raise e
            self.result_queue.put((key, State.SUCCESS))
            self.task_queue.task_done()
            time.sleep(1)


class LocalExecutor(BaseExecutor):

    def __init__(self, parallelism=16):
        super(LocalExecutor, self).__init__()
        self.parallelism = parallelism

    def start(self):
        self.queue = multiprocessing.JoinableQueue()
        self.result_queue = multiprocessing.Queue()
        self.workers = [
            LocalWorker(self.queue, self.result_queue)
            for i in xrange(self.parallelism)
        ]

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
        [self.queue.put((None, None)) for w in self.workers]
        # Wait for commands to finish
        self.queue.join()


class SequentialExecutor(BaseExecutor):
    """
    Will only run one task instance at a time, can be used for debugging.
    """
    def __init__(self):
        super(SequentialExecutor, self).__init__()
        self.commands_to_run = []

    def queue_command(self, key, command):
        self.commands_to_run.append((key, command,))

    def heartbeat(self):
        for key, command in self.commands_to_run:
            try:
                sp = subprocess.Popen(command, shell=True).wait()
            except Exception as e:
                self.change_state(key, State.FAILED)
                raise e
            self.change_state(key, State.SUCCESS)
        self.commands_to_run = []

    def end(self):
        pass
