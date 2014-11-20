import logging

from airflow.utils import State


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
        if key not in self.commands or self.commands[key] in State.runnable():
            logging.info("Adding to queue: " + command)
            self.commands[key] = State.RUNNING
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

