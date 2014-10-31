import subprocess

from airflow.executors.base_executor import BaseExecutor
from airflow.utils import State


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
