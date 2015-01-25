import logging
from subprocess import Popen, STDOUT, PIPE

from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class BashOperator(BaseOperator):

    template_fields = ('bash_command',)
    template_ext = ('.sh', '.bash',)

    __mapper_args__ = {
        'polymorphic_identity': 'BashOperator'
    }

    @apply_defaults
    def __init__(self, bash_command, *args, **kwargs):
        super(BashOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command

    def execute(self, execution_date):

        bash_command = self.bash_command

        logging.info("Runnning command: " + bash_command)
        sp = Popen(
            ['bash', '-c', bash_command],
            stdout=PIPE, stderr=STDOUT)

        self.sp = sp

        logging.info("Output:")
        for line in iter(sp.stdout.readline, ''):
            logging.info(line.strip())
        sp.wait()
        logging.info("Command exited with return code %d", sp.returncode)

        if sp.returncode:
            raise Exception("Bash command failed")

    def on_kill(self):
        logging.info('Sending SIGTERM signal to bash subprocess')
        self.sp.terminate()
