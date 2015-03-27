import logging
import errno
from tempfile import (gettempdir, mkdtemp, NamedTemporaryFile)
import shutil
from subprocess import Popen, STDOUT, PIPE
from contextlib import contextmanager

from airflow.models import BaseOperator
from airflow.utils import apply_defaults

@contextmanager
def TemporaryDirectory(suffix='',prefix=None, dir=None):
    name = mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    try:
        yield name
    finally:
            try:
                shutil.rmtree(name)
            except OSError as exc:
                # ENOENT - no such file or directory
                if exc.errno != errno.ENOENT:
                    raise

class BashOperator(BaseOperator):
    '''
    Execute a Bash script, command or set of commands.

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed.
    :type bash_command: string
    '''
    template_fields = ('bash_command',)
    template_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    __mapper_args__ = {
        'polymorphic_identity': 'BashOperator'
    }

    @apply_defaults
    def __init__(self, bash_command, *args, **kwargs):
        super(BashOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command

    def execute(self, context):
        '''
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        '''
        bash_command = self.bash_command
        logging.info("tmp dir root location: \n"+ gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:
                f.write(bash_command)
                f.flush()
                fname = f.name
                script_location=tmp_dir + "/" + fname
                logging.info("Temporary script "
                             "location :{0}".format(script_location))
                logging.info("Running command: " + bash_command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir)

                self.sp = sp

                logging.info("Output:")
                for line in iter(sp.stdout.readline, ''):
                    logging.info(line.strip())
                sp.wait()
                logging.info("Command exited with "
                             "return code {0}".format(sp.returncode))

                if sp.returncode:
                    raise Exception("Bash command failed")

    def on_kill(self):
        logging.info('Sending SIGTERM signal to bash subprocess')
        self.sp.terminate()
