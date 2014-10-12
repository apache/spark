import logging
from flux.models import BaseOperator
from subprocess import Popen, PIPE


class BashOperator(BaseOperator):

    template_fields = ('bash_command',)

    __mapper_args__ = {
        'polymorphic_identity': 'BashOperator'
    }

    def __init__(self, bash_command, *args, **kwargs):
        super(BashOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command

    def execute(self, execution_date):

        bash_command = self.bash_command

        logging.info("Runnning command: " + bash_command)
        sp = Popen(
            bash_command, shell=True, stdout=PIPE, stderr=PIPE)
        out, err = sp.communicate()
        sp.wait()

        logging.info("Command STDOUT:\n" + out)
        if err:
            logging.error(err)
        if sp.returncode:
            raise Exception("Bash command failed")
