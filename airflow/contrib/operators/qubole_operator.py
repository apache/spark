import logging
import datetime
import os

from qds_sdk.qubole import Qubole
from qds_sdk.commands import *
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow import configuration

Qubole.configure(api_token=configuration.get('qds', 'auth_token'), api_url="https://qa3.qubole.net/api")

CommandClasses = {
    "hivecmd": HiveCommand,
    "sparkcmd": SparkCommand,
    "dbtapquerycmd": DbTapQueryCommand,
    "pigcmd":  PigCommand,
    "hadoopcmd": HadoopCommand,
    "shellcmd": ShellCommand,
    "dbexportcmd": DbExportCommand,
    "dbimportcmd": DbImportCommand,
    "prestocmd": PrestoCommand
}

ArgsWithHyphen = ['cluster_label', 'app_id']

CommandArgs = {
    "hivecmd": ['query', 'script_location', 'macros', 'tags', 'sample_size', 'cluster_label', 'name'],
    'sparkcmd': ['program', 'cmdline', 'sql', 'script_location', 'macros', 'tags', 'cluster_label', 'language', 'app_id', 'name', 'arguments', 'user_program_arguments'],
    'prestocmd': ['query', 'script_location', 'macros', 'tags', 'cluster_label', 'name'],
    'hadoopcmd': ['tags', 'cluster_label', 'name'],
    'shellcmd': ['script', 'script_location', 'files', 'archives', 'tags', 'cluster_label', 'name'],
    'pigcmd': ['script', 'script_location', 'tags', 'cluster_label', 'name'],
    'dbtapquerycmd': ['db_tap_id', 'query', 'macros', 'tags', 'name'],
    'dbexportcmd': ['mode', 'hive_table', 'partition_spec', 'dbtap_id', 'db_table', 'db_update_mode', 'db_update_keys', 'export_dir', 'fields_terminated_by', 'tags', 'name'],
    'dbimportcmd': ['mode', 'hive_table', 'dbtap_id', 'db_table', 'where_clause', 'parallelism', 'extract_query', 'boundary_query', 'split_column', 'tags', 'name']
}


class QuboleOperator(BaseOperator):
    ui_color = '#3064A1'
    ui_fgcolor = '#fff'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        self.args = self.create_cmd_args()
        self.cls = CommandClasses[self.kwargs['command_type']]
        super(QuboleOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        args = self.cls.parse(self.args)
        self.cmd = self.cls.create(**args)
        logging.info("Qubole command created with Id: %s and Status: %s" % (str(self.cmd.id), self.cmd.status))

        while not Command.is_done(self.cmd.status):
            time.sleep(Qubole.poll_interval)
            self.cmd = self.cls.find(self.cmd.id)
            logging.info("Command Id: %s and Status: %s" % (str(self.cmd.id), self.cmd.status))

        if self.kwargs.has_key('fetch_logs') and self.kwargs['fetch_logs'] == True:
            logging.info("Logs for Command Id: %s, %s" % (str(self.cmd.id), self.cmd.get_log()))

    def on_kill(self):
        logging.info('Sending KILL signal to Qubole')
        if self.cls and self.cmd:
            self.cmd.cancel()

    def get_results(self, fp=None, inline=False, delim=None, fetch=False):
        if fp is None:
            iso = datetime.datetime.utcnow().isoformat()
            logpath = os.path.expanduser(configuration.get('core', 'BASE_LOG_FOLDER'))
            resultfile = logpath + self.task_id + '/results'
            configuration.mkdir_p(resultfile)
            fp = open(resultfile + '/' + iso, 'wb')

        self.cmd.get_results(fp, inline, delim, fetch)
        return fp

    def get_log(self):
        self.cmd.get_log()

    def get_jobs_id(self):
        Command.get_jobs_id(self.cls, self.cmd.id)

    def create_cmd_args(self):
        args = []
        for k,v in self.kwargs.items():
            if k in CommandArgs[self.kwargs['command_type']]:
                if k in ArgsWithHyphen:
                    args.append("--%s=%s"%(k.replace('_', '-'),v))
                else:
                    args.append("--%s=%s" %(k,v))

            if k == 'notify' and v is True:
                args.append("--notify")

        return args


