import logging
import tempfile

from airflow.configuration import conf
from airflow.hooks import HiveHook, FtpHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class Hive2FtpOperator(BaseOperator):
    """
    Executes hql code in a specific Hive database.

    :param hql: the hql to be exported
    :type hql: string
    :param hive_dbid: reference to the Hive database
    :type hive_dbid: string
    :param ftp_dbid: reference to the ftp destination
    :type ftp_dbid: string
    """

    __mapper_args__ = {
        'polymorphic_identity': 'Hive2FtpOperator'
    }
    template_fields = ('hql',)
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(
            self, hql,
            ftp_dbid,
            destination_filepath,
            hive_dbid=conf.get('hooks', 'HIVE_DEFAULT_DBID'),
            *args, **kwargs):
        super(Hive2FtpOperator, self).__init__(*args, **kwargs)

        self.hive_dbid = hive_dbid
        self.ftp_dbid = ftp_dbid
        self.destination_filepath = self.destination_filepath
        self.ftp = FtpHook(ftp_dbid=ftp_dbid)
        self.hook = HiveHook(hive_dbid=hive_dbid)
        self.hql = hql.strip().rstrip(';')

    def execute(self, execution_date):
        tmpfile = tempfile.NamedTemporaryFile()
        hql = """\
        INSERT OVERWRITE LOCAL DIRECTORY '{tmpfile.name}'
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        {self.hql};
        """.format(**locals())
        logging.info('Executing: ' + hql)
        self.hook.run_cli(hql=hql)

        ftp.push_from_local(self.destination_filepath, tmpfile.name)

        # Cleaning up
        hql = "DROP TABLE {table};"
        self.hook.run_cli(hql=self.hql)
        tmpfile.close()
