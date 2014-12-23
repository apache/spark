import logging

from airflow.configuration import conf
from airflow.hooks import HiveHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class HiveOperator(BaseOperator):
    """
    Executes hql code in a specific Hive database.

    :param hql: the hql to be executed
    :type hql: string
    :param hive_dbid: reference to the Hive database
    :type hive_dbid: string
    """

    __mapper_args__ = {
        'polymorphic_identity': 'HiveOperator'
    }
    template_fields = ('hql',)
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(
            self, hql,
            hive_dbid=conf.get('hooks', 'HIVE_DEFAULT_DBID'),
            *args, **kwargs):
        super(HiveOperator, self).__init__(*args, **kwargs)

        self.hive_dbid = hive_dbid
        self.hook = HiveHook(hive_dbid=hive_dbid)
        self.hql = hql

    def execute(self, execution_date):
        logging.info('Executing: ' + self.hql)
        self.hook.run_cli(hql=self.hql)
