import logging
from airflow.models import BaseOperator
from airflow.hooks import HiveHook


class HiveOperator(BaseOperator):
    """
    Executes sql code in a specific mysql database.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'HiveOperator'
    }
    template_fields = ('hql',)

    def __init__(self, hql, hive_dbid, *args, **kwargs):
        """
        Parameters:
        mysql_dbid: reference to a specific mysql database
        sql: the sql code you to be executed
        """
        super(HiveOperator, self).__init__(*args, **kwargs)

        self.hive_dbid = hive_dbid
        self.hook = HiveHook(hive_dbid=hive_dbid)
        self.hql = hql


    def execute(self, execution_date):
        logging.info('Executing: ' + self.hql)
        self.hook.run_cli(hql=self.hql, schema=self.hive_dbid)
