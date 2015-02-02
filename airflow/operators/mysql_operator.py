import logging

from airflow.hooks import MySqlHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class MySqlOperator(BaseOperator):
    """
    Executes sql code in a specific MySQL database

    :param mysql_conn_id: reference to a specific mysql database
    :type mysql_conn_id: string
    :param sql: the sql code to be executed
    :type sql: string or string pointing to a template file. Fil must have
        a '.sql' extensions.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'MySqlOperator'
    }
    template_fields = ('sql',)
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self, sql, mysql_conn_id, *args, **kwargs):
        super(MySqlOperator, self).__init__(*args, **kwargs)

        self.hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        self.sql = sql

    def execute(self, execution_date):
        logging.info('Executing: ' + self.sql)
        self.hook.run(self.sql)
