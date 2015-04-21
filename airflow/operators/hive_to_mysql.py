import logging

from airflow.hooks import HiveServer2Hook, MySqlHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class HiveToMySqlTransfer(BaseOperator):
    """
    Moves data from Hive to MySQL, note that for now the data is loaded
    into memory before being pushed to MySQL, so this operator should
    be used for smallish amount of data.

    :param sql: SQL query to execute against the MySQL database
    :type sql: str
    :param mysql_table: target MySQL table, use dot notation to target a
        specific database
    :type mysql_table: str
    :param mysql_conn_id: source mysql connection
    :type mysql_conn_id: str
    :param hive_conn_id: desctination hive connection
    :type hive_conn_id: str
    """

    __mapper_args__ = {
        'polymorphic_identity': 'MySqlToHiveOperator'
    }
    template_fields = ('sql', 'mysql_table')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            sql,
            mysql_table,
            hive_cli_conn_id='hiveserver2_default',
            mysql_conn_id='mysql_default',
            *args, **kwargs):
        super(HiveToMySqlTransfer, self).__init__(*args, **kwargs)
        self.sql = sql
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id
        self.hive_cli_conn_id = hive_cli_conn_id

    def execute(self, context):
        hive = HiveServer2Hook(hiveserver2_conn_id=self.hive_cli_conn_id)
        logging.info("Extracting data from Hive")
        results = hive.get_records(self.sql)

        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        logging.info("Inserting rows into MySQL")
        mysql.insert_rows(table=self.mysql_table, rows=results)
