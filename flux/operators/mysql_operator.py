import logging
from flux.models import BaseOperator
from flux.hooks import MySqlHook


class MySqlOperator(BaseOperator):
    """
    Executes sql code in a specific mysql database.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'MySqlOperator'
    }
    template_fields = ('sql',)

    def __init__(self, sql, mysql_dbid, *args, **kwargs):
        """
        Parameters:
        mysql_dbid: reference to a specific mysql database
        sql: the sql code you to be executed
        """
        super(MySqlOperator, self).__init__(*args, **kwargs)

        self.hook = MySqlHook(mysql_dbid=mysql_dbid)
        self.sql = sql

    def execute(self, execution_date):
        logging.info('Executing: ' + self.sql)
        self.hook.run(self.sql)
