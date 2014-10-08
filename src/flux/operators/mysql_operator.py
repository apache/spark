import logging
from flux.models import BaseOperator
from flux.hooks import MySqlHook


class MySqlOperator(BaseOperator):

    __mapper_args__ = {
        'polymorphic_identity': 'MySqlOperator'
    }
    template_fields = ('sql',)

    def __init__(self, sql, mysql_dbid, *args, **kwargs):
        super(MySqlOperator, self).__init__(*args, **kwargs)

        self.hook = MySqlHook(mysql_dbid=mysql_dbid)
        self.sql = sql

    def execute(self, execution_date):
        logging.info('Executing: ' + self.sql)
        self.hook.run(self.sql)
