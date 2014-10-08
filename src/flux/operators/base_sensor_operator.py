from datetime import datetime
from time import sleep

from flux.models import BaseOperator
from flux.hooks import MySqlHook


class BaseSensorOperator(BaseOperator):

    def __init__(self, poke_interval=5, timeout=60*60*24*7, *args, **kwargs):
        super(BaseSensorOperator, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout

    def poke(self):
        raise Exception('Override me.')

    def execute(self, execution_date):
        started_at = datetime.now()
        while not self.poke():
            sleep(self.poke_interval)
            if (datetime.now() - started_at).seconds > self.timeout:
                raise Exception('Snap. Time is OUT.')


class MySqlSensorOperator(BaseSensorOperator):
    """
    Will fail if sql returns no row, or if the first cell in (0, '0', '')
    """
    template_fields = ('sql',)
    __mapper_args__ = {
        'polymorphic_identity': 'MySqlSensorOperator'
    }

    def __init__(self, mysql_dbid, sql, *args, **kwargs):
        super(MySqlSensorOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.mysql_dbid = mysql_dbid
        self.hook = MySqlHook(mysql_dbid=mysql_dbid)

    def poke(self):
        print('Poking: ' + self.sql)
        records = self.hook.get_records(self.sql)
        if not records:
            return False
        else:
            if str(records[0][0]) in ('0', '',):
                return False
            else:
                return True
            print(records[0][0])
