from datetime import datetime
import logging
from time import sleep

from flux import settings
from flux.models import BaseOperator, TaskInstance, State
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
        logging.info('Poking: ' + self.sql)
        records = self.hook.get_records(self.sql)
        if not records:
            return False
        else:
            if str(records[0][0]) in ('0', '',):
                return False
            else:
                return True
            print(records[0][0])


class ExternalTaskSensor(BaseSensorOperator):
    """
    Waits for a task to complete in a different DAG
    """
    template_fields = ('execution_date',)
    __mapper_args__ = {
        'polymorphic_identity': 'MySqlSensorOperator'
    }

    def __init__(self, external_dag_id, external_task_id, *args, **kwargs):
        super(ExternalTaskSensor, self).__init__(*args, **kwargs)
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.execution_date = "{{ execution_date }}"

    def poke(self):
        logging.info(
            'Poking for '
            '{self.external_dag_id}.'
            '{self.external_task_id} on '
            '{self.execution_date} ... '.format(**locals()))
        TI = TaskInstance
        session = settings.Session()
        count = session.query(TI).filter(
            TI.dag_id == self.external_dag_id,
            TI.task_id == self.external_task_id,
            TI.state == State.SUCCESS,
            TI.execution_date == self.execution_date
        ).count()
        session.commit()
        session.close()
        return count
