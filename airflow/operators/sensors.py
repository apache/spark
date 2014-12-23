from datetime import datetime
import logging
from time import sleep

from airflow import settings
from airflow.configuration import conf
from airflow.hooks import HiveHook
from airflow.models import BaseOperator
from airflow.models import DatabaseConnection as DB
from airflow.models import State
from airflow.models import TaskInstance
from airflow.utils import apply_defaults


class BaseSensorOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            poke_interval=60,
            timeout=60*60*24*7,
            *args, **kwargs):
        super(BaseSensorOperator, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout

        # Since a sensor pokes in a loop, no need for higher level retries
        self.retries = 0

    def poke(self):
        '''
        Function that the sensors defined while deriving this class should
        override.
        '''
        raise Exception('Override me.')

    def execute(self, execution_date):
        started_at = datetime.now()
        while not self.poke():
            sleep(self.poke_interval)
            if (datetime.now() - started_at).seconds > self.timeout:
                raise Exception('Snap. Time is OUT.')
        logging.info("Success criteria met. Exiting.")


class SqlSensor(BaseSensorOperator):
    """
    Will keep trying until sql returns no row, or if the first cell
    in (0, '0', '')
    """
    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)

    __mapper_args__ = {
        'polymorphic_identity': 'SqlSensor'
    }

    @apply_defaults
    def __init__(self, db_id, sql, *args, **kwargs):

        super(SqlSensor, self).__init__(*args, **kwargs)

        self.sql = sql
        self.db_id = db_id

        session = settings.Session()
        db = session.query(DB).filter(DB.db_id==db_id).all()
        if not db:
            raise Exception("db_id doesn't exist in the repository")
        self.hook = db[0].get_hook()
        session.commit()
        session.close()

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
        'polymorphic_identity': 'ExternalTaskSensor'
    }

    @apply_defaults
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


class HivePartitionSensor(BaseSensorOperator):
    """
    Waits for the apparation of a partition in Hive
    """
    template_fields = ('table', 'partition',)
    __mapper_args__ = {
        'polymorphic_identity': 'HivePartitionSensor'
    }

    @apply_defaults
    def __init__(
            self,
            table, partition="ds='{{ ds }}'",
            hive_dbid=conf.get('hooks', 'HIVE_DEFAULT_DBID'),
            schema='default',
            *args, **kwargs):
        super(HivePartitionSensor, self).__init__(*args, **kwargs)
        if '.' in table:
            schema, table = table.split('.')
        self.hive_dbid = hive_dbid
        self.hook = HiveHook(hive_dbid=hive_dbid)
        self.table = table
        self.partition = partition
        self.schema = schema

    def poke(self):
        logging.info(
            'Poking for table {self.schema}.{self.table}, '
            'partition {self.partition}'.format(**locals()))
        return self.hook.check_for_partition(
            self.schema, self.table, self.partition)
