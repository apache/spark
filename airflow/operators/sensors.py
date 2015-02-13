from datetime import datetime
import logging
from time import sleep

from airflow import settings
from airflow.configuration import conf
from airflow.hooks import HiveHook
from airflow.models import BaseOperator
from airflow.models import Connection as DB
from airflow.models import State
from airflow.models import TaskInstance
from airflow.utils import apply_defaults

from snakebite.client import HAClient, Namenode


class BaseSensorOperator(BaseOperator):
    '''
    Sensor operators are derived from this class an inherit these attributes.

    Sensor operators keep executing at a time interval and succeed when
        a criteria is met and fail if and when they time out.

    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    :param timeout: Time, in seconds before the task times out and fails.
    :type timeout: int
    '''
    ui_color = '#e6f1f2'

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

    def execute(self, context):
        started_at = datetime.now()
        while not self.poke():
            sleep(self.poke_interval)
            if (datetime.now() - started_at).seconds > self.timeout:
                raise Exception('Snap. Time is OUT.')
        logging.info("Success criteria met. Exiting.")


class SqlSensor(BaseSensorOperator):
    """
    Runs a sql statement until a criteria is met. It will keep trying until
    sql returns no row, or if the first cell in (0, '0', '').

    :param conn_id: The connection to run the sensor agains
    :type conn_id: string
    :param sql: The sql to run. To pass, it needs to return at least one cell
        that contains a non-zero / empty string value.
    """
    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)

    __mapper_args__ = {
        'polymorphic_identity': 'SqlSensor'
    }

    @apply_defaults
    def __init__(self, conn_id, sql, *args, **kwargs):

        super(SqlSensor, self).__init__(*args, **kwargs)

        self.sql = sql
        self.conn_id = conn_id

        session = settings.Session()
        db = session.query(DB).filter(DB.conn_id == conn_id).all()
        if not db:
            raise Exception("conn_id doesn't exist in the repository")
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

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: string
    :param external_task_id: The task_id that contains the task you want to
        wait for
    :type external_task_id: string
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
    Waits for a partition to show up in Hive

    :param table: The name of the table to wait for, supports the dot
        notation (my_database.my_table)
    :type table: string
    :param partition: The partition clause to wait for. This is passed as
        is to the Metastor Thrift client "get_partitions_by_filter" method,
        and apparently supports SQL like notation as in `ds='2015-01-01'
        AND type='value'` and > < sings as in "ds>=2015-01-01"
    :type partition: string
    """
    template_fields = ('table', 'partition',)
    __mapper_args__ = {
        'polymorphic_identity': 'HivePartitionSensor'
    }

    @apply_defaults
    def __init__(
            self,
            table, partition="ds='{{ ds }}'",
            hive_conn_id=conf.get('hooks', 'HIVE_DEFAULT_CONN_ID'),
            schema='default',
            *args, **kwargs):
        super(HivePartitionSensor, self).__init__(*args, **kwargs)
        if '.' in table:
            schema, table = table.split('.')
        if not partition:
            partition = "ds='{{ ds }}'"
        self.hive_conn_id = hive_conn_id
        self.hook = HiveHook(hive_conn_id=hive_conn_id)
        self.table = table
        self.partition = partition
        self.schema = schema

    def poke(self):
        logging.info(
            'Poking for table {self.schema}.{self.table}, '
            'partition {self.partition}'.format(**locals()))
        return self.hook.check_for_partition(
            self.schema, self.table, self.partition)


class HdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS
    """
    template_fields = ('filepath',)
    __mapper_args__ = {
        'polymorphic_identity': 'HdfsSensor'
    }

    @apply_defaults
    def __init__(
            self,
            filepath,
            hdfs_conn_id='hdfs_default',
            *args, **kwargs):
        super(HdfsSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        session = settings.Session()
        db = session.query(DB).filter(DB.conn_id == hdfs_conn_id).first()
        if not db:
            raise Exception("conn_id doesn't exist in the repository")
        self.host = db.host
        self.port = db.port
        NAMENODES = [Namenode(self.host, self.port)]
        self.sb = HAClient(NAMENODES)
        session.commit()
        session.close()

    def poke(self):
        logging.getLogger("snakebite").setLevel(logging.WARNING)
        logging.info(
            'Poking for file {self.filepath} '.format(**locals()))
        try:
            files = [f for f in self.sb.ls([self.filepath])]
        except:
            return False
        print([i for i in f])
        return True
