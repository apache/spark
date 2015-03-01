from datetime import datetime
import getpass
import logging
import signal
import subprocess
import sys
from time import sleep

from sqlalchemy import (
    Column, Integer, String, DateTime)
from sqlalchemy import func, Index
from sqlalchemy.orm.session import make_transient

from airflow import executors
from airflow.configuration import conf
from airflow import models
from airflow import settings
from airflow import utils
import socket
from airflow.utils import State


Base = models.Base
ID_LEN = models.ID_LEN

# Setting up a statsd client if needed
statsd = None
if conf.get('master', 'statsd_on'):
    from statsd import StatsClient
    statsd = StatsClient(
        host=conf.get('master', 'statsd_host'),
        port=conf.getint('master', 'statsd_port'),
        prefix='airflow')


class BaseJob(Base):
    """
    Abstract class to be derived for jobs. Jobs are processing items with state
    and duration that aren't task instances. For instance a BackfillJob is
    a collection of task instance runs, but should have it's own state, start
    and end time.
    """

    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN),)
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(DateTime())
    end_date = Column(DateTime())
    latest_heartbeat = Column(DateTime())
    executor_class = Column(String(500))
    hostname = Column(String(500))
    unixname = Column(String(1000))

    __mapper_args__ = {
        'polymorphic_on': job_type,
        'polymorphic_identity': 'BaseJob'
    }

    def __init__(
            self,
            executor=executors.DEFAULT_EXECUTOR,
            heartrate=conf.getint('master', 'JOB_HEARTBEAT_SEC'),
            *args, **kwargs):
        self.hostname = socket.gethostname()
        self.executor = executor
        self.executor_class = executor.__class__.__name__
        self.start_date = datetime.now()
        self.latest_heartbeat = datetime.now()
        self.heartrate = heartrate
        self.unixname = getpass.getuser()
        super(BaseJob, self).__init__(*args, **kwargs)

    def is_alive(self):
        return (
            (datetime.now() - self.latest_heartbeat).seconds <
            (conf.getint('master', 'JOB_HEARTBEAT_SEC') * 2.1)
        )

    def kill(self):
        session = settings.Session()
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()
        job.end_date = datetime.now()
        try:
            self.on_kill()
        except:
            logging.error('on_kill() method failed')
        session.merge(job)
        session.commit()
        session.close()
        raise Exception("Job shut down externally.")

    def on_kill(self):
        '''
        Will be called when an external kill command is received
        '''
        pass

    def heartbeat_callback(self):
        pass

    def heartbeat(self):
        '''
        Heartbeats update the job's entry in the the database with a timestamp
        for the latest_heartbeat and allows for the job to be killed
        externally. This allows at the system level to monitor what is
        actually active.

        For instance, an old heartbeat for MasterJob would mean something
        is wrong.

        This also allows for any job to be killed externally, regardless
        of who is running it or on which machine it is running.

        Note that if your heartbeat is set to 60 seconds and you call this
        method after 10 seconds of processing since the last heartbeat, it
        will sleep 50 seconds to complete the 60 seconds and keep a steady
        heart rate. If you go over 60 seconds before calling it, it won't
        sleep at all.
        '''
        session = settings.Session()
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()

        if job.state == State.SHUTDOWN:
            self.kill()

        if job.latest_heartbeat:
            sleep_for = self.heartrate - (
                datetime.now() - job.latest_heartbeat).total_seconds()
            if sleep_for > 0:
                sleep(sleep_for)

        job.latest_heartbeat = datetime.now()

        session.merge(job)
        session.commit()
        session.close()

        self.heartbeat_callback()
        logging.debug('[heart] Boom.')

    def run(self):
        if statsd:
            statsd.incr(self.__class__.__name__.lower()+'_start', 1, 1)
        # Adding an entry in the DB
        session = settings.Session()
        self.state = State.RUNNING
        session.add(self)
        session.commit()
        id_ = self.id
        make_transient(self)
        self.id = id_

        # Run
        self._execute()

        # Marking the success in the DB
        self.end_date = datetime.now()
        self.state = State.SUCCESS
        session.merge(self)
        session.commit()
        session.close()

        if statsd:
            statsd.incr(self.__class__.__name__.lower()+'_end', 1, 1)

    def _execute(self):
        raise NotImplemented("This method needs to be overriden")
Index('job_type_heart', BaseJob.job_type, BaseJob.latest_heartbeat)


class MasterJob(BaseJob):

    __mapper_args__ = {
        'polymorphic_identity': 'MasterJob'
    }

    def __init__(
            self,
            dag_id=None,
            subdir=None,
            test_mode=False,
            *args, **kwargs):
        self.dag_id = dag_id
        self.subdir = subdir
        self.test_mode = test_mode
        super(MasterJob, self).__init__(*args, **kwargs)

        self.heartrate=conf.getint('master', 'MASTER_HEARTBEAT_SEC')

    def _execute(self):
        dag_id = self.dag_id

        def signal_handler(signum, frame):
            logging.error("SIGINT (ctrl-c) received")
            sys.exit(1)
        signal.signal(signal.SIGINT, signal_handler)

        utils.pessimistic_connection_handling()

        # Sleep time (seconds) between master runs

        logging.basicConfig(level=logging.DEBUG)
        logging.info("Starting a master scheduler")

        session = settings.Session()
        TI = models.TaskInstance

        # This should get new code
        dagbag = models.DagBag(self.subdir)
        executor = dagbag.executor
        executor.start()
        i = 0
        while (not self.test_mode) or i < 1:
            i += 1
            self.heartbeat()
            dagbag.collect_dags(only_if_updated=True)
            dags = [dagbag.dags[dag_id]] if dag_id else dagbag.dags.values()
            paused_dag_ids = dagbag.paused_dags()
            for dag in dags:
                if dag.dag_id in paused_dag_ids:
                    continue
                logging.info(
                    "Getting latest instance "
                    "for all task in dag " + dag.dag_id)
                sq = session.query(
                    TI.task_id,
                    func.max(TI.execution_date).label('max_ti')
                ).filter(
                    TI.dag_id == dag.dag_id
                ).group_by(TI.task_id).subquery('sq')

                qry = session.query(TI).filter(
                    TI.dag_id == dag.dag_id,
                    TI.task_id == sq.c.task_id,
                    TI.execution_date == sq.c.max_ti,
                )
                latest_ti = qry.all()
                ti_dict = {ti.task_id: ti for ti in latest_ti}
                session.expunge_all()
                session.commit()

                for task in dag.tasks:
                    if task.adhoc:
                        continue
                    if task.task_id not in ti_dict:
                        # Brand new task, let's get started
                        ti = TI(task, task.start_date)
                        ti.refresh_from_db()
                        if ti.is_runnable():
                            logging.debug(
                                'First run for {ti}'.format(**locals()))
                            cmd = ti.command(local=True)
                            executor.queue_command(ti.key, cmd)
                    else:
                        ti = ti_dict[task.task_id]
                        ti.task = task  # Hacky but worky
                        if ti.state == State.RUNNING:
                            continue  # Only one task at a time
                        elif ti.state == State.UP_FOR_RETRY:
                            # If task instance if up for retry, make sure
                            # the retry delay is met
                            if ti.is_runnable():
                                logging.debug('Queuing retry: ' + str(ti))
                                cmd = ti.command(local=True)
                                executor.queue_command(ti.key, cmd)
                        else:
                            # Trying to run the next schedule
                            ti = TI(
                                task=task,
                                execution_date=(
                                    ti.execution_date + task.schedule_interval)
                            )
                            ti.refresh_from_db()
                            if ti.is_runnable():
                                logging.debug('Queuing next run: ' + str(ti))
                                cmd = ti.command(local=True)
                                executor.queue_command(ti.key, cmd)
                    executor.heartbeat()
                session.close()
        executor.end()

    def heartbeat_callback(self):
        if statsd:
            statsd.gauge('master_heartbeat', 1, 1)



class BackfillJob(BaseJob):
    """
    A backfill job consists of a dag or subdag for a specific time range. It
    triggers a set of task instance runs, in the right order and lasts for
    as long as it takes for the set of task instance to be completed.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'BackfillJob'
    }

    def __init__(
            self,
            dag, start_date=None, end_date=None, mark_success=False,
            include_adhoc=False,
            *args, **kwargs):
        self.dag = dag
        dag.override_start_date(start_date)
        self.dag_id = dag.dag_id
        self.bf_start_date = start_date
        self.bf_end_date = end_date
        self.mark_success = mark_success
        self.include_adhoc = include_adhoc
        super(BackfillJob, self).__init__(*args, **kwargs)

    def _execute(self):
        """
        Runs a dag for a specified date range.
        """
        session = settings.Session()

        start_date = self.bf_start_date
        end_date = self.bf_end_date

        # picklin'
        pickle_id = None
        if self.executor.__class__ not in (
                executors.LocalExecutor, executors.SequentialExecutor):
            pickle = models.DagPickle(self.dag)
            session.add(pickle)
            session.commit()
            pickle_id = pickle.id

        executor = self.executor
        executor.start()

        # Build a list of all intances to run
        tasks_to_run = {}
        failed = []
        succeeded = []
        started = []
        wont_run = []
        for task in self.dag.tasks:
            if (not self.include_adhoc) and task.adhoc:
                continue

            start_date = start_date or task.start_date
            end_date = end_date or task.end_date or datetime.now()
            for dttm in utils.date_range(
                    start_date, end_date, task.dag.schedule_interval):
                ti = models.TaskInstance(task, dttm)
                tasks_to_run[ti.key] = ti

        # Triggering what is ready to get triggered
        while tasks_to_run:
            msg = (
                "Yet to run: {0} | "
                "Succeeded: {1} | "
                "Started: {2} | "
                "Failed: {3} | "
                "Won't run: {4} ").format(
                len(tasks_to_run),
                len(succeeded),
                len(started),
                len(failed),
                len(wont_run))

            logging.info(msg)
            for key, ti in tasks_to_run.items():
                ti.refresh_from_db()
                if ti.state == State.SUCCESS and key in tasks_to_run:
                    succeeded.append(key)
                    del tasks_to_run[key]
                elif ti.is_runnable():
                    executor.queue_command(
                        key=ti.key, command=ti.command(
                            mark_success=self.mark_success,
                            local=True,
                            pickle_id=pickle_id)
                    )
                    ti.state = State.RUNNING
                    if key not in started:
                        started.append(key)
            self.heartbeat()
            executor.heartbeat()

            # Reacting to events
            for key, state in executor.get_event_buffer().items():
                dag_id, task_id, execution_date = key
                if key not in tasks_to_run:
                    continue
                ti = tasks_to_run[key]
                ti.refresh_from_db()
                if ti.state == State.FAILED:
                    failed.append(key)
                    logging.error("Task instance " + str(key) + " failed")
                    del tasks_to_run[key]
                    # Removing downstream tasks from the one that has failed
                    for t in self.dag.get_task(task_id).get_flat_relatives(
                            upstream=False):
                        key = (ti.dag_id, t.task_id, execution_date)
                        if key in tasks_to_run:
                            wont_run.append(key)
                            del tasks_to_run[key]
                elif ti.state == State.SUCCESS:
                    succeeded.append(key)
                    del tasks_to_run[key]
        executor.end()
        session.close()


class LocalTaskJob(BaseJob):

    __mapper_args__ = {
        'polymorphic_identity': 'LocalTaskJob'
    }

    def __init__(
            self,
            task_instance,
            ignore_dependencies=False,
            force=False,
            mark_success=False,
            pickle_id=None,
            *args, **kwargs):
        self.task_instance = task_instance
        self.ignore_dependencies = ignore_dependencies
        self.force = force
        self.pickle_id = pickle_id
        self.mark_success = mark_success
        super(LocalTaskJob, self).__init__(*args, **kwargs)

    def _execute(self):
        command = self.task_instance.command(
            raw=True,
            ignore_dependencies=self.ignore_dependencies,
            force=self.force,
            pickle_id=self.pickle_id,
            mark_success=self.mark_success,
            job_id=self.id,
        )
        self.process = subprocess.Popen(['bash', '-c', command])
        return_code = None
        while return_code is None:
            self.heartbeat()
            return_code = self.process.poll()

    def on_kill(self):
        self.process.terminate()
