from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import datetime

from airflow import models
from airflow.operators.dummy_operator import DummyOperator


class DagRunTest(unittest.TestCase):
    def test_id_for_date(self):
        run_id = models.DagRun.id_for_date(
            datetime.datetime(2015, 1, 2, 3, 4, 5, 6, None))
        assert run_id == 'scheduled__2015-01-02T03:04:05', (
            'Generated run_id did not match expectations: {0}'.format(run_id))


class DagBagTest(unittest.TestCase):

    def test_get_existing_dag(self):
        """
        test that were're able to parse some example DAGs and retrieve them
        """
        dagbag = models.DagBag(include_examples=True)

        some_expected_dag_ids = ["example_bash_operator",
                                 "example_branch_operator"]

        for dag_id in some_expected_dag_ids:
            dag = dagbag.get_dag(dag_id)

            assert dag is not None
            assert dag.dag_id == dag_id

        assert dagbag.size() >= 7

    def test_get_non_existing_dag(self):
        """
        test that retrieving a non existing dag id returns None without crashing
        """
        dagbag = models.DagBag(include_examples=True)

        non_existing_dag_id = "non_existing_dag_id"
        assert dagbag.get_dag(non_existing_dag_id) is None


class TaskInstanceTest(unittest.TestCase):

    def test_run_pooling_task(self):
        """
        test that running task with mark_success param update task state as SUCCESS
        without running task.
        """
        dag = models.DAG(dag_id='test_run_pooling_task')
        task = DummyOperator(task_id='test_run_pooling_task_op', dag=dag,
                             pool='test_run_pooling_task_pool', owner='airflow',
                             start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = models.TaskInstance(task=task, execution_date=datetime.datetime.now())
        ti.run()
        assert ti.state == models.State.QUEUED

    def test_run_pooling_task_with_mark_success(self):
        """
        test that running task with mark_success param update task state as SUCCESS
        without running task.
        """
        dag = models.DAG(dag_id='test_run_pooling_task_with_mark_success')
        task = DummyOperator(task_id='test_run_pooling_task_with_mark_success_op', dag=dag,
                             pool='test_run_pooling_task_with_mark_success_pool', owner='airflow',
                             start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = models.TaskInstance(task=task, execution_date=datetime.datetime.now())
        ti.run(mark_success=True)
        assert ti.state == models.State.SUCCESS
