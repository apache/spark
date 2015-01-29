from datetime import datetime
import unittest

import airflow

NUM_EXAMPLE_DAGS = 3
DEV_NULL = '/dev/null'
LOCAL_EXECUTOR = airflow.executors.LocalExecutor()
DEFAULT_DATE = datetime(2015, 1, 1)

class CoreTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_import_examples(self):
        dagbag = airflow.models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        self.assertEqual(len(dagbag.dags), NUM_EXAMPLE_DAGS)

    def test_backfill_example1(self):
        dagbag = airflow.models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        dag = dagbag.dags['example1']
        TI = airflow.models.TaskInstance
        ti = TI.get_or_create(
            task=dag.get_task('runme_0'), execution_date=DEFAULT_DATE)
        airflow.jobs.RunJob(task_instance=ti, force=True)


if __name__ == '__main__':
        unittest.main()
