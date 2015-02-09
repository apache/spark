from datetime import datetime
import unittest
from airflow import configuration
configuration.test_mode()
from airflow import jobs, models, executors, utils
from airflow.www.app import app

NUM_EXAMPLE_DAGS = 3
DEV_NULL = '/dev/null'
LOCAL_EXECUTOR = executors.LocalExecutor()
DEFAULT_DATE = datetime(2015, 1, 1)
configuration.test_mode()


class CoreTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        print("INITDB")
        utils.initdb()
        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        utils.initdb()
        self.dag_bash = self.dagbag.dags['example_bash_operator']
        self.runme_0 = self.dag_bash.get_task('runme_0')

    def test_confirm_unittest_mod(self):
        assert configuration.conf.get('core', 'unit_test_mode')

    def test_import_examples(self):
        self.assertEqual(len(self.dagbag.dags), NUM_EXAMPLE_DAGS)

    def test_local_task_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        job = jobs.LocalTaskJob(task_instance=ti, force=True)
        job.run()

    def test_master_job(self):
        job = jobs.MasterJob(dag_id='example_bash_operator', test_mode=True)
        job.run()

    def test_local_backfill_job(self):
        self.dag_bash.clear(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE)
        job = jobs.BackfillJob(
            dag=self.dag_bash,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE)
        job.run()

    def test_raw_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        ti.dag = self.dag_bash
        ti.run(force=True)


class WebUiTests(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        utils.initdb()
        app.config['TESTING'] = True
        self.app = app.test_client()

    def test_index(self):
        response = self.app.get('/', follow_redirects=True)
        assert "DAGs" in response.data
        assert "example_bash_operator" in response.data

    def test_query(self):
        response = self.app.get('/admin/airflow/query')
        assert "Ad Hoc Query" in response.data
        response = self.app.get(
            "/admin/airflow/query?"
            "conn_id=presto_default&"
            "sql=SELECT+COUNT%281%29+FROM+airflow.static_babynames")
        assert "Ad Hoc Query" in response.data

    def test_health(self):
        response = self.app.get('/health')
        assert 'The server is healthy!' in response.data

    def test_dag_views(self):
        response = self.app.get(
            '/admin/airflow/graph?dag_id=example_bash_operator')
        assert "runme_0" in response.data
        response = self.app.get(
            '/admin/airflow/tree?num_runs=25&dag_id=example_bash_operator')
        assert "runme_0" in response.data
        response = self.app.get(
            '/admin/airflow/duration?days=30&dag_id=example_bash_operator')
        assert "DAG: example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/landing_times?'
            'days=30&dag_id=example_bash_operator')
        assert "DAG: example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/gantt?dag_id=example_bash_operator')
        assert "DAG: example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/code?dag_id=example_bash_operator')
        assert "DAG: example_bash_operator" in response.data

    def tearDown(self):
        pass


if __name__ == '__main__':
        unittest.main()
