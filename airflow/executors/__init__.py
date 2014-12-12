#from airflow.executors.celery_executor import CeleryExecutor
from airflow.executors.local_executor import LocalExecutor
#from airflow.executors.sequential_executor import SequentialExecutor

# DEFAULT_EXECUTOR = CeleryExecutor()
DEFAULT_EXECUTOR = LocalExecutor()
#DEFAULT_EXECUTOR = SequentialExecutor()
