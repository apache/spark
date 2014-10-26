from celery_executor import CeleryExecutor
from local_executor import LocalExecutor
from sequential_executor import SequentialExecutor

# DEFAULT_EXECUTOR = CeleryExecutor()
# DEFAULT_EXECUTOR = LocalExecutor()
DEFAULT_EXECUTOR = SequentialExecutor()
