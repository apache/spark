import logging

from airflow.settings import conf

_EXECUTOR = conf.get('core', 'EXECUTOR')

if _EXECUTOR == 'LocalExecutor':
    from airflow.executors.local_executor import LocalExecutor
    DEFAULT_EXECUTOR = LocalExecutor()
elif _EXECUTOR == 'CeleryExecutor':
    from airflow.executors.celery_executor import CeleryExecutor
    DEFAULT_EXECUTOR = CeleryExecutor()
elif _EXECUTOR == 'SequentialExecutor':
    from airflow.executors.sequential_executor import SequentialExecutor
    DEFAULT_EXECUTOR = SequentialExecutor()
else:
    raise Exception("Executor {0} not supported.".format(_EXECUTOR))

logging.info("Using executor " + _EXECUTOR)
