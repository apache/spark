import subprocess
import logging

from airflow import configuration
from celery import Celery

# to start the celery worker, run the command:
# "celery -A airflow.executors.celery_worker worker --loglevel=info"

# app = Celery('airflow.executors.celery_worker', backend='amqp', broker='amqp://')
config = configuration.get_config()

app = Celery(
    config.get('celery', 'CELERY_APP_NAME'),
    backend=config.get('celery', 'CELERY_BROKER'),
    broker=config.get('celery', 'CELERY_RESULTS_BACKEND'))

@app.task(name='airflow.executors.celery_worker.execute_command')
def execute_command(command):
    logging.info("Executing command in Celery " + command)
    try:
        subprocess.Popen(command, shell=True).wait()
    except Exception as e:
        raise e
    return True
