import subprocess
import time
import logging
from celery import Celery

# to start the celery worker, run the command:
# "celery -A airflow.executors.celery_worker worker --loglevel=info"

app = Celery('airflow.executors.celery_worker', backend='amqp', broker='amqp://')

@app.task (name='airflow.executors.celery_worker.execute_command')
def execute_command(command):
    logging.info("Executing command in Celery " + command)
    try:
        subprocess.Popen(command, shell=True).wait()
    except Exception as e:
        raise e
    return True
