from ConfigParser import ConfigParser
import errno
import logging
import os

DEFAULT_CONFIG = """\
[core]
airflow_home = {AIRFLOW_HOME}
authenticate = False
dags_folder = {AIRFLOW_HOME}/dags
base_folder = {AIRFLOW_HOME}/airflow
base_url = http://localhost:8080
executor = LocalExecutor
sql_alchemy_conn = sqlite:///{AIRFLOW_HOME}/airflow.db

[server]
web_server_host = 0.0.0.0
web_server_port = 8080

[smtp]
smtp_host = localhost
smtp_user = airflow
smtp_port = 25
smtp_password = airflow
smtp_mail_from = airflow@airflow.com

[celery]
celery_app_name = airflow.executors.celery_executor
celeryd_concurrency = 16
worker_log_server_port = 8793
broker_url = sqla+mysql://airflow:airflow@localhost:3306/airflow
celery_result_backend = db+mysql://airflow:airflow@localhost:3306/airflow

[hooks]
presto_default_conn_id = presto_default
hive_default_conn_id = hive_default

[misc]
job_heartbeat_sec = 5
id_len = 250
"""

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

'''
Setting AIRFLOW_HOME and AIRFLOW_CONFIG from environment variables, using
"~/airflow" and "~/airflow/airflow.cfg" repectively as defaults.
'''

if 'AIRFLOW_HOME' not in os.environ:
    AIRFLOW_HOME = os.path.expanduser('~/airflow')
else:
    AIRFLOW_HOME = os.environ['AIRFLOW_HOME']

mkdir_p(AIRFLOW_HOME)

if 'AIRFLOW_CONFIG' not in os.environ:
    AIRFLOW_CONFIG = AIRFLOW_HOME + '/airflow.cfg'
else:
    AIRFLOW_CONFIG = os.environ['AIRFLOW_CONFIG']

conf = ConfigParser()
if not os.path.isfile(AIRFLOW_CONFIG):
    '''
    These configuration are used to generate a default configuration when
    it is missing. The right way to change your configuration is to alter your
    configuration file, not this code.
    '''
    logging.info("Createing new config file in: " + AIRFLOW_CONFIG)
    f = open(AIRFLOW_CONFIG, 'w')
    f.write(DEFAULT_CONFIG.format(**locals()))
    f.close()

logging.info("Reading the config from " + AIRFLOW_CONFIG)
conf.read(AIRFLOW_CONFIG)
