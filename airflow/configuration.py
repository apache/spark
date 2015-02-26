from ConfigParser import ConfigParser
import errno
import logging
import os

defaults = {
    'misc': {
        'statsd_on': False,
        'statsd_host': 'localhost',
        'statsd_port': 8125,
    },
}

DEFAULT_CONFIG = """\
[core]
airflow_home = {AIRFLOW_HOME}
authenticate = False
dags_folder = {AIRFLOW_HOME}/dags
base_log_folder = {AIRFLOW_HOME}/logs
base_url = http://localhost:8080
executor = SequentialExecutor
sql_alchemy_conn = sqlite:///{AIRFLOW_HOME}/airflow.db
unit_test_mode = False

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
flower_port = 5555

[misc]
job_heartbeat_sec = 5
master_heartbeat_sec = 60
id_len = 250
statsd_on = false
statsd_host = localhost
statsd_port = 8125
"""

TEST_CONFIG = """\
[core]
airflow_home = {AIRFLOW_HOME}
authenticate = False
dags_folder = {AIRFLOW_HOME}/dags
base_log_folder = {AIRFLOW_HOME}/logs
base_url = http://localhost:8080
executor = SequentialExecutor
sql_alchemy_conn = sqlite:///{AIRFLOW_HOME}/unittests.db
unit_test_mode = True

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
flower_port = 5555

[misc]
job_heartbeat_sec = 1
master_heartbeat_sec = 30
id_len = 250

[statsd]
statsd_on = false
statsd_host = localhost
statsd_port = 8125
"""


class ConfigParserWithDefaults(ConfigParser):

    def __init__(self, defaults, *args, **kwargs):
        self.defaults = defaults
        ConfigParser.__init__(self, *args, **kwargs)

    def get(self, section, key):
        d = self.defaults
        try:
            return ConfigParser.get(self, section, key)
        except:
            if section not in d or key not in d[section]:
                raise Exception("section/key not found in config")
            else:
                return d[section][key]


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise Exception('Had trouble creating a directory')

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

TEST_CONFIG_FILE = AIRFLOW_HOME + '/unittests.cfg'
if not os.path.isfile(TEST_CONFIG_FILE):
    logging.info("Createing new config file in: " + TEST_CONFIG_FILE)
    f = open(TEST_CONFIG_FILE, 'w')
    f.write(TEST_CONFIG.format(**locals()))
    f.close()

logging.info("Reading the config from " + AIRFLOW_CONFIG)


def test_mode():
    conf = ConfigParserWithDefaults(defaults)
    conf.read(TEST_CONFIG)

conf = ConfigParserWithDefaults(defaults)
conf.read(AIRFLOW_CONFIG)
