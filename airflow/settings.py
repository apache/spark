import os
import sys
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine


if 'AIRFLOW_HOME' not in os.environ:
    os.environ['AIRFLOW_HOME'] = os.path.join(os.path.dirname(__file__), "..")
AIRFLOW_HOME = os.environ['AIRFLOW_HOME']

BASE_FOLDER = AIRFLOW_HOME + '/airflow'
if BASE_FOLDER not in sys.path:
    sys.path.append(BASE_FOLDER)
DAGS_FOLDER = AIRFLOW_HOME + '/dags'
BASE_LOG_FOLDER = AIRFLOW_HOME + "/logs"
HIVE_HOME_PY = '/usr/lib/hive/lib/py'
RUN_AS_MASTER = True
JOB_HEARTBEAT_SEC = 5
ID_LEN = 250  # Used for dag_id and task_id VARCHAR length
LOG_FORMAT = \
    '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'

Session = sessionmaker()
engine = create_engine('mysql://airflow:airflow@localhost/airflow')
# engine = create_engine('sqlite:///' + BASE_FOLDER + '/airflow.db' )
Session.configure(bind=engine)
HEADER = """\
       .__         _____.__                 
_____  |__|_______/ ____\  |   ______  _  __
\__  \ |  \_  __ \   __\|  |  /  _ \ \/ \/ /
 / __ \|  ||  | \/|  |  |  |_(  <_> )     / 
(____  /__||__|   |__|  |____/\____/ \/\_/  
     \/"""
