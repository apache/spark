import os
import sys
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

if 'FLUX_HOME' not in os.environ:
    os.environ['FLUX_HOME'] = os.path.join(os.path.dirname(__file__), "..")
FLUX_HOME = os.environ['FLUX_HOME']

BASE_FOLDER = FLUX_HOME + '/flux'
if BASE_FOLDER not in sys.path:
    sys.path.append(BASE_FOLDER)
DAGS_FOLDER = FLUX_HOME + '/dags'
BASE_LOG_FOLDER = FLUX_HOME + "/logs"
RUN_AS_MASTER = True
JOB_HEARTBEAT_SEC = 5
ID_LEN = 250  # Used for dag_id and task_id VARCHAR length
LOG_FORMAT = \
    '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
Session = sessionmaker()
engine = create_engine('mysql://flux:flux@localhost/flux')
# engine = create_engine('sqlite:///' + BASE_FOLDER + '/flux.db' )
Session.configure(bind=engine)
HEADER = """\
  _____ __
_/ ____\  |  __ _____  ___
\   __\|  | |  |  \  \/  /
 |  |  |  |_|  |  />    <
 |__|  |____/____//__/\_ \\
                        \/"""
