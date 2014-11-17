import os
import sys
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from configuration import getconf

"""
if 'AIRFLOW_HOME' not in os.environ:
    os.environ['AIRFLOW_HOME'] = os.path.join(os.path.dirname(__file__), "..")
AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
"""

BASE_FOLDER = getconf().get('core', 'BASE_FOLDER')
SQL_ALCHEMY_CONN = getconf().get('core', 'SQL_ALCHEMY_CONN')
if BASE_FOLDER not in sys.path:
    sys.path.append(BASE_FOLDER)

Session = sessionmaker()
#engine = create_engine('mysql://airflow:airflow@localhost/airflow')
engine = create_engine(SQL_ALCHEMY_CONN)
Session.configure(bind=engine)

# can't move this to configuration due to ConfigParser interpolation
LOG_FORMAT =  \
    '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'

HEADER = """\
       .__         _____.__
_____  |__|_______/ ____\  |   ______  _  __
\__  \ |  \_  __ \   __\|  |  /  _ \ \/ \/ /
 / __ \|  ||  | \/|  |  |  |_(  <_> )     /
(____  /__||__|   |__|  |____/\____/ \/\_/
     \/"""
