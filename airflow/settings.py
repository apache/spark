import sys

from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine

from airflow.configuration import conf

HEADER = """\
       .__         _____.__
_____  |__|_______/ ____\  |   ______  _  __
\__  \ |  \_  __ \   __\|  |  /  _ \ \/ \/ /
 / __ \|  ||  | \/|  |  |  |_(  <_> )     /
(____  /__||__|   |__|  |____/\____/ \/\_/
     \/"""


AIRFLOW_HOME = conf.get('core', 'AIRFLOW_HOME')
BASE_FOLDER = conf.get('core', 'BASE_FOLDER')
BASE_LOG_URL = "/admin/airflow/log"
SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN')
print "-"* 100
print(SQL_ALCHEMY_CONN)
print "-"* 100
if BASE_FOLDER not in sys.path:
    sys.path.append(BASE_FOLDER)

engine_args = {}
if 'sqlite' not in SQL_ALCHEMY_CONN:
    # Engine args not supported by sqllite
    engine_args['pool_size'] = 50
    engine_args['pool_recycle'] = 3600

engine = create_engine(
    SQL_ALCHEMY_CONN, **engine_args)
Session = scoped_session(sessionmaker(autocommit=False,
        autoflush=False,
        bind=engine))

# can't move this to configuration due to ConfigParser interpolation
LOG_FORMAT =  \
    '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
SIMPLE_LOG_FORMAT = \
    '%(asctime)s %(levelname)s - %(message)s'
