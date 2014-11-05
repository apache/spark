import sys

from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy.pool import Pool

from airflow.configuration import getconf

HEADER = """\
       .__         _____.__
_____  |__|_______/ ____\  |   ______  _  __
\__  \ |  \_  __ \   __\|  |  /  _ \ \/ \/ /
 / __ \|  ||  | \/|  |  |  |_(  <_> )     /
(____  /__||__|   |__|  |____/\____/ \/\_/
     \/"""

def pessimistic_connection_handling():
    @event.listens_for(Pool, "checkout")
    def ping_connection(dbapi_connection, connection_record, connection_proxy):
        '''
        Disconnect Handling - Pessimistic, taken from:
        http://docs.sqlalchemy.org/en/rel_0_9/core/pooling.html
        '''
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SELECT 1")
        except:
            raise exc.DisconnectionError()
        cursor.close()


BASE_FOLDER = getconf().get('core', 'BASE_FOLDER')
BASE_LOG_URL = "/admin/airflow/log"
SQL_ALCHEMY_CONN = getconf().get('core', 'SQL_ALCHEMY_CONN')
if BASE_FOLDER not in sys.path:
    sys.path.append(BASE_FOLDER)

engine = create_engine(
    SQL_ALCHEMY_CONN, pool_size=50, pool_recycle=3600)
Session = scoped_session(sessionmaker(autocommit=False,
        autoflush=False,
        bind=engine))

# can't move this to configuration due to ConfigParser interpolation
LOG_FORMAT =  \
    '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
SIMPLE_LOG_FORMAT = \
    '%(asctime)s %(levelname)s - %(message)s'
