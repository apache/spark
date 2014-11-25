import sys

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from airflow.configuration import getconf


HEADER = """\
       .__         _____.__
_____  |__|_______/ ____\  |   ______  _  __
\__  \ |  \_  __ \   __\|  |  /  _ \ \/ \/ /
 / __ \|  ||  | \/|  |  |  |_(  <_> )     /
(____  /__||__|   |__|  |____/\____/ \/\_/
     \/"""

BASE_FOLDER = getconf().get('core', 'BASE_FOLDER')
SQL_ALCHEMY_CONN = getconf().get('core', 'SQL_ALCHEMY_CONN')
if BASE_FOLDER not in sys.path:
    sys.path.append(BASE_FOLDER)

Session = sessionmaker()
engine = create_engine(
    SQL_ALCHEMY_CONN, pool_size=50)
Session.configure(bind=engine)

# can't move this to configuration due to ConfigParser interpolation
LOG_FORMAT =  \
    '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'

