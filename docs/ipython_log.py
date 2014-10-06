# IPython log file

from core.hooks import MySqlHook
mysql_hook = MySqlHook(mysql_dbid='local_mysql')
sql = """
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'flux'
"""
mysql_hook.get_pandas_df(sql)
from core.operators import MySqlOperator
from datetime import datetime, timedelta

sql = """
INSERT INTO tmp
SELECT 1;
"""
mysql_op = MySqlOperator(task_id='test_task3', sql=sql, mysql_dbid='local_mysql', owner='max')
mysql_op.run(start_date=datetime(2014, 9, 15))
from core.operators import MySqlOperator
from datetime import datetime, timedelta

sql = """
INSERT INTO tmp
SELECT 1;
"""
mysql_op = MySqlOperator(task_id='test_task3', sql=sql, mysql_dbid='local_mysql', owner='max')
mysql_op.run(start_date=datetime(2014, 9, 15))
from core.operators import MySqlOperator
from datetime import datetime, timedelta

sql = """
INSERT INTO tmp
SELECT 1;
"""
mysql_op = MySqlOperator(task_id='test_task3', sql=sql, mysql_dbid='local_mysql', owner='max')
mysql_op.run(start_date=datetime(2014, 9, 15))
from core.operators import MySqlOperator
from datetime import datetime, timedelta

sql = """
INSERT INTO tmp
SELECT 1;
"""
mysql_op = MySqlOperator(task_id='test_task3', sql=sql, mysql_dbid='local_mysql', owner='max')
mysql_op.run(start_date=datetime(2014, 9, 15))
import logging
import logging
logging.info("tet")
import logging
logging.info("tet")
import logging
logging.info("test")
import logging
logging.basicConfig()
logging.info("test")
import logging
logging.basicConfig()
logging.info("test")
print "hello"
import logging
get_ipython().magic(u'pinfo logging.basicConfig')
logging.info("test")
print "hello"
import logging
logging.basicConfig(level=logging.DEBUG)
logging.info("test")
print "hello"
import logging
logging.basicConfig(filename=None, level=logging.DEBUG)
logging.info("test")
print "hello"
import logging
get_ipython().magic(u'logstart')
logging.basicConfig(filename=None, level=logging.DEBUG)
logging.info("test")
print "hello"
