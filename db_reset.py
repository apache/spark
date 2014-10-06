from core import models
from core import settings
from core.hooks import MySqlHook
import logging

if __name__ == '__main__':
    if 
    session = settings.Session()
    session.query(models.DatabaseConnection).delete()
    mysqldb = models.DatabaseConnection(
            db_id='local_mysql', db_type='mysql', 
            host='localhost', login='flux', password='flux',
            schema='flux')
    session.add(mysqldb)
    session.commit()

    logging.info("Dropping all tables")
    mysql_hook = MySqlHook(mysql_dbid='local_mysql')
    mysql_hook.run("DROP TABLE IF EXISTS task;")
    mysql_hook.run("DROP TABLE IF EXISTS job;")
    mysql_hook.run("DROP TABLE IF EXISTS task_instance;")
    mysql_hook.run("DROP TABLE IF EXISTS user;")
    mysql_hook.run("DROP TABLE IF EXISTS log;")
    mysql_hook.run("DROP TABLE IF EXISTS dag;")

    session.close()
