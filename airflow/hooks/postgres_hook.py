import psycopg2

from airflow import settings
from airflow.utils import AirflowException
from airflow.models import Connection

from airflow.hooks.base_hook import BaseSqlHook


class PostgresHook(BaseSqlHook):
    '''
    Interact with Postgres.
    '''

    def __init__(
            self, host=None, login=None,
            psw=None, db=None, port=None, postgres_conn_id=None):
        if not postgres_conn_id:
            self.host = host
            self.login = login
            self.psw = psw
            self.db = db
            self.port = port
        else:
            session = settings.Session()
            db = session.query(
                Connection).filter(
                    Connection.conn_id == postgres_conn_id)
            if db.count() == 0:
                raise AirflowException("The postgres_dbid you provided isn't defined")
            else:
                db = db.all()[0]
            self.host = db.host
            self.login = db.login
            self.psw = db.password
            self.db = db.schema
            self.port = db.port
            session.commit()
            session.close()

    def get_conn(self):
        conn = psycopg2.connect(
            host=self.host,
            user=self.login,
            password=self.psw,
            dbname=self.db,
            port=self.port)
        return conn

    def run(self, sql, autocommit=False):
        conn = self.get_conn()
        conn.autocommit = autocommit
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
