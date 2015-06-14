import psycopg2

from airflow import settings
from airflow.utils import AirflowException
from airflow.models import Connection


class PostgresHook(object):
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

    def get_records(self, sql):
        '''
        Executes the sql and returns a set of records.
        '''
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def get_pandas_df(self, sql):
        '''
        Executes the sql and returns a pandas dataframe
        '''
        import pandas.io.sql as psql
        conn = self.get_conn()
        df = psql.read_sql(sql, con=conn)
        conn.close()
        return df

    def run(self, sql, autocommit=False):
        conn = self.get_conn()
        conn.autocommit = autocommit
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
