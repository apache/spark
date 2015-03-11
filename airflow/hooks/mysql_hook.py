import MySQLdb

from airflow.hooks.base_hook import BaseHook


class MySqlHook(BaseHook):
    '''
    Interact with MySQL.
    '''

    def __init__(
            self, mysql_conn_id=None):

        conn = self.get_connection(mysql_conn_id)
        self.conn = conn

    def get_conn(self):
        conn = MySQLdb.connect(
            self.conn.host,
            self.conn.login,
            self.conn.password,
            self.conn.schema)
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

    def run(self, sql):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
