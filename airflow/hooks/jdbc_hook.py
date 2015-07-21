__author__ = 'janomar'

import logging
import jaydebeapi

from airflow.hooks.base_hook import BaseHook

class JdbcHook(BaseHook):
    """
    General hook for jdbc db access.

    If a connection id is specified, host, port, schema, username and password will be taken from the predefined connection.
    Raises an airflow error if the given connection id doesn't exist.
    Otherwise host, port, schema, username and password can be specified on the fly.

    :param jdbc_url: jdbc connection url
    :type jdbc_url: string
    :param jdbc_driver_name: jdbc driver name
    :type jdbc_driver_name: string
    :param jdbc_driver_loc: path to jdbc driver
    :type jdbc_driver_loc: string
    :param conn_id: reference to a predefined database
    :type conn_id: string
    :param sql: the sql code to be executed
    :type sql: string or string pointing to a template file. File must have
        a '.sql' extensions.
    """
    def __init__(
            self, jdbc_url, jdbc_driver_name, jdbc_driver_loc,host=None, login=None,
            psw=None, db=None, port=None, extra=None, conn_id=None):
        self.jdbc_driver_loc = jdbc_driver_loc
        self.jdbc_driver_name = jdbc_driver_name

        if (conn_id is None):
            self.host = host
            self.login = login
            self.psw = psw
            self.db = db
            self.port = port
            self.extra = extra
        else:
            conn = self.get_connection(conn_id)
            self.host = conn.host
            self.login = conn.login
            self.psw = conn.password
            self.db = conn.schema
            self.port = conn.port
            self.extra = conn.extra


        self.jdbc_url = jdbc_url.format(self.host, self.port, self.db, self.extra)

    def get_conn(self):
        logging.info("Trying to connect using jdbc url: " + self.jdbc_url)
        conn = jaydebeapi.connect(self.jdbc_driver_name,
                           [str(self.jdbc_url), str(self.login), str(self.psw)],
                                  self.jdbc_driver_loc,)
        return conn

    def get_records(self, sql, autocommit=False):
        '''
        Executes the sql and returns a set of records.
        '''
        conn = self.get_conn()
        conn.jconn.autocommit = autocommit
        cur = conn.cursor()
        cur.execute(sql)
        rows = [] if not cur._rs else cur.fetchall()
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
        conn.jconn.autocommit = autocommit
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()