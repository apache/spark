__author__ = 'janomar'

import logging
import jaydebeapi

from airflow.hooks.dbapi_hook import DbApiHook

class JdbcHook(DbApiHook):
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


    conn_name_attr = 'jdbc_conn_id'
    default_conn_name = 'jdbc_default'
    supports_autocommit = True

    def __init__(
            self, *args, **kwargs):

        super(JdbcHook,self).__init__(*args,**kwargs)

        #conn_id = getattr(self, self.conn_name_attr)
        #if (conn_id is None):
        #    self.host = host
        #    self.login = login
        #    self.psw = psw
        #    #self.db = db
        #    #self.port = port
        #    self.extra = extra
        #    self.jdbc_driver_loc = jdbc_driver_loc
        #   self.jdbc_driver_name = jdbc_driver_name
        #else:
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        self.host = conn.host
        self.login = conn.login
        self.psw = conn.password
        #self.db = conn.schema
        #self.port = conn.port
        self.extra = conn.extra
        self.jdbc_driver_loc = conn.extra_dejson.get('jdbc_drv_path')
        self.jdbc_driver_name = conn.extra_dejson.get('jdbc_drv_clsname')


        #self.jdbc_url = jdbc_url.format(self.host, self.port, self.db, self.extra)

    def get_conn(self):
        conn = jaydebeapi.connect(self.jdbc_driver_name,
                           [str(self.host), str(self.login), str(self.psw)],
                                  self.jdbc_driver_loc,)
        return conn

    def run(self, sql, autocommit=False, parameters=None):
        """
        Runs a command
        """
        conn = self.get_conn()
        if self.supports_autocommit:
            conn.jconn.autocommit = autocommit
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()