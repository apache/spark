import MySQLdb

from airflow.hooks.dbapi_hook import DbApiHook


class MySqlHook(DbApiHook):
    '''
    Interact with MySQL.
    '''

    conn_name_attr = 'mysql_conn_id'
    default_conn_name = 'mysql_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns a mysql connection object
        """
        conn = self.get_connection(self.mysql_conn_id)
        conn = MySQLdb.connect(
            conn.host,
            conn.login,
            conn.password,
            conn.schema)
        return conn
