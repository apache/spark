import psycopg2

from airflow.hooks.dbapi_hook import DbApiHook


class PostgresHook(DbApiHook):
    '''
    Interact with Postgres.
    '''

    conn_name_attr = 'postgres_conn_id'
    default_conn_name = 'postgres_default'
    supports_autocommit = True

    def get_conn(self):
        conn = self.get_connection(self.conn_id_name)
        return psycopg2.connect(
            host=conn.host,
            user=conn.login,
            password=conn.psw,
            dbname=conn.db,
            port=conn.port)
