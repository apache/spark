import cx_Oracle
import logging

from airflow.hooks.dbapi_hook import DbApiHook

class OracleHook(DbApiHook):
    """
    Interact with Oracle SQL Server.
    """
    conn_name_attr = 'oracle_conn_id'
    default_conn_name = 'oracle_default'
    supports_autocommit = False

    def get_conn(self):
        """
        Returns a oracle connection object
        """
        conn = self.get_connection(self.oracle_conn_id)
        try:
            conn = cx_Oracle.connect(conn.login, conn.password, conn.host)
        except:
            # add support for custom DSN connections
            dsn = conn.extra_dejson.get('dsn', None)
            service_name = conn.extra_dejson.get('service_name', None)
            dsn = cx_Oracle.makedsn(dsn, conn.port, service_name=service_name)
            conn = cx_Oracle.connect(conn.login, conn.password, dsn=dsn)
        return conn

    def get_pandas_df(self, sql, parameters = None):
        """
        Executes the sql and returns a pandas dataframe
        """
        import pandas.io.sql as psql
        conn = self.get_conn()
        df = psql.read_sql(sql, con=conn)
        df.columns = df.columns.map(lambda x: x.lower()) # lower column names to have same output as other hooks
        conn.close()
        return df

    def insert_rows(self, table, rows, target_fields = None, commit_every = 1000):
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction
        """
        if target_fields:
            target_fields = ', '.join(target_fields)
            target_fields = '({})'.format(target_fields)
        else:
            target_fields = ''
        conn = self.get_conn()
        cur = conn.cursor()
        if self.supports_autocommit:
            cur.execute('SET autocommit = 0')
        conn.commit()
        i = 0
        for row in rows:
            i += 1
            l = []
            for cell in row:
                if isinstance(cell, basestring):
                    l.append("'" + str(cell).replace("'", "''") + "'")
                elif cell is None:
                    l.append('NULL')
                elif isinstance(cell, numpy.datetime64):
                    l.append("'" + str(cell) + "'")
                elif isinstance(cell, datetime):
                    l.append("'" + cell.isoformat() + "'")
                else:
                    l.append(str(cell))
            values = tuple(l)
            sql = 'INSERT /*+ APPEND */ INTO {0} {1} VALUES ({2})'.format(table, target_fields, ','.join(values))
            cur.execute(sql)
            if i % commit_every == 0:
                conn.commit()
                logging.info('Loaded {i} into {table} rows so far'.format(**locals()))
        conn.commit()
        cur.close()
        conn.close()
        logging.info('Done loading. Loaded a total of {i} rows'.format(**locals()))
