from datetime import datetime
import numpy
import logging

from airflow.hooks.base_hook import BaseHook
from airflow.utils import AirflowException


class DbApiHook(BaseHook):
    """
    Abstract base class for sql hooks.
    """

    """
    Override to provide the connection name.
    """
    conn_name_attr = None

    """
    Override to have a default connection id for a particular dbHook
    """
    default_conn_name = 'default_conn_id'

    """
    Override if this db supports autocommit.
    """
    supports_autocommit = False

    def __init__(self, **kwargs):
        try:
            self.conn_id_name = kwargs[self.conn_name_attr]
        except NameError:
            raise AirflowException("conn_name_attr is not defined")
        except KeyError:
            raise AirflowException(
                self.conn_name_attr + " was not passed in the kwargs")

    def get_pandas_df(self, sql, parameters=None):
        '''
        Executes the sql and returns a pandas dataframe
        '''
        import pandas.io.sql as psql
        conn = self.get_conn()
        df = psql.read_sql(sql, con=conn)
        conn.close()
        return df

    def get_records(self, sql, parameters=None):
        '''
        Executes the sql and returns a set of records.
        '''
        conn = self.get_conn()
        cur = self.get_cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def get_first(self, sql, parameters=None):
        '''
        Executes the sql and returns a set of records.
        '''
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchone()
        cur.close()
        conn.close()
        return rows

    def run(self, sql, autocommit=False, parameters=None):
        """
        Runs a command
        """
        conn = self.get_conn()
        if self.supports_autocommit:
            conn.autocommit = autocommit
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

    def get_cursor(self):
        """Returns a cursor"""
        return self.get_conn().cursor()

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction
        """
        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = "({})".format(target_fields)
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
            sql = "INSERT INTO {0} {1} VALUES ({2});".format(
                table,
                target_fields,
                ",".join(values))
            cur.execute(sql)
            if i % commit_every == 0:
                conn.commit()
                logging.info(
                    "Loaded {i} into {table} rows so far".format(**locals()))
        conn.commit()
        cur.close()
        conn.close()
        logging.info(
            "Done loading. Loaded a total of {i} rows".format(**locals()))

    def get_conn(self):
        """
        Retuns a sql connection that can be used to retrieve a cursor.
        """
        raise NotImplemented()
