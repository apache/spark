import logging

import sqlite3

from airflow.hooks.base_hook import BaseHook


class SqliteHook(BaseHook):

    """
    Interact with SQLite.
    """

    def __init__(
            self, sqlite_conn_id='sqlite_default'):
        self.sqlite_conn_id = sqlite_conn_id

    def get_conn(self):
        """
        Returns a sqlite connection object
        """
        conn = self.get_connection(self.sqlite_conn_id)
        conn = sqlite3.connect(conn.host)
        return conn

    def run(self, sql):
        """
        Runs a command

        >>> h = SqliteHook()
        >>> sql = "CREATE TABLE IF NOT EXISTS test_table (i INTEGER);"
        >>> h.run(sql)
        """
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

    def insert_rows(self, table, rows, target_fields=None):
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction

        >>> h = SqliteHook()
        >>> h.insert_rows('test_table', [[1]])
        """
        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = "({})".format(target_fields)
        else:
            target_fields = ''
        conn = self.get_conn()
        cur = conn.cursor()
        i = 0
        for row in rows:
            i += 1
            l = []
            for cell in row:
                if isinstance(cell, basestring):
                    l.append("'" + str(cell).replace("'", "''") + "'")
                elif cell is None:
                    l.append('NULL')
                else:
                    l.append(str(cell))
            values = tuple(l)
            sql = "INSERT INTO {0} {1} VALUES ({2});".format(
                table,
                target_fields,
                ",".join(values))
            cur.execute(sql)
            conn.commit()
        conn.commit()
        cur.close()
        conn.close()
        logging.info(
            "Done loading. Loaded a total of {i} rows".format(**locals()))

    def get_records(self, sql):
        """
        Executes the sql and returns a set of records.

        >>> h = SqliteHook()
        >>> sql = "SELECT * FROM test_table WHERE i=1 LIMIT 1;"
        >>> h.get_records(sql)
        [(1,)]
        """
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def get_pandas_df(self, sql):
        """
        Executes the sql and returns a pandas dataframe
        >>> h = SqliteHook()
        >>> sql = "SELECT * FROM test_table WHERE i=1 LIMIT 1;"
        >>> h.get_pandas_df(sql)
           i
        0  1
        """
        import pandas.io.sql as psql
        conn = self.get_conn()
        df = psql.read_sql(sql, con=conn)
        conn.close()
        return df
