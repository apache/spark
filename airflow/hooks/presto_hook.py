from pyhive import presto
from pyhive.exc import DatabaseError

from airflow.hooks.base_hook import BaseHook

import logging
logging.getLogger("pyhive").setLevel(logging.INFO)


class PrestoException(Exception):
    pass


class PrestoHook(BaseHook):
    """
    Interact with Presto through PyHive!

    >>> ph = PrestoHook()
    >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
    >>> ph.get_records(sql)
    [[340698]]
    """
    def __init__(self, presto_conn_id='presto_default'):
        self.presto_conn_id = presto_conn_id
    def get_conn(self):
        """Returns a connection object"""
        db = self.get_connection(self.presto_conn_id)
        return presto.connect(
            host=db.host,
            port=db.port,
            username=db.login,
            catalog=db.extra_dejson.get('catalog', 'hive'),
            schema=db.schema)

    def get_cursor(self):
        """Returns a cursor"""
        return self.get_conn().cursor()

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    def get_records(self, hql, parameters=None):
        """
        Get a set of records from Presto
        """
        cursor = self.get_cursor()
        try:
            cursor.execute(self._strip_sql(hql), parameters)
            records = cursor.fetchall()
        except DatabaseError as e:
            obj = eval(str(e))
            raise PrestoException(obj['message'])
        return records

    def get_first(self, hql, parameters=None):
        """
        Returns only the first row, regardless of how many rows the query
        returns.
        """
        cursor = self.get_cursor()
        try:
            cursor.execute(self._strip_sql(hql), parameters)
            record = cursor.fetchone()
        except DatabaseError as e:
            obj = eval(str(e))
            raise PrestoException(obj['message'])
        return record

    def get_pandas_df(self, hql, parameters=None):
        """
        Get a pandas dataframe from a sql query.
        """
        import pandas
        cursor = self.get_cursor()
        cursor.execute(self._strip_sql(hql), parameters)
        try:
            data = cursor.fetchall()
        except DatabaseError as e:
            obj = eval(str(e))
            raise PrestoException(obj['message'])
        column_descriptions = cursor.description
        if data:
            df = pandas.DataFrame(data)
            df.columns = [c[0] for c in column_descriptions]
        else:
            df = pandas.DataFrame()
        return df

    def run(self, hql, parameters=None):
        """
        Execute the statement against Presto. Can be used to create views.
        """
        cursor = self.get_cursor()
        cursor.execute(self._strip_sql(hql), parameters)
