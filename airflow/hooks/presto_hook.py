from airflow import settings
from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook
from pyhive import presto
from pyhive.exc import DatabaseError

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
    def __init__(self, host=None, db=None, port=None,
                 presto_conn_id='presto_default'):
        self.user = 'airflow'
        if not presto_conn_id:
            self.host = host
            self.db = db
            self.port = port
        else:
            session = settings.Session()
            db = session.query(
                Connection).filter(
                    Connection.conn_id == presto_conn_id)
            if db.count() == 0:
                raise Exception("The presto_conn_id you provided isn't defined")
            else:
                db = db.all()[0]
            self.host = db.host
            self.db = db.schema
            self.catalog = 'hive'
            self.port = db.port
            self.cursor = presto.connect(host=db.host, port=db.port,
                                         username=self.user,
                                         catalog=self.catalog,
                                         schema=db.schema).cursor()
            session.close()    # currently only a pass in pyhive

    def get_cursor(self):
        """

        Returns a cursor.
        """

        return self.cursor

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    def get_records(self, hql, parameters=None):
        """

        Get a set of records from Presto
        """

        try:
            self.cursor.execute(self._strip_sql(hql), parameters)
            records = self.cursor.fetchall()
        except DatabaseError as e:
            obj = eval(str(e))
            raise PrestoException(obj['message'])
        return records

    def get_first(self, hql, parameters=None):
        """

        Returns only the first row, regardless of how many rows the query
        returns.
        """

        try:
            self.cursor.execute(self._strip_sql(hql), parameters)
            record = self.cursor.fetchone()
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

        self.cursor.execute(self._strip_sql(hql), parameters)
