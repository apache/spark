from airflow import settings
from airflow.configuration import conf
from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook
from pyhive import presto

import logging
logging.getLogger("pyhive").setLevel(logging.INFO)


class PrestoHook(BaseHook):
    """
    Interact with Presto through PyHive!
    """
    def __init__(self, host=None, db=None, port=None,
                 presto_conn_id=conf.get('hooks', 'PRESTO_DEFAULT_CONN_ID')):
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
        '''
        Returns a cursor.
        '''
        return self.cursor

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    def get_records(self, hql, parameters=None):
        '''
        Get a set of records from Presto
        '''
        self.cursor.execute(self._strip_sql(hql), parameters)
        return self.cursor.fetchall()

    def get_first(self, hql, parameters=None):
        '''
        Returns only the first row, regardless of how many rows the query
        returns.
        '''
        self.cursor.execute(self._strip_sql(hql), parameters)
        return self.cursor.fetchone()

    def get_pandas_df(self, hql, parameters=None):
        '''
        Get a pandas dataframe from a sql query.
        '''
        import pandas
        cursor = self.get_cursor()
        cursor.execute(self._strip_sql(hql), parameters)
        data = cursor.fetchall()
        column_descriptions = cursor.description
        if data:
            df = pandas.DataFrame(data)
            df.columns = [c[0] for c in column_descriptions]
        else:
            df = pandas.DataFrame()
        return df

    def run(self, hql, parameters=None):
        '''
        Execute the statement against Presto. Can be used to create views.
        '''
        self.cursor.execute(self._strip_sql(hql), parameters)
