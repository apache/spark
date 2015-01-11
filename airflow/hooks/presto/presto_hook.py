from airflow import settings
from airflow.configuration import conf
from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.presto.presto_client import PrestoClient

class PrestoException(Exception):
    pass

class PrestoHook(BaseHook):
    """
    Interact with Presto!
    """
    def __init__(
            self, presto_conn_id=conf.get('hooks', 'PRESTO_DEFAULT_CONN_ID')):
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
        self.port = db.port
        session.commit()
        session.close()

        self.client = PrestoClient(
            self.host, in_port=self.port, in_catalog=self.db,
            in_user='airflow')

    def get_records(self, hql, schema="default"):
        if self.client.runquery(hql, schema):
            return self.client.getdata()
        else:
            raise PrestoException(self.client.getlasterrormessage())

    def get_pandas_df(self, hql, schema="default"):
        import pandas
        client = self.client
        if client.runquery(hql, schema):
            data = client.getdata()
            if data:
                df = pandas.DataFrame(data)
                df.columns = [c['name'] for c in client.getcolumns()]
            else:
                df = pandas.DataFrame()
            return df
        else:
            raise PrestoException(self.client.getlasterrormessage())

    def run(self, hql, schema="default"):
        if not self.client.runquery(hql, schema):
            raise PrestoException(self.client.getlasterrormessage())
