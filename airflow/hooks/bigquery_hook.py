import httplib2
import logging
import pandas

from airflow.hooks.dbapi_hook import DbApiHook
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials

logging.getLogger("bigquery").setLevel(logging.INFO)

BQ_SCOPE = 'https://www.googleapis.com/auth/bigquery'

class BigQueryHook(DbApiHook):
    """
    Interact with BigQuery. Connections must be defined with an extras JSON field containing:

    {
        "project": "<google project ID>",
        "service_account": "<google service account email>",
        "key_path": "<p12 key path>"
    }
    """

    conn_name_attr = 'bigquery_conn_id'
    default_conn_name = 'bigquery_default'

    def get_conn(self):
        """
        Returns a BigQuery service object.
        """
        connection_info = self.get_connection(self.bigquery_conn_id)
        connection_extras = connection_info.extra_dejson
        service_account = connection_extras['service_account']
        key_path = connection_extras['key_path']

        with file(key_path, 'rb') as key_file:
            key = key_file.read()

        credentials = SignedJwtAssertionCredentials(
            service_account,
            key,
            scope=BQ_SCOPE)
            # TODO Support domain delegation, which will allow us to set a sub-account to execute as. We can then
            # pass DAG owner emails into the connection_info, and use it here.
            # sub='some@email.com')

        http = httplib2.Http()
        http_authorized = credentials.authorize(http)
        service = build('bigquery', 'v2', http=http_authorized)

        return service

    def get_pandas_df(self, bql, parameters=None):
        """
        Returns a Pandas DataFrame for the results produced by a BigQuery query.
        """
        service = self.get_conn()
        connection_info = self.get_connection(self.bigquery_conn_id)
        connection_extras = connection_info.extra_dejson
        project = connection_extras['project']
        response = service.jobs().query(projectId=project, body={
            "query": bql,
        }).execute()

        if len(response['rows']) > 0:
            # Extract column names from response.
            columns = [c['name'] for c in response['schema']['fields']]

            # Extract data into a two-dimensional list of values.
            data = []
            for row in response['rows']:
                row = map(lambda kv: kv['v'], row['f'])
                data.append(row)

            return pandas.DataFrame(data, columns = columns)
        else:
            return pandas.DataFrame()
