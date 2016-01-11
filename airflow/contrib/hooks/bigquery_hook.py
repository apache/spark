import httplib2
import logging
import pandas
import time

from airflow.hooks.base_hook import BaseHook
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from pandas.io.gbq import GbqConnector, _parse_data as gbq_parse_data
from pandas.tools.merge import concat

logging.getLogger("bigquery").setLevel(logging.INFO)

BQ_SCOPE = 'https://www.googleapis.com/auth/bigquery'

class BigQueryHook(BaseHook):
    """
    Interact with BigQuery. Connections must be defined with an extras JSON field containing:

    {
        "project": "<google project ID>",
        "service_account": "<google service account email>",
        "key_path": "<p12 key path>"
    }
    """
    conn_name_attr = 'bigquery_conn_id'

    def __init__(self, bigquery_conn_id='bigquery_default'):
        self.bigquery_conn_id = bigquery_conn_id

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
        connector = BigQueryPandasConnector(project, service)
        schema, pages = connector.run_query(bql, verbose=False)
        dataframe_list = []

        while len(pages) > 0:
            page = pages.pop()
            dataframe_list.append(gbq_parse_data(schema, page))

        if len(dataframe_list) > 0:
            return concat(dataframe_list, ignore_index=True)
        else:
            return gbq_parse_data(schema, [])

    def run(self, bql, destination_dataset_table = False):
        """
        Executes a BigQuery SQL query. Either returns results and schema, or
        stores results in a BigQuery table, if destination is set.
        """
        connection_info = self.get_connection(self.bigquery_conn_id)
        connection_extras = connection_info.extra_dejson
        project = connection_extras['project']
        configuration = {
            'query': {
                'query': bql
            }
        }

        if destination_dataset_table:
            assert '.' in destination_dataset_table, \
                'Expected destination_dataset_table in the format of <dataset>.<table>. Got: {}'.format(destination_dataset_table)
            destination_dataset, destination_table = destination_dataset_table.split('.', 1)
            configuration['query']['destinationTable'] = {
                'projectId': project,
                'datasetId': destination_dataset,
                'tableId': destination_table,
            }

        return self.run_with_configuration(configuration)

    def run_with_configuration(self, configuration):
        """
        Executes a BigQuery SQL query.

        :param configuration: The configuration parameter maps directly to
        BigQuery's configuration field in the job object. See
        https://cloud.google.com/bigquery/docs/reference/v2/jobs for details.
        """
        service = self.get_conn()
        connection_info = self.get_connection(self.bigquery_conn_id)
        connection_extras = connection_info.extra_dejson
        project = connection_extras['project']
        job_collection = service.jobs()
        job_data = {
            'configuration': configuration
        }

        # Send query and wait for reply.
        query_reply = job_collection \
            .insert(projectId=project, body=job_data) \
            .execute()
        job_reference = query_reply['jobReference']

        # Wait for query to finish.
        while not query_reply.get('jobComplete', False):
            logging.info('Waiting for job to complete: %s', job_reference)
            time.sleep(5)
            query_reply = job_collection \
                .getQueryResults(projectId=job_reference['projectId'], jobId=job_reference['jobId']) \
                .execute()

        total_rows = int(query_reply['totalRows'])
        results = list()
        seen_page_tokens = set()
        current_row = 0
        schema = query_reply['schema']

        # Loop through each page of data, and stuff it into the results variable.
        while 'rows' in query_reply and current_row < total_rows:
            page = query_reply['rows']
            results.extend(page)
            current_row += len(page)
            page_token = query_reply.get('pageToken', None)

            if not page_token and current_row < total_rows:
                raise Exception('Required pageToken was missing. Received {0} of {1} rows.'.format(current_row, total_rows))
            elif page_token in seen_page_tokens:
                raise Exception('A duplicate pageToken was returned. This is unexpected.')

            seen_page_tokens.add(page_token)

            query_reply = job_collection \
                .getQueryResults(projectId=job_reference['projectId'], jobId=job_reference['jobId'], pageToken=page_token) \
                .execute()

        if current_row < total_rows:
            raise Exception('Didn\'t get complete result set. Something went wrong. Aborting.')

        return schema, results

class BigQueryPandasConnector(GbqConnector):
    """
    This connector behaves identically to GbqConnector (from Pandas), except
    that it allows the service to be injected, and disables a call to
    self.get_credentials(). This allows Airflow to use BigQuery with Pandas
    without forcing a three legged OAuth connection. Instead, we can inject
    service account credentials into the binding.
    """
    def __init__(self, project_id, service, reauth=False):
        self.test_google_api_imports()
        self.project_id = project_id
        self.reauth = reauth
        self.service = service
