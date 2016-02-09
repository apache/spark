import httplib2
import logging
import pandas
import time

from airflow.contrib.hooks.gc_base_hook import GoogleCloudBaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from apiclient.discovery import build
from pandas.io.gbq import GbqConnector, _parse_data as gbq_parse_data
from pandas.tools.merge import concat

logging.getLogger("bigquery").setLevel(logging.INFO)

class BigQueryHook(GoogleCloudBaseHook, DbApiHook):
    """
    Interact with BigQuery. Connections must be defined with an extras JSON 
    field containing:

    {
        "project": "<google project ID>",
        "service_account": "<google service account email>",
        "key_path": "<p12 key path>"
    }

    If you have used ``gcloud auth`` to authenticate on the machine that's 
    running Airflow, you can exclude the service_account and key_path 
    parameters.
    """
    conn_name_attr = 'bigquery_conn_id'

    def __init__(self,
                 scope='https://www.googleapis.com/auth/bigquery',
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None):
        """
        :param scope: The scope of the hook.
        :type scope: string
        """
        super(BigQueryHook, self).__init__(
            scope=scope,
            conn_id=bigquery_conn_id,
            delegate_to=delegate_to)

    def get_conn(self):
        """
        Returns a BigQuery service object.
        """
        service = self.get_service()
        connection_extras = self._extras_dejson()
        project = connection_extras['project']
        return BigQueryConnection(service=service, project_id=project)

    def get_service(self):
        """
        Returns a BigQuery service object.
        """
        http_authorized = self._authorize()
        return build('bigquery', 'v2', http=http_authorized)

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        raise NotImplementedError()

class BigQueryConnection(object):
    """
    BigQuery does not have a notion of a persistent connection. Thus, these
    objects are small stateless factories for cursors, which do all the real
    work.
    """

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def close(self):
        """ BigQueryConnection does not have anything to close. """
        pass

    def commit(self):
        """ BigQueryConnection does not support transactions. """
        pass

    def cursor(self):
        """ Return a new :py:class:`Cursor` object using the connection. """
        return BigQueryCursor(*self._args, **self._kwargs)

    def rollback(self):
        raise NotSupportedError("BigQueryConnection does not have transactions")

class BigQueryBaseCursor(object):
    # TODO pydocs
    def __init__(self, service, project_id):
        # TODO pydocs
        self.service = service
        self.project_id = project_id

    def run_query(self, bql, destination_dataset_table = False, write_disposition = 'WRITE_EMPTY'):
        """
        Executes a BigQuery SQL query. Optionally persists results in a BigQuery
        table. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param bql: The BigQuery SQL to execute.
        :type bql: string
        :param destination_dataset_table: The dotted <dataset>.<table> 
            BigQuery table to save the query results.
        :param write_disposition: What to do if the table already exists in 
            BigQuery.
        """
        configuration = {
            'query': {
                'query': bql
            }
        }

        if destination_dataset_table:
            assert '.' in destination_dataset_table, \
                'Expected destination_dataset_table in the format of <dataset>.<table>. Got: {}'.format(destination_dataset_table)
            destination_dataset, destination_table = destination_dataset_table.split('.', 1)
            configuration['query'].update({
                'writeDisposition': write_disposition,
                'destinationTable': {
                    'projectId': self.project_id,
                    'datasetId': destination_dataset,
                    'tableId': destination_table,
                }
            })

        return self.run_with_configuration(configuration)

    def run_extract(self, source_dataset_table, destination_cloud_storage_uris, compression='NONE', export_format='CSV', field_delimiter=',', print_header=True):
        """
        Executes a BigQuery extract command to copy data from BigQuery to 
        Google Cloud Storage. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param source_dataset_table: The dotted <dataset>.<table> BigQuery table to use as the source data.
        :type source_dataset_table: string
        :param destination_cloud_storage_uris: The destination Google Cloud 
            Storage URI (e.g. gs://some-bucket/some-file.txt). Follows 
            convention defined here: 
            https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
        :type destination_cloud_storage_uris: list
        :param compression: Type of compression to use.
        :type compression: string
        :param export_format: File format to export.
        :type field_delimiter: string
        :param field_delimiter: The delimiter to use when extracting to a CSV.
        :type field_delimiter: string
        :param print_header: Whether to print a header for a CSV file extract.
        :type print_header: boolean
        """
        assert '.' in source_dataset_table, \
            'Expected source_dataset_table in the format of <dataset>.<table>. Got: {}'.format(source_dataset_table)

        source_dataset, source_table = source_dataset_table.split('.', 1)
        configuration = {
            'extract': {
                'sourceTable': {
                    'projectId': self.project_id,
                    'datasetId': source_dataset,
                    'tableId': source_table,
                },
                'compression': compression,
                'destinationUris': destination_cloud_storage_uris,
                'destinationFormat': export_format,
                'fieldDelimiter': field_delimiter,
                'printHeader': print_header,
            }
        }

        return self.run_with_configuration(configuration)

    def run_with_configuration(self, configuration):
        """
        Executes a BigQuery SQL query. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about the configuration parameter.

        :param configuration: The configuration parameter maps directly to
            BigQuery's configuration field in the job object. See
            https://cloud.google.com/bigquery/docs/reference/v2/jobs for
            details.
        """
        jobs = self.service.jobs()
        job_data = {
            'configuration': configuration
        }

        # Send query and wait for reply.
        query_reply = jobs \
            .insert(projectId=self.project_id, body=job_data) \
            .execute()
        job_id = query_reply['jobReference']['jobId']
        job = jobs.get(projectId=self.project_id, jobId=job_id).execute()

        # Wait for query to finish.
        while not job['status']['state'] == 'DONE':
            logging.info('Waiting for job to complete: %s, %s', self.project_id, job_id)
            time.sleep(5)
            job = jobs.get(projectId=self.project_id, jobId=job_id).execute()

        # Check if job had errors.
        if 'errorResult' in job['status']:
            raise Exception('BigQuery job failed. Final error was: %s', job['status']['errorResult'])

        return job_id

class BigQueryCursor(BigQueryBaseCursor):
    """
    The PyHive PEP 249 implementation was used as a reference:

    https://github.com/dropbox/PyHive/blob/master/pyhive/presto.py
    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py
    """

    def __init__(self, service, project_id):
        """
        # TODO param docs
        """
        super(BigQueryCursor, self).__init__(service=service, project_id=project_id)
        self.buffersize = None
        self.page_token = None
        self.job_id = None
        self.buffer = []

    @property
    def description(self):
        raise NotImplementedError

    def close(self):
        """ By default, do nothing """
        pass

    @property
    def rowcount(self):
        """ By default, return -1 to indicate that this is not supported. """
        return -1

    def execute(self, operation, parameters=None):
        """
        # TODO javadocs
        """
        bql = _bind_parameters(operation, parameters) if parameters else operation
        self.job_id = self.run_query(bql)

    def executemany(self, operation, seq_of_parameters):
        """
        Execute an operation multiple times with different parameters.
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def fetchone(self):
        # TODO pydocs
        return self.next()

    def next(self):
        # TODO pydocs
        if not self.job_id:
            return None

        if len(self.buffer) == 0:
            query_results = self.service.jobs().getQueryResults(projectId=self.project_id, jobId=self.job_id, pageToken=self.page_token).execute()

            if len(query_results['rows']) == 0:
                # Reset all state since we've exhausted the results.
                self.page_token = None
                self.job_id = None
                self.page_token = None
                return None
            else:
                self.page_token = query_results.get('pageToken')
                fields = query_results['schema']['fields']
                col_types = [field['type'] for field in fields]
                rows = query_results['rows']

                for idx, dict_row in enumerate(rows):
                    typed_row = [_bq_cast(vs['v'], col_types[idx]) for vs in dict_row['f']]
                    self.buffer.append(typed_row)

        return self.buffer.pop(0)

    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a
        list of tuples). An empty sequence is returned when no more rows are available.
        The number of rows to fetch per call is specified by the parameter. If it is not given, the
        cursor's arraysize determines the number of rows to be fetched. The method should try to
        fetch as many rows as indicated by the size parameter. If this is not possible due to the
        specified number of rows not being available, fewer rows may be returned.
        An :py:class:`~pyhive.exc.Error` (or subclass) exception is raised if the previous call to
        :py:meth:`execute` did not produce any result set or no call was issued yet.
        """
        if size is None:
            size = self.arraysize
        result = []
        for _ in xrange(size):
            one = self.fetchone()
            if one is None:
                break
            else:
                result.append(one)
        return result

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a list of tuples).
        """
        result = []
        while True:
            one = self.fetchone()
            if one is None:
                break
            else:
                result.append(one)
        return result

    def get_arraysize(self):
        # TODO pydocs
        # PEP 249
        return self._buffersize if self.buffersize else 1

    def set_arraysize(self, arraysize):
        # TODO pydocs
        # PEP 249
        self.buffersize = arraysize

    arraysize = property(get_arraysize, set_arraysize)

    def setinputsizes(self, sizes):
        """ Does nothing by default """
        pass

    def setoutputsize(self, size, column=None):
        """ Does nothing by default """
        pass

def _bind_parameters(operation, parameters):
    # TODO pydocs
    # inspired by MySQL Python Connector (conversion.py)
    string_parameters = {}
    for (name, value) in parameters.iteritems():
        if value is None:
            string_parameters[name] = 'NULL'
        elif isinstance(value, basestring):
            string_parameters[name] = "'" + _escape(value) + "'"
        else:
            string_parameters[name] = str(value)
    return operation % string_parameters

def _escape(s):
    # TODO pydocs
    e = s
    e = e.replace('\\', '\\\\')
    e = e.replace('\n', '\\n')
    e = e.replace('\r', '\\r')
    e = e.replace("'", "\\'")
    e = e.replace('"', '\\"')
    return e

def _bq_cast(string_field, bq_type):
    # TODO pydocs
    if bq_type == 'INTEGER' or bq_type == 'TIMESTAMP':
        return int(string_field)
    elif bq_type == 'FLOAT':
        return float(string_field)
    elif bq_type == 'BOOLEAN':
        return bool(string_field)
    else:
        return string_field