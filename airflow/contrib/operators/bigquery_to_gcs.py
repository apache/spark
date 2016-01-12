import logging

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults

class BigQueryToCloudStorageOperator(BaseOperator):
    """
    Executes BigQuery SQL queries in a specific BigQuery database
    """

    template_fields = ('source_dataset_table','destination_cloud_storage_uris',)
    template_ext = ('.sql',)
    ui_color = '#e4e6f0'

    @apply_defaults
    def __init__(
        self, 
        source_dataset_table, 
        destination_cloud_storage_uris, 
        compression='NONE', 
        export_format='CSV', 
        field_delimiter=',', 
        print_header=True, 
        bigquery_conn_id='bigquery_default', 
        *args, 
        **kwargs):
        """
        Create a new BigQueryToCloudStorage to move data from BigQuery to 
        Google Cloud Storage.

        TODO: more params to doc
        :param bigquery_conn_id: reference to a specific BigQuery hook.
        :type bigquery_conn_id: string
        """
        super(BigQueryToCloudStorageOperator, self).__init__(*args, **kwargs)
        self.source_dataset_table = source_dataset_table 
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        self.compression = compression
        self.export_format = export_format
        self.field_delimiter = field_delimiter
        self.print_header = print_header
        self.bigquery_conn_id = bigquery_conn_id

    def execute(self, context):
        logging.info('Executing extract of %s into: %s', self.source_dataset_table, self.destination_cloud_storage_uris)
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)
        hook.run_extract(
            self.source_dataset_table,
            self.destination_cloud_storage_uris,
            self.compression,
            self.export_format,
            self.field_delimiter,
            self.print_header)
