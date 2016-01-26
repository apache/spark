import logging

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults

class GoogleCloudStorageDownloadOperator(BaseOperator):
    """
    Downloads a file from Google Cloud Storage.
    """
    template_fields = ('bucket','object',)
    template_ext = ('.sql',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
        self,
        bucket,
        object,
        filename,
        google_cloud_storage_conn_id='google_cloud_storage_default',
        *args,
        **kwargs):
        """
        Create a new GoogleCloudStorageDownloadOperator.
        """
        super(GoogleCloudStorageDownloadOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.object = object
        self.filename = filename
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id

    def execute(self, context):
        logging.info('Executing download: %s, %s, %s', self.bucket, self.object, self.filename)
        hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)
        print(hook.download(self.bucket, self.object, self.filename))
