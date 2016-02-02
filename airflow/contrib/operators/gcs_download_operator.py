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

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: string
        :param object: The name of the object to download in the Google cloud
            storage bucket.
        :type object: string
        :param filename: The file path on the local file system (where the
            operator is being executed) that the file should be downloaded to.
        :type filename: string
        :param google_cloud_storage_conn_id: The connection ID to use when
            connecting to Google cloud storage.
        :type google_cloud_storage_conn_id: string
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
