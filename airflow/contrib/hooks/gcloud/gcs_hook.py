from future.standard_library import install_aliases
install_aliases()

from urllib.parse import urlparse
from airflow.hooks import BaseHook
from airflow.utils import AirflowException

import gcloud.storage as gcs

def parse_gcs_url(gsurl):
    """
    Given a Google Cloud Storage URL (gs://<bucket>/<blob>), returns a
    tuple containing the corresponding bucket and blob.
    """
    parsed_url = urlparse(gsurl)
    if not parsed_url.netloc:
        raise AirflowException('Please provide a bucket name')
    else:
        bucket = parsed_url.netloc
        if parsed_url.path.startswith('/'):
            blob = parsed_url.path[1:]
        else:
            blob = parsed_url.path
        return (bucket, blob)

class GCSHook(BaseHook):
    """
    A hook for working wth Google Cloud Storage via the gcloud library.

    GCS Connections can contain two optional fields in "extras":
    {
        "project": "<google cloud project id>",
        "keyfile_path": "<path to service account JSON keyfile>"
    }

    If the project field is missing, the project will be inferred from the host
    environment (if possible). To set a default project, use:
        gcloud config set project <project-id>

    If the keyfile_path is missing, the host authorization credentials will be
    used (if possible). To log in, use:
        gcloud auth
    """
    def __init__(self, gcs_conn_id=None):
        self.gcs_conn_id = gcs_conn_id
        self.gcs_conn = self.get_conn()

    def get_conn(self):
        project, keyfile_path = None, None
        if self.gcs_conn_id:
            conn = self.get_connection(self.gcs_conn_id)
            extras = conn.extra_dejson
            project = extras.get('project', None)
            keyfile_path = extras.get('keyfile_path', None)

        if not project:
            project = gcloud._helpers._determine_default_project()
            # workaround for
            # https://github.com/GoogleCloudPlatform/gcloud-python/issues/1470
            if isinstance(project, bytes):
                project = project.decode()

        if keyfile_path:
            client = gcs.Client.from_service_account_json(
                keyfile_path, project=project)
        else:
            client = gcs.Client(project=project)

        return client

    def bucket_exists(self, bucket):
        return self.get_conn().bucket(bucket).exists()

    def get_bucket(self, bucket):
        return self.get_conn().get_bucket(bucket)

    def list_blobs(
            self,
            bucket,
            max_results=None,
            page_token=None,
            prefix=None,
            delimiter=None):
        return self.get_conn().bucket(bucket).list_blobs(
            max_results=max_results,
            page_token=page_token,
            prefix=prefix,
            delimiter=delimiter)

    def get_blob(self, blob, bucket=None):
        """
        Returns None if the blob does not exist
        """
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        return self.get_conn().bucket(bucket).get_blob(blob)

    def blob_exists(self, blob, bucket=None):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        return self.get_conn().bucket(bucket).blob(blob).exists()

    def upload_from_file(
            self,
            file_obj,
            blob,
            bucket=None,
            replace=False):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.get_conn().bucket(bucket).blob(blob)
        if gcs_blob.exists() and not replace:
            raise ValueError(
                'The blob {bucket}/{blob} already exists.'.format(**locals()))
        gcs_blob.upload_from_file(file_obj)

    def upload_from_filename(
            self,
            filename,
            blob,
            bucket=None,
            replace=False):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.get_conn().bucket(bucket).blob(blob)
        if gcs_blob.exists() and not replace:
            raise ValueError(
                'The blob {bucket}/{blob} already exists.'.format(**locals()))
        gcs_blob.upload_from_filename(filename)

    def upload_from_string(
            self,
            string,
            blob,
            bucket=None,
            replace=False):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.get_conn().bucket(bucket).blob(blob)
        if gcs_blob.exists() and not replace:
            raise ValueError(
                'The blob {bucket}/{blob} already exists.'.format(**locals()))
        gcs_blob.upload_from_string(string)

    def download_as_string(
            self,
            blob,
            bucket=None):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.get_conn().bucket(bucket).get_blob(blob)
        if not gcs_blob:
            raise ValueError(
                'Blob does not exist: {bucket}/{blob}'.format(**locals()))
        return gcs_blob.download_as_string()

    def download_to_file(
            self,
            file_obj,
            blob,
            bucket=None):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.get_conn().bucket(bucket).get_blob(blob)
        if not gcs_blob:
            raise ValueError(
                'Blob does not exist: {bucket}/{blob}'.format(**locals()))
        return gcs_blob.download_to_file(file_obj)

    def download_to_filename(
            self,
            filename,
            blob,
            bucket=None):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.get_conn().bucket(bucket).get_blob(blob)
        if not gcs_blob:
            raise ValueError(
                'Blob does not exist: {bucket}/{blob}'.format(**locals()))
        return gcs_blob.download_to_filename(filename)
