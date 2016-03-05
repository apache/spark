from future.standard_library import install_aliases
install_aliases()

from urllib.parse import urlparse
from airflow.hooks import BaseHook
from airflow.utils import AirflowException

import gcloud.storage as gcs
import gcloud

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

    A GCP connection ID can be provided. If it is provided, its "extra" values
    will OVERRIDE any argments passed to GCSHook. The following precendance is
    observed:
        GCP connection "extra"
        GCSHook initialization arguments
        host environment

    Extras should be JSON and take the form:
    {
        "project": "<google cloud project id>",
        "key_path": "<path to service account keyfile, either JSON or P12>"
        "service_account": "<google service account email, required for P12>"
        "scope": "<google service scopes>"
    }

    service_account is only required if the key_path points to a P12 file.

    scope is only used if key_path is provided. Scopes can include:
        https://www.googleapis.com/auth/devstorage.full_control
        https://www.googleapis.com/auth/devstorage.read_only
        https://www.googleapis.com/auth/devstorage.read_write

    If fields are not provided, either as arguments or extras, they can be set
    in the host environment.

    To set a default project, use:
        gcloud config set project <project-id>

    To log in:
        gcloud auth

    """
    def __init__(
            self,
            gcp_conn_id=None,
            project=None,
            key_path=None,
            service_account=None,
            scope=None,
            *args,
            **kwargs):

        self.gcp_conn_id = gcp_conn_id
        self.project = project
        self.key_path = key_path
        self.service_account = service_account
        self.scope = scope

        self.gcs_conn = self.get_conn()

    def get_conn(self):
        # parse arguments and connection extras
        if self.gcp_conn_id:
            extras = self.get_connection(self.gcp_conn_id).extra_dejson
        else:
            extras = {}
        project = extras.get('project', self.project)
        key_path = extras.get('key_path', self.key_path)
        service_account = extras.get('service_account', self.service_account)
        scope = extras.get('scope', self.scope)

        # guess project, if possible
        if not project:
            project = gcloud._helpers._determine_default_project()
            # workaround for
            # https://github.com/GoogleCloudPlatform/gcloud-python/issues/1470
            if isinstance(project, bytes):
                project = project.decode()

        # load credentials/scope
        if key_path:
            if key_path.endswith('.json') or key_path.endswith('.JSON'):
                credentials = gcloud.credentials.get_for_service_account_json(
                    json_credentials_path=key_path,
                    scope=scope)
            elif key_path.endswith('.p12') or key_path.endswith('.P12'):
                credentials = gcloud.credentials.get_for_service_account_p12(
                    client_email=service_account,
                    private_key_path=key_path,
                    scope=scope)
            else:
                raise ValueError('Unrecognized keyfile: {}'.format(key_path))
            client = gcs.Client(
                credentials=credentials,
                project=project)
        else:
            client = gcs.Client(project=project)

        return client

    def bucket_exists(self, bucket):
        return self.gcs_conn.bucket(bucket).exists()

    def get_bucket(self, bucket):
        return self.gcs_conn.get_bucket(bucket)

    def list_blobs(
            self,
            bucket,
            max_results=None,
            page_token=None,
            prefix=None,
            delimiter=None):
        return self.gcs_conn.bucket(bucket).list_blobs(
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
        return self.gcs_conn.bucket(bucket).get_blob(blob)

    def blob_exists(self, blob, bucket=None):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        return self.gcs_conn.bucket(bucket).blob(blob).exists()

    def upload_from_file(
            self,
            file_obj,
            blob,
            bucket=None,
            replace=False):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.gcs_conn.bucket(bucket).blob(blob)
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
        gcs_blob = self.gcs_conn.bucket(bucket).blob(blob)
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
        gcs_blob = self.gcs_conn.bucket(bucket).blob(blob)
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
        gcs_blob = self.gcs_conn.bucket(bucket).get_blob(blob)
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
        gcs_blob = self.gcs_conn.bucket(bucket).get_blob(blob)
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
        gcs_blob = self.gcs_conn.bucket(bucket).get_blob(blob)
        if not gcs_blob:
            raise ValueError(
                'Blob does not exist: {bucket}/{blob}'.format(**locals()))
        return gcs_blob.download_to_filename(filename)

    def download(
            self,
            bucket,
            object,
            filename=False):
        """
        This method is provided for compatibility with
        contrib/GoogleCloudStorageHook.
        """
        if filename:
            return self.download_to_filename(
                filename=filename, blob=object, bucket=bucket)
        else:
            return self.download_as_string(blob=object, bucket=bucket)

    def upload(
            self,
            bucket,
            object,
            filename,
            mime_type='application/octet-stream'):
        """
        This method is provided for compatibility with
        contrib/GoogleCloudStorageHook.

        Warning: acts as if replace == True!
        """
        self.upload_from_filename(
                filename=filename, blob=object, bucket=bucket, replace=True)
