from airflow.hooks.base_hook import BaseHook
from airflow.configuration import conf
import logging

from hdfs import InsecureClient
from hdfs.utils import Hdfserror
from airflow.utils import AirflowException


class WebHDFSHookException(AirflowException):
    pass


class WebHDFSHook(BaseHook):
    """
    Interact with HDFS. This class is a wrapper around the hdfscli library.
    """
    def __init__(self, hdfs_conn_id='hdfs_default'):
        self.hdfs_conn_id = hdfs_conn_id

    def get_conn(self):
        """
        Returns a hdfscli InsecureClient object.
        """
        nn_connections = self.get_connections(self.hdfs_conn_id)
        for nn in nn_connections:
            try:
                logging.debug('Trying namenode {nn.host}'.format(nn))
                client = InsecureClient('http://{nn.host}:{nn.port}'.format(nn))
                client.content('/')
                logging.debug('Using namenode {nn.host} for hook'.format(nn))
                return client
            except Hdfserror as e:
                logging.debug("Read operation on namenode {nn.host} failed with"
                             " error: {e.messaage}".format(**locals()))
        nn_hosts = [c.host for c in nn_connections]
        no_nn_error = "Read operations failed the namenodes below:\n{}".format("\n".join(nn_hosts))
        raise WebHDFSHookException(no_nn_error)

    def check_for_path(self, hdfs_path):
        """
        Check for the existence of a path in HDFS by querying FileStatus.
        """
        c = self.get_conn()
        return bool(c.status(hdfs_path, strict=False))

    def load_file(self, source, destination, overwrite=True, parallelism=1,
                  tmp_dir=None, chunk_size=2 ** 16, progress=None, **kwargs):
        """
        Uploads a file to HDFS

        :param source: Local path to file or folder. If a folder, all the files
          inside of it will be uploaded (note that this implies that folders empty
          of files will not be created remotely).
        :type source: str
        :param destination: PTarget HDFS path. If it already exists and is a
          directory, files will be uploaded inside.
        :type destination: str
        :param overwrite: Overwrite any existing file or directory.
        :type overwrite: bool
        :param parallelism: Number of threads to use for parallelization. A value of
          `0` (or negative) uses as many threads as there are files.
        :type parallelism: int
        :param temp_dir: Directory under which the files will first be uploaded
          when `overwrite=True` and the final remote path already exists. Once the
          upload successfully completes, it will be swapped in.
        :param chunk_size: Interval in bytes by which the files will be uploaded.
        :param progress: Callback function to track progress, called every
          `chunk_size` bytes. It will be passed two arguments, the path to the
          file being uploaded and the number of bytes transferred so far. On
          completion, it will be called once with `-1` as second argument.
        :param \*\*kwargs: Keyword arguments forwarded to :meth:`write`.


        """
        c = self.get_conn()
        c.upload(hdfs_path=destination,
                 local_path=source,
                 overwrite=overwrite,
                 n_threads=parallelism,
                 tmp_dir=tmp_dir,
                 chunk_size=chunk_size,
                 progress=progress,
                 **kwargs)
        logging.debug("Uploaded file {} to {}".format(source, destination))
