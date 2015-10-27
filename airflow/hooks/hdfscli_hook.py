from airflow.hooks.base_hook import BaseHook
from airflow.configuration import conf

from hdfs import InsecureClient
from airflow.utils import AirflowException


class HDFSCLIHookException(AirflowException):
    pass


class HDFSCLIHook(BaseHook):
    """
    Interact with HDFS. This class is a wrapper around the hdfscli library.
    """
    def __init__(self, hdfs_conn_id='hdfs_default'):
        self.hdfs_conn_id = hdfs_conn_id

    def get_conn(self):
        """
        Returns a snakebite HDFSClient object.
        """
        conn = self.get_connection(self.hdfs_conn_id)
        client = InsecureClient('http://{conn.host}:{conn.port}'.format(conn))
        return client

    def load_file(self, source, destination, replace):
        """
        Uploads a file to HDFS
        """

