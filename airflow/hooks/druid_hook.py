import logging
import json

import pydruid
import requests

from airflow.hooks.base_hook import BaseHook


class DruidHook(BaseHook):
    '''
    Interact with druid.
    '''

    def __init__(
            self,
            druid_query_conn_id='druid_query_default',
            druid_ingest_conn_id='druid_ingest_default',
            ):
        self.druid_query_conn_id = druid_query_conn_id
        self.druid_ingest_conn_id = druid_ingest_conn_id

    @property
    def domain_port(self):
        pass

    def get_conn(self):
        """
        Returns a druid connection object
        """
        conn = self.get_connection(self.druid_query_conn_id)
        client = pydruid.client.PyDruid(
            "http://{conn.host}:{conn.port}".format(**locals()),
            conn.extra_dejson.get('enpoint', ''))
        return client

    def load_from_hdfs(self, hdfs_uri):
        """
        """
        conn = self.get_connection(self.druid_query_conn_id)
        endpoint = conn.extra_dejson.get('enpoint', '')
        requests.post(conn.host, hdfs_uri)

