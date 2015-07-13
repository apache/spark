import logging
import json
import time

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
	self.header = {'content-type': 'application/json'}


    def get_conn(self):
        """
        Returns a druid connection object for query
        """
        conn = self.get_connection(self.druid_query_conn_id)
        client = pydruid.client.PyDruid(
            "http://{conn.host}:{conn.port}".format(**locals()),
            conn.extra_dejson.get('endpoint', ''))
        return client

    @property
    def ingest_post_url(self):
	conn = self.get_connection(self.druid_ingest_conn_id)
        host = conn.host
        port = conn.port
        endpoint = conn.extra_dejson.get('endpoint', '')
	return "http://{host}:{port}/{endpoint}".format(**locals())

    def get_ingest_status_url(self, task_id):
        post_url = self.ingest_post_url
        return "{post_url}/{task_id}/status".format(**locals())

    def construct_ingest_query(self, datasource, static_path,
                                    ts_dim, dimensions, metric_spec):
	ingest_query_dict = {
            "type": "index_hadoop",
            "spec": {
                "dataSchema": {
                    "metricsSpec": metric_spec,
                    "granularitySpec": {
                        "queryGranularity": "NONE",
                        "intervals": ["1901-01-01/2040-05-25"],
                        "type": "uniform",
                        "segmentGranularity": "DAY"
                    },
                    "parser": {
                        "type": "string",
                        "parseSpec": {
                            "dimensionsSpec": {
                                "dimensionExclusions": [],
                                "dimensions": dimensions,  # list of names
                                "spatialDimensions": []
                            },
                            "timestampSpec": {
                                "column": ts_dim,
                                "format": "auto"
                            },
                            "format": "tsv"
                        }
                    },
                    "dataSource": datasource
                },
                "tuningConfig": {
                    "type": "hadoop"
                },
                "ioConfig": {
                    "inputSpec": {
                        "paths": static_path,
                        "type": "static"
                    },
                    "type": "hadoop"
                }
            }
        }

	return json.dumps(ingest_query_dict)


    def send_ingest_query(self, datasource, static_path, ts_dim,
                                                dimensions, metric_spec):
	query = self.construct_ingest_query(datasource, static_path,
                                            ts_dims, dimensions, metric_spec)
	r = requests.post(self.ingest_post_url, headers=self.header,
                                                                data=query)
        d = json.loads(r.text)
        if "task" not in d:
            raise AirflowException("[Error]: Ingesting data to druid failed.")
        return d["task"]


    def load_from_hdfs(self, datasource, static_path,  ts_dim,
                                    dimensions, metric_spec=None):
        """
	load data to druid from hdfs
        :params ts_dim: The column name to use as a timestamp
        :params dimensions: A list of column names to use as dimension
        :params metric_spec: A list of dictionaries
        """
        task_id = send_ingest_query(datasource, static_path, ts_dim,
                                        dimensions, metric_spec)
        status_url = self.get_ingest_status_url(task_id)
        while True:
            r = requests.get(status_url)
            d = json.loads(r.text)
            if d['status']['status'] == 'FAILED':
                raise AirflowException(
                        "[Error]: Ingesting data to druid failed.")
            elif d['status']['status'] == 'SUCCESS':
                break
            time.sleep(30)

