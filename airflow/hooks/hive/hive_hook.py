import logging
import sys
import os
from airflow import settings

# Adding the Hive python libs to python path
sys.path.insert(0, settings.HIVE_HOME_PY)

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hive_metastore import ThriftHiveMetastore
from hive_service import ThriftHive

from airflow.hooks.base_hook import BaseHook

METASTORE_THRIFT_HOST = "localhost"
METASTORE_THRIFT_PORT = 10000


class HiveHook(BaseHook):

    def __init__(self, hive_dbid=None):

        # Connection to Hive
        transport = TSocket.TSocket(
            METASTORE_THRIFT_HOST, METASTORE_THRIFT_PORT)
        self.transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.hive = ThriftHive.Client(protocol)

    def get_conn(self):
        self.transport.open()
        return self.hive

    def check_for_partition(self, schema, table, filter):
        try:
            self.transport.open()
            partitions = self.hive.get_partitions_by_filter(
                schema, table, filter, 1)
            self.transport.close()
            if partitions:
                return True
            else:
                return False
        except Exception as e:
            logging.error(
                "Metastore down? Activing as if partition doesn't "
                "exist to be safe...")
            return False

    def get_records(self, hql, schema=None):
        self.transport.open()
        if schema:
            self.hive.execute("USE " + schema)
        self.hive.execute(hql)
        records = self.hive.fetchAll()
        self.transport.close()
        return [row.split("\t") for row in records]

    def run(self, hql, schema=None):
        self.transport.open()
        if schema:
            self.hive.execute("USE " + schema)
        self.hive.execute(hql)
        self.transport.close()

if __name__ == "__main__":
    hh = HiveHook()
    hql = "SELECT * FROM fct_nights_booked WHERE ds='2014-10-01' LIMIT 2"
    print hh.get_records(schema="core_data", hql=hql)

    print "Checking for partition:" + str(hh.check_for_partition(
        schema="core_data", table="fct_nights_booked", filter="ds=2014-10-01"))
