import logging
import json
import subprocess
from tempfile import NamedTemporaryFile

from airflow.models import Connection
from airflow import settings

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hive_service import ThriftHive

from airflow.hooks.base_hook import BaseHook


class HiveHook(BaseHook):
    '''
    Interact with Hive. This class is both a wrapper around the Hive Thrift
    client and the Hive CLI.
    '''
    def __init__(
            self,
            hive_conn_id='hive_default'):
        session = settings.Session()
        db = session.query(
            Connection).filter(
                Connection.conn_id == hive_conn_id)
        if db.count() == 0:
            raise Exception("The conn_id you provided isn't defined")
        else:
            db = db.all()[0]
        self.host = db.host
        self.db = db.schema
        self.hive_cli_params = ""
        try:
            self.hive_cli_params = json.loads(db.extra)['hive_cli_params']
        except:
            pass

        self.port = db.port
        session.commit()
        session.close()

        # Connection to Hive
        self.hive = self.get_hive_client()

    def __getstate__(self):
        # This is for pickling to work despite the thirft hive client not
        # being pickable
        d = dict(self.__dict__)
        del d['hive']
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.__dict__['hive'] = self.get_hive_client()

    def get_hive_client(self):
        '''
        Returns a Hive thrift client.
        '''
        transport = TSocket.TSocket(self.host, self.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        return ThriftHive.Client(protocol)

    def get_conn(self):
        return self.hive

    def check_for_partition(self, schema, table, partition):
        '''
        Checks whether a partition exists

        >>> hh = HiveHook()
        >>> t = 'static_babynames_partitioned'
        >>> hh.check_for_partition('airflow', t, "ds='2015-01-01'")
        True
        '''
        self.hive._oprot.trans.open()
        partitions = self.hive.get_partitions_by_filter(
            schema, table, partition, 1)
        self.hive._oprot.trans.close()
        if partitions:
            return True
        else:
            return False

    def get_records(self, hql, schema=None):
        '''
        Get a set of records from a Hive query.

        >>> hh = HiveHook()
        >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
        >>> hh.get_records(sql)
        [['340698']]
        '''
        self.hive._oprot.trans.open()
        if schema:
            self.hive.execute("USE " + schema)
        self.hive.execute(hql)
        records = self.hive.fetchAll()
        self.hive._oprot.trans.close()
        return [row.split("\t") for row in records]

    def get_pandas_df(self, hql, schema=None):
        '''
        Get a pandas dataframe from a Hive query

        >>> hh = HiveHook()
        >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
        >>> df = hh.get_pandas_df(sql)
        >>> df.to_dict()
        {0: {0: '340698'}}
        '''
        import pandas as pd
        self.hive._oprot.trans.open()
        if schema:
            self.hive.execute("USE " + schema)
        self.hive.execute(hql)
        records = self.hive.fetchAll()
        self.hive._oprot.trans.close()
        df = pd.DataFrame([row.split("\t") for row in records])
        return df

    def run(self, hql, schema=None):
        self.hive._oprot.trans.open()
        if schema:
            self.hive.execute("USE " + schema)
        self.hive.execute(hql)
        self.hive._oprot.trans.close()

    def run_cli(self, hql, schema=None):
        '''
        Run an hql statement using the hive cli

        >>> hh = HiveHook()
        >>> hh.run_cli("USE airflow;")
        '''
        if schema:
            hql = "USE {schema};\n{hql}".format(**locals())

        f = NamedTemporaryFile()
        f.write(hql)
        f.flush()
        sp = subprocess.Popen(
            "hive -f {f.name} {self.hive_cli_params}".format(**locals()),
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        all_err = ''
        self.sp = sp
        for line in iter(sp.stdout.readline, ''):
            logging.info(line.strip())
        sp.wait()
        f.close()

        if sp.returncode:
            raise Exception(all_err)

    def get_table(self, db, table_name):
        '''
        Get a metastore table object

        >>> hh = HiveHook()
        >>> t = hh.get_table(db='airflow', table_name='static_babynames')
        >>> t.tableName
        'static_babynames'
        >>> [col.name for col in t.sd.cols]
        ['state', 'year', 'name', 'gender', 'num']
        '''
        self.hive._oprot.trans.open()
        table = self.hive.get_table(dbname=db, tbl_name=table_name)
        self.hive._oprot.trans.close()
        return table

    def get_partitions(self, schema, table_name):
        '''
        Returns a list of all partitions in a table. Works only
        for tables with less than 32767 (java short max val).
        For subpartitionned table, the number might easily exceed this.

        >>> hh = HiveHook()
        >>> t = 'static_babynames_partitioned'
        >>> parts = hh.get_partitions(schema='airflow', table_name=t)
        >>> len(parts)
        1
        >>> max(parts)
        '2015-01-01'
        '''
        self.hive._oprot.trans.open()
        table = self.hive.get_table(dbname=schema, tbl_name=table_name)
        if len(table.partitionKeys) == 0:
            raise Exception("The table isn't partitionned")
        elif len(table.partitionKeys) > 1:
            raise Exception(
                "The table is partitionned by multiple columns, "
                "use a signal table!")
        else:
            parts = self.hive.get_partitions(
                db_name=schema, tbl_name=table_name, max_parts=32767)

            self.hive._oprot.trans.close()
            return [p.values[0] for p in parts]

    def max_partition(self, schema, table_name):
        '''
        Returns the maximum value for all partitions in a table. Works only
        for tables that have a single partition key. For subpartitionned
        table, we recommend using signal tables.

        >>> hh = HiveHook()
        >>> t = 'static_babynames_partitioned'
        >>> hh.max_partition(schema='airflow', table_name=t)
        '2015-01-01'
        '''
        return max(self.get_partitions(schema, table_name))

    def kill(self):
        if hasattr(self, 'sp'):
            if self.sp.poll() is None:
                print("Killing the Hive job")
                self.sp.kill()
