import csv
import logging
import json
import subprocess
from tempfile import NamedTemporaryFile

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hive_service import ThriftHive
import pyhs2

from airflow.hooks.base_hook import BaseHook


class HiveCliHook(BaseHook):
    '''
    Simple wrapper around the hive CLI
    '''
    def __init__(
            self,
            hive_cli_conn_id="hive_cli_default"
            ):
        conn = self.get_connection(hive_cli_conn_id)
        self.hive_cli_params = ""
        try:
            self.hive_cli_params = json.loads(
                conn.extra)['hive_cli_params']
        except:
            pass

    def run_cli(self, hql, schema=None):
        '''
        Run an hql statement using the hive cli

        >>> hh = HiveCliHook()
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

    def kill(self):
        if hasattr(self, 'sp'):
            if self.sp.poll() is None:
                print("Killing the Hive job")
                self.sp.kill()


class HiveMetastoreHook(BaseHook):
    '''
    Wrapper to interact with the Hive Metastore
    '''
    def __init__(self, metastore_conn_id='metastore_default'):
        self.metastore_conn = self.get_connection(metastore_conn_id)
        self.metastore = self.get_metastore_client()

    def __getstate__(self):
        # This is for pickling to work despite the thirft hive client not
        # being pickable
        d = dict(self.__dict__)
        del d['metastore']
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.__dict__['metastore'] = self.get_metastore_client()

    def get_metastore_client(self):
        '''
        Returns a Hive thrift client.
        '''
        ms = self.metastore_conn
        transport = TSocket.TSocket(ms.host, ms.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        return ThriftHive.Client(protocol)

    def get_conn(self):
        return self.metastore

    def check_for_partition(self, schema, table, partition):
        '''
        Checks whether a partition exists

        >>> hh = HiveMetastoreHook()
        >>> t = 'static_babynames_partitioned'
        >>> hh.check_for_partition('airflow', t, "ds='2015-01-01'")
        True
        '''
        self.metastore._oprot.trans.open()
        partitions = self.metastore.get_partitions_by_filter(
            schema, table, partition, 1)
        self.metastore._oprot.trans.close()
        if partitions:
            return True
        else:
            return False

    def get_table(self, db, table_name):
        '''
        Get a metastore table object

        >>> hh = HiveMetastoreHook()
        >>> t = hh.get_table(db='airflow', table_name='static_babynames')
        >>> t.tableName
        'static_babynames'
        >>> [col.name for col in t.sd.cols]
        ['state', 'year', 'name', 'gender', 'num']
        '''
        self.metastore._oprot.trans.open()
        table = self.metastore.get_table(dbname=db, tbl_name=table_name)
        self.metastore._oprot.trans.close()
        return table

    def get_tables(self, db, pattern='*'):
        '''
        Get a metastore table object
        '''
        self.metastore._oprot.trans.open()
        tables = self.metastore.get_tables(db_name=db, pattern=pattern)
        objs = self.metastore.get_table_objects_by_name(db, tables)
        self.metastore._oprot.trans.close()
        return objs

    def get_partitions(self, schema, table_name):
        '''
        Returns a list of all partitions in a table. Works only
        for tables with less than 32767 (java short max val).
        For subpartitionned table, the number might easily exceed this.

        >>> hh = HiveMetastoreHook()
        >>> t = 'static_babynames_partitioned'
        >>> parts = hh.get_partitions(schema='airflow', table_name=t)
        >>> len(parts)
        1
        >>> max(parts)
        '2015-01-01'
        '''
        self.metastore._oprot.trans.open()
        table = self.metastore.get_table(dbname=schema, tbl_name=table_name)
        if len(table.partitionKeys) == 0:
            raise Exception("The table isn't partitionned")
        elif len(table.partitionKeys) > 1:
            raise Exception(
                "The table is partitionned by multiple columns, "
                "use a signal table!")
        else:
            parts = self.metastore.get_partitions(
                db_name=schema, tbl_name=table_name, max_parts=32767)

            self.metastore._oprot.trans.close()
            return [p.values[0] for p in parts]

    def max_partition(self, schema, table_name):
        '''
        Returns the maximum value for all partitions in a table. Works only
        for tables that have a single partition key. For subpartitionned
        table, we recommend using signal tables.

        >>> hh = HiveMetastoreHook()
        >>> t = 'static_babynames_partitioned'
        >>> hh.max_partition(schema='airflow', table_name=t)
        '2015-01-01'
        '''
        return max(self.get_partitions(schema, table_name))


class HiveServer2Hook(BaseHook):
    '''
    Wrapper around the pyhs2 lib
    '''
    def __init__(self, hiveserver2_conn_id='hiveserver2_default'):
        self.hiveserver2_conn = self.get_connection(hiveserver2_conn_id)

    def get_results(self, hql, schema='default'):
        schema = schema or 'default'
        with pyhs2.connect(
                host=self.hiveserver2_conn.host,
                port=self.hiveserver2_conn.port,
                authMechanism="NOSASL",
                user='airflow',
                database=schema) as conn:
            with conn.cursor() as cur:
                cur.execute(hql)
                return {
                    'data': cur.fetchall(),
                    'header': cur.getSchema(),
                }

    def to_csv(self, hql, csv_filepath, schema='default'):
        schema = schema or 'default'
        with pyhs2.connect(
            host=self.hiveserver2_conn.host,
            port=self.hiveserver2_conn.port,
            authMechanism="NOSASL",
            user='airflow',
            database=schema) as conn:
            with conn.cursor() as cur:
                logging.info("Running query: " + hql)
                cur.execute(hql)
                schema = cur.getSchema()
                with open(csv_filepath, 'w') as f:
                    writer = csv.writer(f)
                    writer.writerow([c['columnName'] for c in cur.getSchema()])
                    i = 0
                    while cur.hasMoreRows:
                        rows = [row for row in cur.fetchmany() if row]
                        writer.writerows(rows)
                        i += len(rows)
                        logging.info("Written {0} rows so far.".format(i))
                    logging.info("Done. Loaded a total of {0} rows.".format(i))


    def get_records(self, hql, schema='default'):
        '''
        Get a set of records from a Hive query.

        >>> hh = HiveServer2Hook()
        >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
        >>> hh.get_records(sql)
        [[340698]]
        '''
        return self.get_results(hql, schema=schema)['data']

    def get_pandas_df(self, hql, schema='default'):
        '''
        Get a pandas dataframe from a Hive query

        >>> hh = HiveServer2Hook()
        >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
        >>> df = hh.get_pandas_df(sql)
        >>> df.to_dict()
        {'num': {0: 340698}}
        '''
        import pandas as pd
        res = self.get_results(hql, schema=schema)
        df = pd.DataFrame(res['data'])
        df.columns = [c['columnName'] for c in res['header']]
        return df

    def run(self, hql, schema=None):
        self.hive._oprot.trans.open()
        if schema:
            self.hive.execute("USE " + schema)
        self.hive.execute(hql)
        self.hive._oprot.trans.close()
