import csv
import subprocess
import StringIO

from airflow.hooks.base_hook import BaseHook

PRESTO_BIN = "java -jar /usr/bin/presto-cli.jar --server http://i-5966e3b2:3400 --catalog hive --debug"


class PrestoHook(BaseHook):
    """
    Interact with Presto! This is just a thin wrapper on the Presto CLI
    """
    def __init__(self):
        pass

    def query(hql, schema="default", headers=False):
        csv_format = 'CSV_HEADER' if headers else 'CSV'

        cmd = (PRESTO_BIN +
            ' --execute "{hql}" --schema {schema} ' +
            ' --output-format {csv_format}').format(**locals())
        sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        out, err = sp.communicate()
        return out

    def get_records(self, hql, schema="default"):
        results = self.query(hql, schema)
        return [row for row in csv.reader(StringIO.StringIO(results))]

    def get_pandas_df(self, hql, schema="default"):
        import pandas
        results = self.query(hql, schema, headers=True)
        return pandas.DataFrame.from_csv(
            StringIO.StringIO(results),
            header=True,
        )

    def run(self, sql):
        raise NotImplemented()
