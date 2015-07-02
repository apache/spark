import logging

from airflow.hooks import HiveServer2Hook, DruidHook, HiveMetastoreHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class HiveToDruidTransfer(BaseOperator):
    """
    Moves data from Hive to Druid, note that for now the data is loaded
    into memory before being pushed to Druid, so this operator should
    be used for smallish amount of data.

    :param sql: SQL query to execute against the Druid database
    :type sql: str
    """

    template_fields = ('sql', 'druid_table')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            sql,
            druid_datasource,
            hive_cli_conn_id='hiveserver2_default',
            druid_ingest_conn_id='druid_ingest_default',
            metastore_conn_id='metastore_default',
            *args, **kwargs):
        super(HiveToDruidTransfer, self).__init__(*args, **kwargs)
        self.sql = sql
        self.druid_datasource = druid_datasource
        self.hive_cli_conn_id = hive_cli_conn_id
        self.druid_ingest_conn_id = druid_ingest_conn_id
        self.metastore_conn_id = metastore_conn_id

    def execute(self, context):
        hive = HiveServer2Hook(hiveserver2_conn_id=self.hive_cli_conn_id)
        logging.info("Extracting data from Hive")
        hive_table = (
            'tmp.druid__' + context['task_instance'].task_instance_key_str)
        hql = hql.strip().strip(';')
        sql = """\
        CREATE EXTERNAL TABLE {hive_table}
        STORED AS TEXTFILE AS
        {sql};
        """.format(**locals())
        m = HiveMetastoreHook(self.metastore_conn_id)
        hdfs_uri = m.get_table(hive_table).sd.location
        hive.run(self.sql)
        druid = DruidHook(druid_ingest_conn_id=self.druid_ingest_conn_id)
        logging.info("Inserting rows into Druid")
        druid.load_from_hdfs(table=self.druid_datasource, hdfs_uri=hdfs_uri)
