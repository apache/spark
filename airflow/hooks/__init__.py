import logging

try:
    from airflow.hooks.mysql_hook import MySqlHook
except:
    logging.INFO("Couldn't load MySQLHook")
    pass

try:
    from airflow.hooks.postgres_hook import PostgresHook
except:
    logging.INFO("Couldn't import PostgreHook")
    pass

from airflow.hooks.hive_hooks import HiveCliHook
from airflow.hooks.hive_hooks import HiveMetastoreHook
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.hooks.presto_hook import PrestoHook

try:
    from airflow.hooks.samba_hook import SambaHook
except:
    logging.INFO("Couldn't import SambaHook")
    pass

try:
    from airflow.hooks.S3_hook import S3Hook
except:
    logging.INFO("Couldn't import S3Hook")
    pass
