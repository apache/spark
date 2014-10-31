from airflow import settings


def max_partition(
        table, schema="default", hive_dbid=settings.HIVE_DEFAULT_DBID):
    from airflow.hooks.hive_hook import HiveHook
    hh = HiveHook(hive_dbid=hive_dbid)
    return hh.max_partition(schema=schema, table=table)
