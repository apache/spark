from airflow import configuration


def max_partition(
        table, schema="default",
        hive_dbid=configuration.get_config().get('hooks', 'HIVE_DEFAULT_DBID')):
    from airflow.hooks.hive_hook import HiveHook
    hh = HiveHook(hive_dbid=hive_dbid)
    return hh.max_partition(schema=schema, table=table)
