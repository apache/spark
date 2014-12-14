from airflow.configuration import getconf

def max_partition(
        table, schema="default",
        hive_dbid=getconf().get('hooks', 'HIVE_DEFAULT_DBID')):
    from airflow.hooks.hive_hook import HiveHook
    if '.' in table:
        schema, table = table.split('.')
    hh = HiveHook(hive_dbid=hive_dbid)
    return hh.max_partition(schema=schema, table=table)
