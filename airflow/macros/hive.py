from airflow.configuration import conf

def max_partition(
        table, schema="default",
        hive_conn_id=conf.get('hooks', 'HIVE_DEFAULT_CONN_ID')):
    from airflow.hooks.hive_hook import HiveHook
    if '.' in table:
        schema, table = table.split('.')
    hh = HiveHook(hive_conn_id=hive_conn_id)
    return hh.max_partition(schema=schema, table_name=table)
