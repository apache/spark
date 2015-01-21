from airflow.configuration import conf

def max_partition(
        table, schema="default",
        hive_conn_id=conf.get('hooks', 'HIVE_DEFAULT_CONN_ID')):
    from airflow.hooks.hive_hook import HiveHook
    if '.' in table:
        schema, table = table.split('.')
    hh = HiveHook(hive_conn_id=hive_conn_id)
    return hh.max_partition(schema=schema, table_name=table)

def closest_ds_partition(
        table, ds_partition, schema="default",
        hive_conn_id=conf.get('hooks', 'HIVE_DEFAULT_CONN_ID')):
    from airflow.hooks.hive_hook import HiveHook
    if '.' in table:
        schema, table = table.split('.')
    hh = HiveHook(hive_conn_id=hive_conn_id)
    partitions = hh.get_partitions(schema=schema, table_name=table)
    if ds_partition in partitions:
        return ds_partition
    else:
        return max(partitions)
