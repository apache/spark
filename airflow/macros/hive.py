from airflow.configuration import conf
import datetime

def max_partition(
        table, schema="default",
        hive_conn_id=conf.get('hooks', 'HIVE_DEFAULT_CONN_ID')):
    from airflow.hooks.hive_hook import HiveHook
    if '.' in table:
        schema, table = table.split('.')
    hh = HiveHook(hive_conn_id=hive_conn_id)
    return hh.max_partition(schema=schema, table_name=table)

def closest_date(target_ds, ds_list, before_target = None):
    '''
    This function finds the date in a list closest to the target date.
    An optional paramter can be given to get the closest before or after.

    :param target_ds: The target date
    :type target_ds: datetime.date
    :param ds_list: The list of dates to search
    :type ds_list: datetime.date list
    :param before_target: closest before or after the target
    :type before_target: bool or None
    :returns: The closest date
    :rtype: datetime.date or None
    '''
    fb = lambda d: d - target_ds if d > target_ds else datetime.timedelta.max
    fa = lambda d: d - target_ds if d < target_ds else datetime.timedelta.min
    fnone = lambda d: target_ds - d if d < target_ds else d - target_ds
    if before is None:
        return min(ds_list, key = fnone )
    if before:
        return min(ds_list, key = fb )
    else:
        return min(ds_list, key = fa )


def closest_ds_partition(
        table, ds_partition, before=True, schema="default",
        hive_conn_id=conf.get('hooks', 'HIVE_DEFAULT_CONN_ID')):
    from airflow.hooks.hive_hook import HiveHook
    if '.' in table:
        schema, table = table.split('.')
    target_ds = datetime.datetime.strptime(ds_partition, '%Y-%m-%d')
    hh = HiveHook(hive_conn_id=hive_conn_id)
    partitions = hh.get_partitions(schema=schema, table_name=table)
    parts = [ datetime.datetime.strptime(p, '%Y-%m-%d') for p in partitions ]
    if partitions is None or partitions == []:
        return None
    if ds_partition in partitions:
        return ds_partition
    else:
        closest_ds = closest_date(target_ds, parts, before_target = before)
        return closest_ds.isoformat()
