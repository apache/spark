from airflow.configuration import conf
import datetime


def max_partition(
        table, schema="default",
        hive_conn_id=conf.get('hooks', 'HIVE_DEFAULT_CONN_ID')):
    '''
    Gets the max partition for a table.

    :param schema: The hive schema the table lives in
    :type schema: string
    :param table: The hive table you are interested in, supports the dot
        notation as in "my_database.my_table", if a dot is found,
        the schema param is disregarded
    :type table: string
    :param hive_conn_id: The hive connection you are interested in.
        If your default is set you don't need to use this parameter.
    :type hive_conn_id: string
    '''
    from airflow.hooks.hive_hook import HiveHook
    if '.' in table:
        schema, table = table.split('.')
    hh = HiveHook(hive_conn_id=hive_conn_id)
    return hh.max_partition(schema=schema, table_name=table)


def _closest_date(target_dt, date_list, before_target=None):
    '''
    This function finds the date in a list closest to the target date.
    An optional paramter can be given to get the closest before or after.

    :param target_dt: The target date
    :type target_dt: datetime.date
    :param date_list: The list of dates to search
    :type date_list: datetime.date list
    :param before_target: closest before or after the target
    :type before_target: bool or None
    :returns: The closest date
    :rtype: datetime.date or None
    '''
    fb = lambda d: d - target_dt if d > target_dt else datetime.timedelta.max
    fa = lambda d: d - target_dt if d < target_dt else datetime.timedelta.min
    fnone = lambda d: target_dt - d if d < target_dt else d - target_dt
    if before_target is None:
        return min(date_list, key=fnone).date()
    if before_target:
        return min(date_list, key=fb).date()
    else:
        return min(date_list, key=fa).date()


def closest_ds_partition(
        table, ds, before=True, schema="default",
        hive_conn_id=conf.get('hooks', 'HIVE_DEFAULT_CONN_ID')):
    '''
    This function finds the date in a list closest to the target date.
    An optional paramter can be given to get the closest before or after.

    :param table: A hive table name
    :type table: str
    :param ds: A datestamp '%Y-%m-%d' i.e. 'yyyy-mm-dd'
    :type ds: datetime.date list
    :param before: closest before (True), after (False) or either side of ds
    :type before: bool or None
    :returns: The closest date
    :rtype: str or None

    '''
    from airflow.hooks.hive_hook import HiveHook
    if '.' in table:
        schema, table = table.split('.')
    target_dt = datetime.datetime.strptime(ds, '%Y-%m-%d')
    hh = HiveHook(hive_conn_id=hive_conn_id)
    partitions = hh.get_partitions(schema=schema, table_name=table)
    parts = [datetime.datetime.strptime(p, '%Y-%m-%d') for p in partitions]
    if partitions is None or partitions == []:
        return None
    if ds in partitions:
        return ds
    else:
        closest_ds = _closest_date(target_dt, parts, before_target=before)
        return closest_ds.isoformat()
