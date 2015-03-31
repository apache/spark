'''
Imports operators dynamically while keeping the package API clean,
abstracting the underlying modules
'''
from airflow.utils import import_module_attrs as _import_module_attrs

_operators = {
    'bash_operator': ['BashOperator'],
    'python_operator': ['PythonOperator'],
    'hive_operator': ['HiveOperator'],
    'presto_check_operator': [
        'PrestoCheckOperator',
        'PrestoValueCheckOperator',
        'PrestoIntervalCheckOperator',
    ],
    'dummy_operator': ['DummyOperator'],
    'email_operator': ['EmailOperator'],
    'hive2samba_operator': ['Hive2SambaOperator'],
    'mysql_operator': ['MySqlOperator'],
    'postgres_operator': ['PostgresOperator'],
    'sensors': [
        'SqlSensor',
        'ExternalTaskSensor',
        'HivePartitionSensor',
        'S3KeySensor',
        'S3PrefixSensor',
        'HdfsSensor',
        'TimeSensor',
    ],
    'subdag_operator': ['SubDagOperator'],
    }

_import_module_attrs(globals(), _operators)
