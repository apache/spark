'''
Imports operators dynamically while keeping the package API clean,
abstracting the underlying modules
'''

import imp as _imp
import os as _os

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

def f():
    __all__ = []
    for mod, ops in _operators.items():
        try:
            f, filename, description = _imp.find_module(mod, [_os.path.dirname(__file__)])
            module = _imp.load_module(mod, f, filename, description)
            for op in ops:
                globals()[op] = getattr(module, op)
                __all__ += [op]
        except:
            pass
f()
del f
