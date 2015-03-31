'''
Imports the hooks dynamically while keeping the package API clean,
abstracting the underlying modules
'''

import imp as _imp
import os as _os
_hooks = {
    'hive_hooks': [
        'HiveCliHook',
        'HiveMetastoreHook',
        'HiveServer2Hook',
    ],
    'presto_hook': ['PrestoHook'],
    'mysql_hook': ['MySqlHook'],
    'postgres_hook': ['PostgresHook'],
    'samba_hook': ['SambaHook'],
    'S3_hook': ['S3Hook'],
}

def f():
    __all__ = []
    for mod, hks in _hooks.items():
        #try:
        f, filename, description = _imp.find_module(mod, [_os.path.dirname(__file__)])
        module = _imp.load_module(mod, f, filename, description)
        for hk in hks:
            globals()[hk] = getattr(module, hk)
            __all__ += [hk]
        #except:
        #    pass
f()
del f
