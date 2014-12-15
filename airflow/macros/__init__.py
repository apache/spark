from random import random
from datetime import datetime
import time
import hive


def ds_add(ds, days):
    '''Add or subtract days from a YYYY-MM-DD'''
    ds = datetime.strptime(ds, '%Y-%m-%d')
    if days:
        ds = ds + timedelta(days)
    return ds.isoformat()[:10]
