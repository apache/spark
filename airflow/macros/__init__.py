from random import random
from datetime import datetime, timedelta
import time
import hive
import uuid


def ds_add(ds, days):
    '''
    Add or subtract days from a YYYY-MM-DD

    :param ds: anchor date in ``YYYY-MM-DD`` format to add to
    :type ds: str
    :param days: number of days to add to the ds, you can use negative values
    :type days: int

    >>> ds_add('2015-01-01', 5)
    '2015-01-06'
    >>> ds_add('2015-01-06', -5)
    '2015-01-01'
    '''

    ds = datetime.strptime(ds, '%Y-%m-%d')
    if days:
        ds = ds + timedelta(days)
    return ds.isoformat()[:10]
