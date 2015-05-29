#coding: utf-8
'''
Compatibility functions for unified behavior between Python 2.x and 3.x.

:author: Alex Grönholm
'''
from __future__ import unicode_literals, absolute_import

import inspect
import sys

if sys.version_info[0] < 3:
    items = lambda d: d.items()
    iteritems = lambda d: d.iteritems()
    next = lambda x: x.next()
    range = xrange
    long = long
    basestring = basestring
    unicode = unicode
    bytearray2 = bytearray
    unichr = unichr
    bytestr = str
    tobytestr = str
    isbytestr = lambda s: isinstance(s, str)
    ispython3bytestr = lambda s: False
    isbytearray = lambda s: isinstance(s, bytearray)
    bytetoint = lambda b: ord(b)
    bytetostr = lambda b: b
    strtobyte = lambda b: b
else:
    items = lambda d: list(d.items())
    iteritems = lambda d: d.items()
    next = next
    range = range
    long = int
    basestring = str
    unicode = str
    bytearray2 = bytes
    unichr = chr
    bytestr = bytes
    tobytestr = lambda s: bytes(s, 'ascii')
    isbytestr = lambda s: isinstance(s, bytes)
    ispython3bytestr = lambda s: isinstance(s, bytes)
    isbytearray = lambda s: isinstance(s, bytearray)
    bytetoint = lambda b: b
    bytetostr = lambda b: str(b, encoding='ascii')
    strtobyte = lambda s: bytes(s, encoding='ascii')

if hasattr(inspect, 'getattr_static'):
    hasattr2 = lambda obj, attr: bool(inspect.getattr_static(obj, attr, False))
else:
    hasattr2 = hasattr
