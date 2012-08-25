"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> b = sc.broadcast([1, 2, 3, 4, 5])
>>> b.value
[1, 2, 3, 4, 5]

>>> from pyspark.broadcast import _broadcastRegistry
>>> _broadcastRegistry[b.uuid] = b
>>> from cPickle import dumps, loads
>>> loads(dumps(b)).value
[1, 2, 3, 4, 5]

>>> sc.parallelize([0, 0]).flatMap(lambda x: b.value).collect()
[1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
"""
# Holds broadcasted data received from Java, keyed by UUID.
_broadcastRegistry = {}


def _from_uuid(uuid):
    from pyspark.broadcast import _broadcastRegistry
    if uuid not in _broadcastRegistry:
        raise Exception("Broadcast variable '%s' not loaded!" % uuid)
    return _broadcastRegistry[uuid]


class Broadcast(object):
    def __init__(self, uuid, value, java_broadcast=None, pickle_registry=None):
        self.value = value
        self.uuid = uuid
        self._jbroadcast = java_broadcast
        self._pickle_registry = pickle_registry

    def __reduce__(self):
        self._pickle_registry.add(self)
        return (_from_uuid, (self.uuid, ))


def _test():
    import doctest
    doctest.testmod()


if __name__ == "__main__":
    _test()
