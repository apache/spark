#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> b = sc.broadcast([1, 2, 3, 4, 5])
>>> b.value
[1, 2, 3, 4, 5]
>>> sc.parallelize([0, 0]).flatMap(lambda x: b.value).collect()
[1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
>>> b.unpersist()

>>> large_broadcast = sc.broadcast(list(range(10000)))
"""
import os

from pyspark.serializers import LargeObjectSerializer


__all__ = ['Broadcast']


# Holds broadcasted data received from Java, keyed by its id.
_broadcastRegistry = {}


def _from_id(bid):
    from pyspark.broadcast import _broadcastRegistry
    if bid not in _broadcastRegistry:
        raise Exception("Broadcast variable '%s' not loaded!" % bid)
    return _broadcastRegistry[bid]


class Broadcast(object):

    """
    A broadcast variable created with
    L{SparkContext.broadcast()<pyspark.context.SparkContext.broadcast>}.
    Access its value through C{.value}.
    """

    def __init__(self, bid, value, java_broadcast=None,
                 pickle_registry=None, path=None):
        """
        Should not be called directly by users -- use
        L{SparkContext.broadcast()<pyspark.context.SparkContext.broadcast>}
        instead.
        """
        self.bid = bid
        if path is None:
            self._value = value
        self._jbroadcast = java_broadcast
        self._pickle_registry = pickle_registry
        self.path = path

    @property
    def value(self):
        """ Return the broadcasted value
        """
        if not hasattr(self, "_value") and self.path is not None:
            ser = LargeObjectSerializer()
            self._value = ser.load_stream(open(self.path)).next()
        return self._value

    def unpersist(self, blocking=False):
        """
        Delete cached copies of this broadcast on the executors.
        """
        self._jbroadcast.unpersist(blocking)
        os.unlink(self.path)

    def __reduce__(self):
        self._pickle_registry.add(self)
        return (_from_id, (self.bid, ))


if __name__ == "__main__":
    import doctest
    doctest.testmod()
