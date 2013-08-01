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

>>> from pyspark.broadcast import _broadcastRegistry
>>> _broadcastRegistry[b.bid] = b
>>> from cPickle import dumps, loads
>>> loads(dumps(b)).value
[1, 2, 3, 4, 5]

>>> sc.parallelize([0, 0]).flatMap(lambda x: b.value).collect()
[1, 2, 3, 4, 5, 1, 2, 3, 4, 5]

>>> large_broadcast = sc.broadcast(list(range(10000)))
"""
# Holds broadcasted data received from Java, keyed by its id.
_broadcastRegistry = {}


def _from_id(bid):
    from pyspark.broadcast import _broadcastRegistry
    if bid not in _broadcastRegistry:
        raise Exception("Broadcast variable '%s' not loaded!" % bid)
    return _broadcastRegistry[bid]


class Broadcast(object):
    def __init__(self, bid, value, java_broadcast=None, pickle_registry=None):
        self.value = value
        self.bid = bid
        self._jbroadcast = java_broadcast
        self._pickle_registry = pickle_registry

    def __reduce__(self):
        self._pickle_registry.add(self)
        return (_from_id, (self.bid, ))
