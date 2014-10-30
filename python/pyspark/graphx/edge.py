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
Python bindings for GraphX.
"""
import operator
import itertools
from pyspark.graphx.partitionstrategy import PartitionStrategy
from pyspark import RDD, StorageLevel
from pyspark.rdd import PipelinedRDD

__all__ = ["EdgeRDD", "Edge"]


class Edge(object):
    """
    Edge object contains a source vertex id, target vertex id and edge properties
    """

    def __init__(self, src_id, tgt_id, edge_property):
        self._src_id = src_id
        self._tgt_id = tgt_id
        self._property = edge_property

    @property
    def srcId(self):
        return self._src_id

    @property
    def tgtId(self):
        return self._tgt_id

    def asTuple(self):
        return (self._src_id, self._tgt_id, self._property)

    def __str__(self):
        return self._src_id + self._tgt_id + self._property


class EdgeRDD(RDD):
    """
    EdgeRDD class is used to enter the vertex class for GraphX
    """

    def __init__(self, jrdd, ctx, jrdd_deserializer):
        self._jrdd = jrdd
        self._ctx = ctx
        self._jrdd_deserializer = jrdd_deserializer
        self._name = "VertexRDD"

    # TODO: Does not work
    def __repr__(self):
        return RDD(self._jrdd, self._ctx, self._jrdd_deserializer).take(1).__repr__()

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY_SER):
        return self._jrdd.persist(storageLevel)

    def cache(self):
        self._jrdd.cache()

    def count(self):
        return self._jrdd.count()

    def collect(self):
        return self._jrdd.collect()

    def take(self, num=10):
        return self._jrdd.take(num)

    def sum(self):
        """
        Add up the elements in this RDD.

        >>> sc.parallelize([1.0, 2.0, 3.0]).sum()
        6.0
        """
        return self.mapPartitions(lambda x: [sum(x)]).reduce(operator.add)

    def mapValues(self, f, preservesPartitioning=False):
        """
        Return a new RDD by applying a function to each element of this RDD.

        >>> rdd = sc.parallelize(["b", "a", "c"])
        >>> sorted(rdd.map(lambda x: (x, 1)).collect())
        [('a', 1), ('b', 1), ('c', 1)]
        """
        def func(_, iterator):
            return itertools.imap(f, iterator)
        return self.mapVertexPartitions(func, preservesPartitioning)

    def filter(self, f):
        return self._jrdd.filter(f)

    def innerJoin(self, f):
        return self._jrdd.innerJoin(f)

    def leftOuterJoin(self, other, numPartitions=None):
        return self._jrdd.leftOuterJoin(other, numPartitions)