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

import itertools
import os
from py4j.java_collections import MapConverter, ListConverter
from tempfile import NamedTemporaryFile
from types import TupleType, IntType
import operator
from numpy.numarray.numerictypes import Long
from pyspark.accumulators import PStatsParam
from pyspark.rdd import PipelinedRDD
from pyspark.serializers import CloudPickleSerializer, NoOpSerializer
from pyspark import RDD, PickleSerializer, StorageLevel
from pyspark.graphx.partitionstrategy import PartitionStrategy
from pyspark.sql import StringType, LongType
from pyspark.traceback_utils import SCCallSiteSync

__all__ = ["VertexRDD", "VertexId", "Vertex"]


"""
Vertex id type is long by default.
Defining a type for that enables
us to override it in future if
need be
"""
VertexId = Long


class Vertex(object):
    """
    Vertex class is a tuple of (VertexId and VertexProperty)
    """
    def __init__(self, vertex_id, vertex_property):
        self._id = VertexId(vertex_id)
        self._property = vertex_property

    @property
    def property(self):
        return self._property

    def asTuple(self):
        return (self._id, self._property)

    def __str__(self):
        return self._id + self._property


class VertexRDD(object):
    """
    VertexRDD class defines vertex operations/transformation and vertex properties
    The schema of the vertex properties are specified as a tuple to the vertex
    The vertex operations are mapValues, filter, diff, innerJoin, leftOuterJoin
    and aggergateUsingIndex. These operations are mapped to Scala functions defined
    in PythonVertexRDD class in [[org.apache.spark.graphx.api.python package]]
    """

    def __init__(self, jrdd, ctx, jrdd_deserializer):
        self._jrdd = jrdd
        self._ctx = ctx
        self._jrdd_deserializer = jrdd_deserializer
        self._preserve_partitioning = False
        self._name = "VertexRDD"
        self.is_cached = False
        self.is_checkpointed = False
        self._id = jrdd.id()
        self._partitionFunc = None
        self._jrdd_val = None
        self._bypass_serializer = False
        self._jrdd_val = self.toVertexRDD(jrdd, ctx, jrdd_deserializer)


    # TODO: Does not work
    def __repr__(self):
        return self._jrdd.toString()

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY_SER):
        return self._jrdd.persist(storageLevel)

    def cache(self):
        self._jrdd.cache()

    def count(self):
        return self._jrdd.count()

    # def collect(self):
    #     return self._jrdd.collect()

    def collect(self):
        print "in collect() of vertex.py"
        """
        Return a list that contains all of the elements in this RDD.
        """
        # with SCCallSiteSync(self._ctx) as css:
        bytesInJava = self._jrdd.collect().iterator()
        return list(self._collect_iterator_through_file(bytesInJava))

    def _collect_iterator_through_file(self, iterator):
        # Transferring lots of data through Py4J can be slow because
        # socket.readline() is inefficient.  Instead, we'll dump the data to a
        # file and read it back.
        tempFile = NamedTemporaryFile(delete=False, dir=self._ctx._temp_dir)
        tempFile.close()
        self._ctx._writeToFile(iterator, tempFile.name)
        # Read the data into Python and deserialize it:
        with open(tempFile.name, 'rb') as tempFile:
            for item in self._jrdd_deserializer.load_stream(tempFile):
                yield item
        os.unlink(tempFile.name)

    def take(self, num=10):
        return self._jrdd.take(num)

    def sum(self):
        self._jrdd.sum()

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

    def diff(self, other):
        """
        Return a new RDD containing only the elements that satisfy a predicate.

        >>> rdd1 = sc.parallelize([1, 2, 3, 4, 5])
        >>> rdd2 = sc.parallelize([2, 3, 4])
        >>> rdd1.diff(rdd2).collect()
        [1, 5]
        """
        self._jrdd = self._jrdd._jvm.org.apache.spark.PythonVertexRDD()
        return self._jrdd._jvm.org.apache.spark.PythonVertexRDD.diff(other)

    def leftJoin(self, other):
        return self._jrdd._jvm.org.apache.spark.PythonVertexRDD.leftJoin()

    def innerJoin(self, other, func):
        return self._jrdd._jvm.org.apache.spark.PythonVertexRDD.innerJoin()

    def aggregateUsingIndex(self, other, reduceFunc):
        return self._jrdd._jvm.org.apache.spark.PythonVertexRDD.aggregateUsingIndex()

    def mapVertexPartitions(self, f, preservesPartitioning=False):
        """
        Return a new RDD by applying a function to each partition of this RDD.

        >>> rdd = sc.parallelize([1, 2, 3, 4], 2)
        >>> def f(iterator): yield sum(iterator)
        >>> rdd.mapPartitions(f).collect()
        [3, 7]
        """
        def func(s, iterator):
            return f(iterator)
        return self._jrdd.mapPartitionsWithIndex(func, preservesPartitioning)

    def reduce(self, f):
        """
        Reduces the elements of this RDD using the specified commutative and
        associative binary operator. Currently reduces partitions locally.

        >>> from operator import add
        >>> sc.parallelize([1, 2, 3, 4, 5]).reduce(add)
        15
        >>> sc.parallelize((2 for _ in range(10))).map(lambda x: 1).cache().reduce(add)
        10
        >>> sc.parallelize([]).reduce(add)
        Traceback (most recent call last):
            ...
        ValueError: Can not reduce() empty RDD
        """
        def func(iterator):
            iterator = iter(iterator)
            try:
                initial = next(iterator)
            except StopIteration:
                return
            yield reduce(f, iterator, initial)

        vals = self.mapVertexPartitions(func).collect()
        if vals:
            return reduce(f, vals)
        raise ValueError("Can not reduce() empty RDD")

    def toVertexRDD(self, jrdd, ctx, jrdd_deserializer):
        if self._jrdd_val:
            return self._jrdd_val
        if self._bypass_serializer:
            self._jrdd_deserializer = NoOpSerializer()
        enable_profile = self._ctx._conf.get("spark.python.profile", "false") == "true"
        profileStats = self._ctx.accumulator(None, PStatsParam) if enable_profile else None
        command = (self._jrdd_deserializer)
        # the serialized command will be compressed by broadcast
        ser = CloudPickleSerializer()
        pickled_command = ser.dumps(command)
        if len(pickled_command) > (1 << 20):  # 1M
            self._broadcast = self._ctx.broadcast(pickled_command)
            pickled_command = ser.dumps(self._broadcast)
        broadcast_vars = ListConverter().convert(
            [x._jbroadcast for x in self._ctx._pickled_broadcast_vars],
            self._ctx._gateway._gateway_client)
        self._ctx._pickled_broadcast_vars.clear()
        env = MapConverter().convert(self._ctx.environment,
                                     self._ctx._gateway._gateway_client)
        includes = ListConverter().convert(self._ctx._python_includes,
                                           self._ctx._gateway._gateway_client)
        python_rdd = self._ctx._jvm.PythonVertexRDD(jrdd._jrdd,
                                                    bytearray(pickled_command),
                                                    env, includes, self._preserve_partitioning,
                                                    self._ctx.pythonExec,
                                                    broadcast_vars, self._ctx._javaAccumulator)
        if enable_profile:
            self._id = self._jrdd_val.id()
            self._ctx._add_profile(self._id, profileStats)

        return python_rdd.asJavaRDD()

    def id(self):
        """
        A unique ID for this RDD (within its SparkContext).
        """
        return self._id


class PipelinedVertexRDD(VertexRDD):

    """
    Pipelined maps:

    >>> rdd = sc.parallelize([1, 2, 3, 4])
    >>> rdd.map(lambda x: 2 * x).cache().map(lambda x: 2 * x).collect()
    [4, 8, 12, 16]
    >>> rdd.map(lambda x: 2 * x).map(lambda x: 2 * x).collect()
    [4, 8, 12, 16]

    Pipelined reduces:
    >>> from operator import add
    >>> rdd.map(lambda x: 2 * x).reduce(add)
    20
    >>> rdd.flatMap(lambda x: [x, x]).reduce(add)
    20
    """

    def __init__(self, prev, func, preservesPartitioning=False):
        if not isinstance(prev, PipelinedVertexRDD) or not prev._is_pipelinable():
            # This transformation is the first in its stage:
            self.func = func
            self.preservesPartitioning = preservesPartitioning
            self._prev_jrdd = prev._jrdd
            self._prev_jrdd_deserializer = prev._jrdd_deserializer
        else:
            prev_func = prev.func

            def pipeline_func(split, iterator):
                return func(split, prev_func(split, iterator))
            self.func = pipeline_func
            self.preservesPartitioning = \
                prev.preservesPartitioning and preservesPartitioning
            self._prev_jrdd = prev._prev_jrdd  # maintain the pipeline
            self._prev_jrdd_deserializer = prev._prev_jrdd_deserializer
        self.is_cached = False
        self.is_checkpointed = False
        self._ctx = prev._ctx
        self.prev = prev
        self._jrdd_val = None
        self._id = None
        self._jrdd_deserializer = self._ctx.serializer
        self._bypass_serializer = False
        self._partitionFunc = prev._partitionFunc if self.preservesPartitioning else None
        self._broadcast = None

    def __del__(self):
        if self._broadcast:
            self._broadcast.unpersist()
            self._broadcast = None

    @property
    def _jrdd(self):
        print "in _jrdd of vertex.py"
        if self._jrdd_val:
            return self._jrdd_val
        if self._bypass_serializer:
            self._jrdd_deserializer = NoOpSerializer()
        enable_profile = self._ctx._conf.get("spark.python.profile", "false") == "true"
        profileStats = self._ctx.accumulator(None, PStatsParam) if enable_profile else None
        command = (self.func, profileStats, self._prev_jrdd_deserializer,
                   self._jrdd_deserializer)
        # the serialized command will be compressed by broadcast
        ser = CloudPickleSerializer()
        pickled_command = ser.dumps(command)
        if len(pickled_command) > (1 << 20):  # 1M
            self._broadcast = self._ctx.broadcast(pickled_command)
            pickled_command = ser.dumps(self._broadcast)
        broadcast_vars = ListConverter().convert(
            [x._jbroadcast for x in self._ctx._pickled_broadcast_vars],
            self._ctx._gateway._gateway_client)
        self._ctx._pickled_broadcast_vars.clear()
        env = MapConverter().convert(self._ctx.environment,
                                     self._ctx._gateway._gateway_client)
        includes = ListConverter().convert(self._ctx._python_includes,
                                           self._ctx._gateway._gateway_client)
        python_rdd = self._ctx._jvm.PythonVertexRDD(self._prev_jrdd.rdd(),
                                             bytearray(pickled_command),
                                             env, includes, self.preservesPartitioning,
                                             self._ctx.pythonExec,
                                             broadcast_vars, self._ctx._javaAccumulator)
        self._jrdd_val = python_rdd.asJavaRDD()

        if enable_profile:
            self._id = self._jrdd_val.id()
            self._ctx._add_profile(self._id, profileStats)
        return self._jrdd_val

    def id(self):
        if self._id is None:
            self._id = self._jrdd.id()
        return self._id

    def _is_pipelinable(self):
        return not (self.is_cached or self.is_checkpointed)
