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
from tempfile import NamedTemporaryFile
from numpy.numarray.numerictypes import Long

from py4j.java_collections import MapConverter, ListConverter
from pyspark.accumulators import PStatsParam
from pyspark.rdd import PipelinedRDD
from pyspark.serializers import CloudPickleSerializer, NoOpSerializer, AutoBatchedSerializer
from pyspark import RDD, PickleSerializer, StorageLevel, SparkContext
from pyspark.traceback_utils import SCCallSiteSync


__all__ = ["VertexRDD", "VertexId"]


"""
Vertex id type is long by default.
Defining a type for that enables
us to override it in future if
need be
"""
VertexId = Long


class VertexRDD(object):
    """
    VertexRDD class defines the vertex operations/transformation. The list of
    vertex transformations and actions are available at
    `http://spark.apache.org/docs/latest/graphx-programming-guide.html`
    These operations are mapped to Scala functions defined
    in `org.apache.spark.graphx.api.python.PythonVertexRDD`
    """

    def __init__(self, jrdd, jrdd_deserializer=AutoBatchedSerializer(PickleSerializer())):
        """
        Constructor
        :param jrdd:               A JavaRDD reference passed from the parent
                                   RDD object
        :param jrdd_deserializer:  The deserializer used in Python workers
                                   created from PythonRDD to execute a
                                   serialized Python function and RDD

        """

        self.name = "VertexRDD"
        self.jrdd = jrdd
        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = SparkContext._active_spark_context
        self.jvertex_rdd_deserializer = jrdd_deserializer
        self.id = jrdd.id()
        self.partitionFunc = None
        self.bypass_serializer = False
        self.preserve_partitioning = False

        self.jvertex_rdd = self.getJavaVertexRDD(jrdd, jrdd_deserializer)

    def __repr__(self):
        return self.jvertex_rdd.toString()

    @property
    def context(self):
        return self.ctx

    def cache(self):
        """
        Persist this vertex RDD with the default storage level (C{MEMORY_ONLY_SER}).
        """
        self.is_cached = True
        self.persist(StorageLevel.MEMORY_ONLY_SER)
        return self

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY_SER):
        self.is_cached = True
        java_storage_level = self.context._getJavaStorageLevel(storageLevel)
        self.jvertex_rdd.persist(java_storage_level)
        return self

    def unpersist(self):
        self.is_cached = False
        self.jvertex_rdd.unpersist()
        return self

    def checkpoint(self):
        self.is_checkpointed = True
        self.jvertex_rdd.rdd().checkpoint()

    def count(self):
        return self.mapPartitions(lambda i: [sum(1 for _ in i)]).sum()
        # return self.jvertex_rdd.count()

    def take(self, num=10):
        return self.jrdd.take(num)

    def sum(self):
        self.jvertex_rdd.sum()

    def mapValues(self, f, preserves_partitioning=False):
        """
        Return a new vertex RDD by applying a function to each vertex attributes,
        preserving the index

        >>> rdd = sc.parallelize([(1, "b"), (2, "a"), (3, "c")])
        >>> vertices = VertexRDD(rdd)
        >>> sorted(vertices.mapValues(lambda x: (x + ":" + x)).collect())
        [(1, 'a:a'), (2, 'b:b'), (3, 'c:c')]
        """
        map_func = lambda (k, v): (k, f(v))
        def func(_, iterator):
            return itertools.imap(map_func, iterator)
        return PipelinedVertexRDD(self, func, preserves_partitioning)

    def mapVertexPartitions(self, f, preserve_partitioning=False):
        def func(s, iterator):
            return f(iterator)
        return PipelinedVertexRDD(self, func, preserve_partitioning)

    def filter(self, f):
        """
        Return a new vertex RDD containing only the elements that satisfy a predicate.

        >>> rdd = sc.parallelize([(1, "b"), (2, "a"), (3, "c")])
        >>> vertices = VertexRDD(rdd)
        >>> vertices.filter(lambda x: x._1 % 2 == 0).collect()
        [2]
        """
        def func(iterator):
            return itertools.ifilter(f, iterator)
        return self.mapVertexPartitions(func, True)

    def diff(self, other, numPartitions=2):
        """
        Hides vertices that are the same between `this` and `other`.
        For vertices that are different, keeps the values from `other`.

        TODO: give an example
        """
        if (isinstance(other, RDD)):
            vs = self.map(lambda (k, v): (k, (1, v)))
            ws = other.map(lambda (k, v): (k, (2, v)))
        return vs.union(ws).groupByKey(numPartitions).mapValues(lambda x: x.diff(x.__iter__()))

    def leftJoin(self, other, numPartitions=None):
        def dispatch(seq):
            vbuf, wbuf = [], []
            for (n, v) in seq:
                if n == 1:
                    vbuf.append(v)
                elif n == 2:
                    wbuf.append(v)
            if not wbuf:
                wbuf.append(None)
            return [(v, w) for v in vbuf for w in wbuf]
        vs = self.map(lambda (k, v): (k, (1, v)))
        ws = other.map(lambda (k, v): (k, (2, v)))
        return vs.union(ws).groupByKey(numPartitions)\
                .flatMapValues(lambda x: dispatch(x.__iter__()))


    def innerJoin(self, other, numPartitions=None):
        def dispatch(seq):
            vbuf, wbuf = [], []
            for (n, v) in seq:
                if n == 1:
                    vbuf.append(v)
                    vbuf.append(v)
                elif n == 2:
                    wbuf.append(v)
            return [(v, w) for v in vbuf for w in wbuf]
        vs = self.map(lambda (k, v): (k, (1, v)))
        ws = other.map(lambda (k, v): (k, (2, v)))
        return vs.union(ws).groupByKey(numPartitions).flatMapValues(lambda x: dispatch(x.__iter__()))



    # def aggregateUsingIndex(self, other, reduceFunc):
    #     return self._jrdd._jvm.org.apache.spark.PythonVertexRDD.aggregateUsingIndex()

    def collect(self):
        """
        Return a list that contains all of the elements in this RDD.
        """
        with SCCallSiteSync(self.context) as css:
            bytesInJava = self.jrdd.collect()
        return list(bytesInJava)

    def getJavaVertexRDD(self, rdd, rdd_deserializer):
        if self.bypass_serializer:
            self.jvertex_rdd_deserializer = NoOpSerializer()
        enable_profile = self.context._conf.get("spark.python.profile", "false") == "true"
        profileStats = self.context.accumulator(None, PStatsParam) if enable_profile else None

        # the serialized command will be compressed by broadcast
        broadcast_vars = ListConverter().convert(
            [x._jbroadcast for x in self.context._pickled_broadcast_vars],
            self.context._gateway._gateway_client)
        self.context._pickled_broadcast_vars.clear()
        env = MapConverter().convert(self.context.environment,
                                     self.context._gateway._gateway_client)
        includes = ListConverter().convert(self.context._python_includes,
                                           self.context._gateway._gateway_client)
        target_storage_level = StorageLevel.MEMORY_ONLY
        java_storage_level = self.context._getJavaStorageLevel(target_storage_level)
        jvertex_rdd = self.context._jvm.PythonVertexRDD(rdd._jrdd,
                                                   bytearray(" "),
                                                   env, includes, self.preserve_partitioning,
                                                   self.context.pythonExec,
                                                   broadcast_vars, self.context._javaAccumulator,
                                                   java_storage_level)

        if enable_profile:
            self.id = self.jvertex_rdd.id()
            self.context._add_profile(self.id, profileStats)
        return jvertex_rdd


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
        if isinstance(prev, PipelinedRDD) or not prev._is_pipelinable():
            # This transformation is the first in its stage:
            self.func = func
            self.preservesPartitioning = preservesPartitioning
            self.prev_jvertex_rdd = prev._prev_jrdd
            self.prev_jvertex_rdd_deserializer = prev._prev_jrdd_deserializer
        elif isinstance(prev, PipelinedVertexRDD) or isinstance(prev, RDD):
            prev_func = prev.func

            def pipeline_func(split, iterator):
                return func(split, prev_func(split, iterator))
            self.func = pipeline_func
            self.preservesPartitioning = \
                prev.preservesPartitioning and preservesPartitioning
            self.prev_jvertex_rdd = prev.prev_jvertex_rdd
            self.prev_jvertex_rdd_deserializer = prev.prev_jvertex_rdd_deserializer
        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = prev.ctx
        self.prev = prev
        self._jrdd_val = None
        self._id = None
        self._jrdd_deserializer = self.ctx.serializer
        self._bypass_serializer = False
        self._partitionFunc = prev._partitionFunc if self.preservesPartitioning else None
        self._broadcast = None

    def __del__(self):
        if self._broadcast:
            self._broadcast.unpersist()
            self._broadcast = None

    @property
    def jvertex_rdd(self):
        print "**********************************"
        print "in jvertex_rdd of vertex.py"
        print "**********************************"
        if self._jrdd_val:
            return self._jrdd_val
        if self._bypass_serializer:
            self._jrdd_deserializer = NoOpSerializer()
        enable_profile = self.ctx._conf.get("spark.python.profile", "false") == "true"
        profileStats = self.ctx.accumulator(None, PStatsParam) if enable_profile else None
        command = (self.func, profileStats, self._prev_jrdd_deserializer,
                   self._jrdd_deserializer)
        # the serialized command will be compressed by broadcast
        ser = CloudPickleSerializer()
        pickled_command = ser.dumps(command)
        if len(pickled_command) > (1 << 20):  # 1M
            self._broadcast = self.ctx.broadcast(pickled_command)
            pickled_command = ser.dumps(self._broadcast)
        broadcast_vars = ListConverter().convert(
            [x._jbroadcast for x in self.ctx._pickled_broadcast_vars],
            self.ctx._gateway._gateway_client)
        self.ctx._pickled_broadcast_vars.clear()
        env = MapConverter().convert(self.ctx.environment,
                                     self.ctx._gateway._gateway_client)
        includes = ListConverter().convert(self.ctx._python_includes,
                                           self.ctx._gateway._gateway_client)
        target_storage_level = StorageLevel.MEMORY_ONLY
        python_rdd = self.ctx._jvm.PythonVertexRDD(self._prev_jrdd.rdd(),
                                             bytearray(pickled_command),
                                             env, includes, self.preservesPartitioning,
                                             self.ctx.pythonExec,
                                             broadcast_vars, self.ctx._javaAccumulator,
                                             target_storage_level)
        self._jrdd_val = python_rdd.asJavaVertexRDD()

        if enable_profile:
            self._id = self._jrdd_val.id()
            self.ctx._add_profile(self._id, profileStats)
        return self._jrdd_val

    def id(self):
        if self._id is None:
            self._id = self._jrdd.id()
        return self._id

    def is_pipelinable(self):
        return not (self.is_cached or self.is_checkpointed)
