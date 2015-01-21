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
Python bindings for EdgeRDD in GraphX
"""

import os
import itertools
from tempfile import NamedTemporaryFile
# from build.py4j.java_collections import MapConverter, ListConverter
from pyspark.accumulators import PStatsParam
from pyspark import RDD, StorageLevel
from pyspark.serializers import BatchedSerializer, PickleSerializer, CloudPickleSerializer, \
    NoOpSerializer
from pyspark.traceback_utils import SCCallSiteSync

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


class EdgeRDD(object):
    def __init__(self, jrdd,
                 jrdd_deserializer = BatchedSerializer(PickleSerializer())):
        """
        Constructor
        :param jrdd:               A JavaRDD reference passed from the parent
                                   RDD object
        :param jrdd_deserializer:  The deserializer used in Python workers
                                   created from PythonRDD to execute a
                                   serialized Python function and RDD

        """

        self._jrdd = jrdd
        self._ctx = jrdd._jrdd.context
        self._jrdd_deserializer = jrdd_deserializer
        self._preserve_partitioning = False
        self._name = "VertexRDD"
        self._is_cached = False
        self._is_checkpointed = False
        self._id = jrdd.id()
        self._partitionFunc = None
        self._jrdd_val = None
        self._bypass_serializer = False


    def id(self):
        """
        VertexRDD has a unique id
        """
        return self._id

    # TODO: Does not work
    def __repr__(self):
        return self._jrdd.toString()

    def context(self):
        return self._ctx

    def cache(self):
        """
        Persist this vertex RDD with the default storage level (C{MEMORY_ONLY_SER}).
        """
        self.is_cached = True
        self.persist(StorageLevel.MEMORY_ONLY_SER)
        return self

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY_SER):
        self._is_cached = True
        javaStorageLevel = self.ctx._getJavaStorageLevel(storageLevel)
        self._jrdd.persist(javaStorageLevel)
        return self

    def unpersist(self):
        self._is_cached = False
        self._jrdd.unpersist()
        return self

    def checkpoint(self):
        self.is_checkpointed = True
        self._jrdd.rdd().checkpoint()

    def count(self):
        return self._jrdd.count()

    def collect(self):
        """
        Return all of the elements in this vertex RDD as a list
        """
        with SCCallSiteSync(self._ctx) as css:
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

    def mapValues(self, f, preserves_partitioning=False):
        """
        Return a new vertex RDD by applying a function to each vertex attributes,
        preserving the index

        >>> rdd = sc.parallelize([Edge(1, 2, "b"), (2, 3, "a"), (3, 2, "c")])
        >>> vertices = EdgeRDD(rdd)
        >>> sorted(edges.mapValues(lambda x: (x + ":" + x)).collect())
        [(1, 2, 'a:a'), (2, 3, 'b:b'), (3, 2, 'c:c')]
        """
        map_func = lambda (k, v): (k, f(v))
        def func(_, iterator):
            return itertools.imap(map_func, iterator)
        return PipelinedEdgeRDD(self, func, preserves_partitioning)

    def mapEdgePartitions(self, f, preserve_partitioning=False):
        def func(s, iterator):
            return f(iterator)
        return PipelinedEdgeRDD(self, func, preserve_partitioning)

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
        return self.mapEdgePartitions(func, True)

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
        return self.maEdgePartitions(func, True)

    def innerJoin(self, other, numPartitions=None):
        def dispatch(seq):
            vbuf, wbuf = [], []
            for (n, v) in seq:
                if n == 1:
                    vbuf.append(v)
                elif n == 2:
                    wbuf.append(v)
            return [(v, w) for v in vbuf for w in wbuf]
        vs = self.map(lambda (k, v): (k, (1, v)))
        ws = other.map(lambda (k, v): (k, (2, v)))
        return vs.union(ws).groupByKey(numPartitions).flatMapValues(lambda x: dispatch(x.__iter__()))



class PipelinedEdgeRDD(EdgeRDD):

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
        if not isinstance(prev, PipelinedEdgeRDD) or not prev._is_pipelinable():
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
        print "in _jrdd of edge.py"
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
        targetStorageLevel = StorageLevel.MEMORY_ONLY
        python_rdd = self._ctx._jvm.PythonEdgeRDD(self._prev_jrdd.rdd(),
                                                    bytearray(pickled_command),
                                                    env, includes, self.preservesPartitioning,
                                                    self._ctx.pythonExec,
                                                    broadcast_vars, self._ctx._javaAccumulator,
                                                    targetStorageLevel)
        self._jrdd_val = python_rdd.asJavaEdgeRDD()

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
