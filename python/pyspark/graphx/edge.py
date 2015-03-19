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
from py4j.java_collections import ListConverter, MapConverter
# from pyspark.accumulators import PStatsParam
from pyspark import RDD, StorageLevel, SparkContext
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
    """
    EdgeRDD class defines the edge actions and transformations. The complete list of
    transformations and actions is available at
    `http://spark.apache.org/docs/latest/graphx-programming-guide.html`
    These operations are mapped to Scala functions defined
    in `org.apache.spark.graphx.impl.EdgeRDDImpl`
    """

    def __init__(self, jrdd, jrdd_deserializer = BatchedSerializer(PickleSerializer())):
        """
        Constructor
        :param jrdd:               A JavaRDD reference passed from the parent
                                   RDD object
        :param jrdd_deserializer:  The deserializer used in Python workers
                                   created from PythonRDD to execute a
                                   serialized Python function and RDD

        """

        self.name = "EdgeRDD"
        self.jrdd = jrdd
        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = SparkContext._active_spark_context
        self.jedge_rdd_deserializer = jrdd_deserializer
        self.id = jrdd.id()
        self.partitionFunc = None
        self.bypass_serializer = False
        self.preserve_partitioning = False

        self.jedge_rdd = self.getJavaEdgeRDD(jrdd, jrdd_deserializer)

    def __repr__(self):
        return self.jedge_rdd.toString()

    def cache(self):
        """
        Persist this vertex RDD with the default storage level (C{MEMORY_ONLY_SER}).
        """
        self.is_cached = True
        self.persist(StorageLevel.MEMORY_ONLY_SER)
        return self

    def checkpoint(self):
        self.is_checkpointed = True
        self.jedge_rdd.checkpoint()

    def count(self):
        return self.jedge_rdd.count()

    def isCheckpointed(self):
        """
        Return whether this RDD has been checkpointed or not
        """
        return self.is_checkpointed

    def mapValues(self, f, preserves_partitioning=False):
        """
        Return a new vertex RDD by applying a function to each vertex attributes,
        preserving the index

        >>> rdd = sc.parallelize([(1, "b"), (2, "a"), (3, "c")])
        >>> edges = EdgeRDD(rdd)
        >>> sorted(edges.mapValues(lambda x: (x + ":" + x)).collect())
        [(1, 'a:a'), (2, 'b:b'), (3, 'c:c')]
        """
        def func(_, iterator):
            return itertools.imap(lambda (k, v): (k, f(v)), iterator)
        return PipelinedEdgeRDD(self, func, preserves_partitioning)

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY_SER):
        self.is_cached = True
        java_storage_level = self.ctx._getJavaStorageLevel(storageLevel)
        self.jedge_rdd.persist(java_storage_level)
        return self

    # TODO: This is a hack. take() must call JavaVertexRDD.take()
    def take(self, num=10):
        return self.jrdd.take(num)

    def unpersist(self, blocking = False):
        self.is_cached = False
        self.jedge_rdd.unpersist(blocking)
        return self

    def mapEdgePartitions(self, f, preserve_partitioning=False):
        def func(s, iterator):
            return f(iterator)
        return PipelinedEdgeRDD(self, func, preserve_partitioning)

    # TODO: The best way to do an innerJoin on vertex RDDs is to use the optimized inner
    # TODO: technique defined in VertexRDDImpl. This solution does not scale
    def innerJoin(self, other):
        return self.jrdd.join(other.jrdd)

    def leftJoin(self, other, numPartitions=None):
        return self.jrdd.leftOuterJoin(other.jrdd, numPartitions)

    def collect(self):
        """
        Return a list that contains all of the elements in this RDD.
        """
        with SCCallSiteSync(self.ctx) as css:
            bytesInJava = self.jedge_rdd.collect().iterator()
        return list(self._collect_iterator_through_file(bytesInJava))

    def _collect_iterator_through_file(self, iterator):
        # Transferring lots of data through Py4J can be slow because
        # socket.readline() is inefficient.  Instead, we'll dump the data to a
        # file and read it back.
        tempFile = NamedTemporaryFile(delete=False, dir=self.ctx._temp_dir)
        tempFile.close()
        self.ctx._writeToFile(iterator, tempFile.name)
        # Read the data into Python and deserialize it:
        with open(tempFile.name, 'rb') as tempFile:
            for item in self.jedge_rdd_deserializer.load_stream(tempFile):
                yield item
        os.unlink(tempFile.name)

    def getJavaEdgeRDD(self, rdd, rdd_deserializer):
        if self.bypass_serializer:
            self.jedge_rdd_deserializer = NoOpSerializer()
            rdd_deserializer = NoOpSerializer()
        # enable_profile = self.ctx._conf.get("spark.python.profile", "false") == "true"
        # profileStats = self.ctx.accumulator(None, PStatsParam) if enable_profile else None
        def f(index, iterator):
            return iterator
        command = (f, rdd_deserializer, rdd_deserializer)
        # the serialized command will be compressed by broadcast
        ser = CloudPickleSerializer()
        pickled_command = ser.dumps(command)
        if len(pickled_command) > (1 << 20):  # 1M
            self.broadcast = self.ctx.broadcast(pickled_command)
            pickled_command = ser.dumps(self.broadcast)

        # the serialized command will be compressed by broadcast
        broadcast_vars = ListConverter().convert(
            [x._jbroadcast for x in self.ctx._pickled_broadcast_vars],
            self.ctx._gateway._gateway_client)
        self.ctx._pickled_broadcast_vars.clear()
        env = MapConverter().convert(self.ctx.environment,
                                     self.ctx._gateway._gateway_client)
        includes = ListConverter().convert(self.ctx._python_includes,
                                           self.ctx._gateway._gateway_client)
        java_storage_level = self.ctx._getJavaStorageLevel(StorageLevel.MEMORY_ONLY)
        prdd = self.ctx._jvm.PythonEdgeRDD(rdd._jrdd,
                                             bytearray(pickled_command),
                                             env, includes, self.preserve_partitioning,
                                             self.ctx.pythonExec,
                                             broadcast_vars, self.ctx._javaAccumulator,
                                             java_storage_level)
        self.jedge_rdd = prdd.asJavaEdgeRDD()
        # if enable_profile:
        #     self.id = self.jedge_rdd.id()
        #     self.ctx._add_profile(self.id, profileStats)
        return self.jedge_rdd


class PipelinedEdgeRDD(EdgeRDD):

    """
    Pipelined mapValues in EdgeRDD:

    >>> rdd = sc.parallelize([(1, ("Alice", 29)), (2, ("Bob", 30)), \
                              (3, ("Charlie", 31)), (4, ("Dwayne", 32))])
    >>> vertices = VertexRDD(rdd)
    >>> vertices.mapValues(lambda x: x[1] * 2).cache().collect()
    [(1, ("Alice", 58)), (2, ("Bob", 60)), \
     (3, ("Charlie", 62)), (4, ("Dwayne", 64))]

    Pipelined reduces in EdgeRDD:
    >>> from operator import add
    >>> rdd.map(lambda x: 2 * x).reduce(add)
    20
    >>> rdd.flatMap(lambda x: [x, x]).reduce(add)
    20
    """

    def __init__(self, prev, func, preservesPartitioning=False):
        if not isinstance(prev, PipelinedEdgeRDD) or not prev.is_pipelinable():
            # This transformation is the first in its stage:
            self.func = func
            self.preservesPartitioning = preservesPartitioning
            self.prev_jedge_rdd = prev.jedge_rdd
            self.prev_jedge_rdd_deserializer = prev.jedge_rdd_deserializer
        else:
            prev_func = prev.func

            def pipeline_func(split, iterator):
                return func(split, prev_func(split, iterator))
            self.func = pipeline_func
            self.preservesPartitioning = \
                prev.preservesPartitioning and preservesPartitioning
            self.prev_jedge_rdd = prev.jedge_rdd
            self.prev_jedge_rdd_deserializer = prev.prev_jedge_rdd_deserializer

        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = prev.ctx
        self.prev = prev
        self.jerdd_val = None
        self.id = None
        self.jedge_rdd_deserializer = self.ctx.serializer
        self.bypass_serializer = False
        self.partitionFunc = prev._partitionFunc if self.preservesPartitioning else None
        self.broadcast = None

    def __del__(self):
        if self.broadcast:
            self.broadcast.unpersist()
            self.broadcast = None

    @property
    def jedge_rdd(self):
        if self.jerdd_val:
            return self.jerdd_val
        if self.bypass_serializer:
            self.jedge_rdd_deserializer = NoOpSerializer()
        enable_profile = self.ctx._conf.get("spark.python.profile", "false") == "true"
        profileStats = self.ctx.accumulator(None, PStatsParam) if enable_profile else None
        command = (self.func, profileStats, self.prev_jedge_rdd_deserializer,
                   self.jedge_rdd_deserializer)
        # the serialized command will be compressed by broadcast
        ser = CloudPickleSerializer()
        pickled_command = ser.dumps(command)
        if len(pickled_command) > (1 << 20):  # 1M
            self.broadcast = self.ctx.broadcast(pickled_command)
            pickled_command = ser.dumps(self.broadcast)
        broadcast_vars = ListConverter().convert(
            [x._jbroadcast for x in self.ctx._pickled_broadcast_vars],
            self.ctx._gateway._gateway_client)
        self.ctx._pickled_broadcast_vars.clear()
        env = MapConverter().convert(self.ctx.environment,
                                     self.ctx._gateway._gateway_client)
        includes = ListConverter().convert(self.ctx._python_includes,
                                           self.ctx._gateway._gateway_client)
        java_storage_level = self.ctx._getJavaStorageLevel(StorageLevel.MEMORY_ONLY)
        python_rdd = self.ctx._jvm.PythonEdgeRDD(self.prev_jedge_rdd,
                                                   bytearray(pickled_command),
                                                   env, includes, self.preservesPartitioning,
                                                   self.ctx.pythonExec,
                                                   broadcast_vars, self.ctx._javaAccumulator,
                                                   java_storage_level)
        self.jerdd_val = python_rdd.asJavaEdgeRDD()

        if enable_profile:
            self.id = self.jerdd_val.id()
            self.ctx._add_profile(self.id, profileStats)
        return self.jerdd_val

    def id(self):
        if self.id is None:
            self.id = self.jedge_rdd.id()
        return self.id

    def is_pipelinable(self):
        return not (self.is_cached or self.is_checkpointed)
