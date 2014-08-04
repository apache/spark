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

from collections import defaultdict
from itertools import chain, ifilter, imap
import operator

from pyspark.serializers import NoOpSerializer,\
    BatchedSerializer, CloudPickleSerializer, pack_long
from pyspark.rdd import _JavaStackTrace

from py4j.java_collections import ListConverter, MapConverter

__all__ = ["DStream"]


class DStream(object):
    def __init__(self, jdstream, ssc, jrdd_deserializer):
        self._jdstream = jdstream
        self._ssc = ssc
        self.ctx = ssc._sc
        self._jrdd_deserializer = jrdd_deserializer

    def count(self):
        """
        Return a new DStream which contains the number of elements in this DStream.
        """
        return self._mapPartitions(lambda i: [sum(1 for _ in i)])._sum()

    def _sum(self):
        """
        Add up the elements in this DStream.
        """
        return self._mapPartitions(lambda x: [sum(x)]).reduce(operator.add)

    def print_(self):
        """
        Since print is reserved name for python, we cannot define a print method function.
        This function prints serialized data in RDD in DStream because Scala and Java cannot
        deserialized pickled python object. Please use DStream.pyprint() instead to print results.

        Call DStream.print().
        """
        # a hack to call print function in DStream
        getattr(self._jdstream, "print")()

    def filter(self, f):
        """
        Return a new DStream containing only the elements that satisfy predicate.
        """
        def func(iterator): return ifilter(f, iterator)
        return self._mapPartitions(func)

    def flatMap(self, f, preservesPartitioning=False):
        """
        Pass each value in the key-value pair DStream through flatMap function
        without changing the keys: this also retains the original RDD's partition.
        """
        def func(s, iterator): return chain.from_iterable(imap(f, iterator))
        return self._mapPartitionsWithIndex(func, preservesPartitioning)

    def map(self, f):
        """
        Return a new DStream by applying a function to each element of DStream.
        """
        def func(iterator): return imap(f, iterator)
        return self._mapPartitions(func)

    def _mapPartitions(self, f):
        """
        Return a new DStream by applying a function to each partition of this DStream.
        """
        def func(s, iterator): return f(iterator)
        return self._mapPartitionsWithIndex(func)

    def _mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        """
        Return a new DStream by applying a function to each partition of this DStream,
        while tracking the index of the original partition.
        """
        return PipelinedDStream(self, f, preservesPartitioning)

    def reduce(self, func):
        """
        Return a new DStream by reduceing the elements of this RDD using the specified
        commutative and associative binary operator.
        """
        return self.map(lambda x: (None, x)).reduceByKey(func, 1).map(lambda x: x[1])

    def reduceByKey(self, func, numPartitions=None):
        """
        Merge the value for each key using an associative reduce function.

        This will also perform the merging locally on each mapper before
        sending results to reducer, similarly to a "combiner" in MapReduce.

        Output will be hash-partitioned with C{numPartitions} partitions, or
        the default parallelism level if C{numPartitions} is not specified.
        """
        return self.combineByKey(lambda x: x, func, func, numPartitions)

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                      numPartitions = None):
        """
        Count the number of elements for each key, and return the result to the
        master as a dictionary
        """
        if numPartitions is None:
            numPartitions = self._defaultReducePartitions()

        def combineLocally(iterator):
            combiners = {}
            for x in iterator:
                (k, v) = x
                if k not in combiners:
                    combiners[k] = createCombiner(v)
                else:
                    combiners[k] = mergeValue(combiners[k], v)
            return combiners.iteritems()
        locally_combined = self._mapPartitions(combineLocally)
        shuffled = locally_combined.partitionBy(numPartitions)

        def _mergeCombiners(iterator):
            combiners = {}
            for (k, v) in iterator:
                if not k in combiners:
                    combiners[k] = v
                else:
                    combiners[k] = mergeCombiners(combiners[k], v)

        return shuffled._mapPartitions(_mergeCombiners)

    def partitionBy(self, numPartitions, partitionFunc=None):
        """
        Return a copy of the DStream partitioned using the specified partitioner.
        """
        if numPartitions is None:
            numPartitions = self.ctx._defaultReducePartitions()

        if partitionFunc is None:
            partitionFunc = lambda x: 0 if x is None else hash(x)

        # Transferring O(n) objects to Java is too expensive.  Instead, we'll
        # form the hash buckets in Python, transferring O(numPartitions) objects
        # to Java.  Each object is a (splitNumber, [objects]) pair.
        outputSerializer = self.ctx._unbatched_serializer
        def add_shuffle_key(split, iterator):

            buckets = defaultdict(list)

            for (k, v) in iterator:
                buckets[partitionFunc(k) % numPartitions].append((k, v))
            for (split, items) in buckets.iteritems():
                yield pack_long(split)
                yield outputSerializer.dumps(items)
        keyed = PipelinedDStream(self, add_shuffle_key)
        keyed._bypass_serializer = True
        with _JavaStackTrace(self.ctx) as st:
            partitioner = self.ctx._jvm.PythonPartitioner(numPartitions,
                                                      id(partitionFunc))
            jdstream = self.ctx._jvm.PairwiseDStream(keyed._jdstream.dstream(), partitioner).asJavaDStream()
        dstream = DStream(jdstream, self._ssc, BatchedSerializer(outputSerializer))
        # This is required so that id(partitionFunc) remains unique, even if
        # partitionFunc is a lambda:
        dstream._partitionFunc = partitionFunc
        return dstream

    def _defaultReducePartitions(self):
        """
        Returns the default number of partitions to use during reduce tasks (e.g., groupBy).
        If spark.default.parallelism is set, then we'll use the value from SparkContext
        defaultParallelism, otherwise we'll use the number of partitions in this RDD.

        This mirrors the behavior of the Scala Partitioner#defaultPartitioner, intended to reduce
        the likelihood of OOMs. Once PySpark adopts Partitioner-based APIs, this behavior will
        be inherent.
        """
        if self.ctx._conf.contains("spark.default.parallelism"):
            return self.ctx.defaultParallelism
        else:
            return self.getNumPartitions()

    def getNumPartitions(self):
        """
        Return the number of partitions in RDD
        """
        # TODO: remove hardcoding. RDD has NumPartitions but DStream does not have.
        return 2

    def foreachRDD(self, func):
        """
        """
        from utils import RDDFunction
        wrapped_func = RDDFunction(self.ctx, self._jrdd_deserializer, func)
        self.ctx._jvm.PythonForeachDStream(self._jdstream.dstream(), wrapped_func)

    def pyprint(self):
        """
        Print the first ten elements of each RDD generated in this DStream. This is an output
        operator, so this DStream will be registered as an output stream and there materialized.

        """
        def takeAndPrint(rdd, time):
            taken = rdd.take(11)
            print "-------------------------------------------"
            print "Time: %s" % (str(time))
            print "-------------------------------------------"
            for record in taken[:10]:
                print record
            if len(taken) > 10:
                print "..."
            print

        self.foreachRDD(takeAndPrint)

    #def transform(self, func):
    #    from utils import RDDFunction
    #    wrapped_func = RDDFunction(self.ctx, self._jrdd_deserializer, func)
    #    jdstream = self.ctx._jvm.PythonTransformedDStream(self._jdstream.dstream(), wrapped_func).toJavaDStream
    #    return DStream(jdstream, self._ssc, ...)  ## DO NOT KNOW HOW 

class PipelinedDStream(DStream):
    def __init__(self, prev, func, preservesPartitioning=False):
        if not isinstance(prev, PipelinedDStream) or not prev._is_pipelinable():
            # This transformation is the first in its stage:
            self.func = func
            self.preservesPartitioning = preservesPartitioning
            self._prev_jdstream = prev._jdstream
            self._prev_jrdd_deserializer = prev._jrdd_deserializer
        else:
            prev_func = prev.func
            def pipeline_func(split, iterator):
                return func(split, prev_func(split, iterator))
            self.func = pipeline_func
            self.preservesPartitioning = \
                prev.preservesPartitioning and preservesPartitioning
            self._prev_jdstream = prev._prev_jdstream  # maintain the pipeline
            self._prev_jrdd_deserializer = prev._prev_jrdd_deserializer
        self.is_cached = False
        self._ssc = prev._ssc
        self.ctx = prev.ctx
        self.prev = prev
        self._jdstream_val = None
        self._jrdd_deserializer = self.ctx.serializer
        self._bypass_serializer = False

    @property
    def _jdstream(self):
        if self._jdstream_val:
            return self._jdstream_val
        if self._bypass_serializer:
            serializer = NoOpSerializer()
        else:
            serializer = self.ctx.serializer

        command = (self.func, self._prev_jrdd_deserializer, serializer)
        pickled_command = CloudPickleSerializer().dumps(command)
        broadcast_vars = ListConverter().convert(
            [x._jbroadcast for x in self.ctx._pickled_broadcast_vars],
            self.ctx._gateway._gateway_client)
        self.ctx._pickled_broadcast_vars.clear()
        class_tag = self._prev_jdstream.classTag()
        env = MapConverter().convert(self.ctx.environment,
                                     self.ctx._gateway._gateway_client)
        includes = ListConverter().convert(self.ctx._python_includes,
                                     self.ctx._gateway._gateway_client)
        python_dstream = self.ctx._jvm.PythonDStream(self._prev_jdstream.dstream(),
                bytearray(pickled_command),
                env, includes, self.preservesPartitioning,
                self.ctx.pythonExec, broadcast_vars, self.ctx._javaAccumulator,
                class_tag)
        self._jdstream_val = python_dstream.asJavaDStream()
        return self._jdstream_val

    def _is_pipelinable(self):
        return not (self.is_cached)
