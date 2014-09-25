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

from pyspark import RDD
from pyspark.serializers import NoOpSerializer,\
    BatchedSerializer, CloudPickleSerializer, pack_long,\
    CompressedSerializer
from pyspark.storagelevel import StorageLevel
from pyspark.resultiterable import ResultIterable
from pyspark.streaming.util import rddToFileName, RDDFunction
from pyspark.rdd import portable_hash, _parse_memory
from pyspark.traceback_utils import SCCallSiteSync

from py4j.java_collections import ListConverter, MapConverter

__all__ = ["DStream"]


class DStream(object):
    def __init__(self, jdstream, ssc, jrdd_deserializer):
        self._jdstream = jdstream
        self._ssc = ssc
        self.ctx = ssc._sc
        self._jrdd_deserializer = jrdd_deserializer
        self.is_cached = False
        self.is_checkpointed = False
        self._partitionFunc = None

    def context(self):
        """
        Return the StreamingContext associated with this DStream
        """
        return self._ssc

    def count(self):
        """
        Return a new DStream which contains the number of elements in this DStream.
        """
        return self.mapPartitions(lambda i: [sum(1 for _ in i)]).sum()

    def sum(self):
        """
        Add up the elements in this DStream.
        """
        return self.mapPartitions(lambda x: [sum(x)]).reduce(operator.add)

    def print_(self, label=None):
        """
        Since print is reserved name for python, we cannot define a "print" method function.
        This function prints serialized data in RDD in DStream because Scala and Java cannot
        deserialized pickled python object. Please use DStream.pyprint() to print results.

        Call DStream.print() and this function will print byte array in the DStream
        """
        # a hack to call print function in DStream
        getattr(self._jdstream, "print")(label)

    def filter(self, f):
        """
        Return a new DStream containing only the elements that satisfy predicate.
        """
        def func(iterator):
            return ifilter(f, iterator)
        return self.mapPartitions(func, True)

    def flatMap(self, f, preservesPartitioning=False):
        """
        Pass each value in the key-value pair DStream through flatMap function
        without changing the keys: this also retains the original RDD's partition.
        """
        def func(s, iterator):
            return chain.from_iterable(imap(f, iterator))
        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def map(self, f, preservesPartitioning=False):
        """
        Return a new DStream by applying a function to each element of DStream.
        """
        def func(iterator):
            return imap(f, iterator)
        return self.mapPartitions(func, preservesPartitioning)

    def mapPartitions(self, f, preservesPartitioning=False):
        """
        Return a new DStream by applying a function to each partition of this DStream.
        """
        def func(s, iterator):
            return f(iterator)
        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        """
        Return a new DStream by applying a function to each partition of this DStream,
        while tracking the index of the original partition.
        """
        return self.transform(lambda rdd: rdd.mapPartitionsWithIndex(f, preservesPartitioning))

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
                     numPartitions=None):
        """
        Count the number of elements for each key, and return the result to the
        master as a dictionary
        """
        def func(rdd):
            return rdd.combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions)
        return self.transform(func)

    def partitionBy(self, numPartitions, partitionFunc=portable_hash):
        """
        Return a copy of the DStream partitioned using the specified partitioner.
        """
        return self.transform(lambda rdd: rdd.partitionBy(numPartitions, partitionFunc))

    def foreach(self, func):
        return self.foreachRDD(lambda rdd, _: rdd.foreach(func))

    def foreachRDD(self, func):
        """
        Apply userdefined function to all RDD in a DStream.
        This python implementation could be expensive because it uses callback server
        in order to apply function to RDD in DStream.
        This is an output operator, so this DStream will be registered as an output
        stream and there materialized.
        """
        jfunc = RDDFunction(self.ctx, lambda a, b, t: func(a, t), self._jrdd_deserializer)
        self.ctx._jvm.PythonForeachDStream(self._jdstream.dstream(), jfunc)

    def pyprint(self):
        """
        Print the first ten elements of each RDD generated in this DStream. This is an output
        operator, so this DStream will be registered as an output stream and there materialized.
        """
        def takeAndPrint(rdd, time):
            """
            Closure to take element from RDD and print first 10 elements.
            This closure is called by py4j callback server.
            """
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

    def mapValues(self, f):
        """
        Pass each value in the key-value pair RDD through a map function
        without changing the keys; this also retains the original RDD's
        partitioning.
        """
        map_values_fn = lambda (k, v): (k, f(v))
        return self.map(map_values_fn, preservesPartitioning=True)

    def flatMapValues(self, f):
        """
        Pass each value in the key-value pair RDD through a flatMap function
        without changing the keys; this also retains the original RDD's
        partitioning.
        """
        flat_map_fn = lambda (k, v): ((k, x) for x in f(v))
        return self.flatMap(flat_map_fn, preservesPartitioning=True)

    def glom(self):
        """
        Return a new DStream in which RDD is generated by applying glom() to RDD of
        this DStream. Applying glom() to an RDD coalesces all elements within each partition into
        an list.
        """
        def func(iterator):
            yield list(iterator)
        return self.mapPartitions(func)

    def cache(self):
        """
        Persist this DStream with the default storage level (C{MEMORY_ONLY_SER}).
        """
        self.is_cached = True
        self.persist(StorageLevel.MEMORY_ONLY_SER)
        return self

    def persist(self, storageLevel):
        """
        Set this DStream's storage level to persist its values across operations
        after the first time it is computed. This can only be used to assign
        a new storage level if the DStream does not have a storage level set yet.
        """
        self.is_cached = True
        javaStorageLevel = self.ctx._getJavaStorageLevel(storageLevel)
        self._jdstream.persist(javaStorageLevel)
        return self

    def checkpoint(self, interval):
        """
        Mark this DStream for checkpointing. It will be saved to a file inside the
        checkpoint directory set with L{SparkContext.setCheckpointDir()}

        @param interval: Time interval after which generated RDD will be checkpointed
               interval has to be pyspark.streaming.duration.Duration
        """
        self.is_checkpointed = True
        self._jdstream.checkpoint(interval._jduration)
        return self

    def groupByKey(self, numPartitions=None):
        """
        Return a new DStream which contains group the values for each key in the
        DStream into a single sequence.
        Hash-partitions the resulting RDD with into numPartitions partitions in
        the DStream.

        Note: If you are grouping in order to perform an aggregation (such as a
        sum or average) over each key, using reduceByKey will provide much
        better performance.

        """
        return self.transform(lambda rdd: rdd.groupByKey(numPartitions))

    def countByValue(self):
        """
        Return new DStream which contains the count of each unique value in this
        DStreeam as a (value, count) pairs.
        """
        return self.map(lambda x: (x, None)).reduceByKey(lambda x, y: None).count()

    def saveAsTextFiles(self, prefix, suffix=None):
        """
        Save this DStream as a text file, using string representations of elements.
        """

        def saveAsTextFile(rdd, time):
            """
            Closure to save element in RDD in DStream as Pickled data in file.
            This closure is called by py4j callback server.
            """
            path = rddToFileName(prefix, suffix, time)
            rdd.saveAsTextFile(path)

        return self.foreachRDD(saveAsTextFile)

    def saveAsPickleFiles(self, prefix, suffix=None):
        """
        Save this DStream as a SequenceFile of serialized objects. The serializer
        used is L{pyspark.serializers.PickleSerializer}, default batch size
        is 10.
        """

        def saveAsPickleFile(rdd, time):
            """
            Closure to save element in RDD in the DStream as Pickled data in file.
            This closure is called by py4j callback server.
            """
            path = rddToFileName(prefix, suffix, time)
            rdd.saveAsPickleFile(path)

        return self.foreachRDD(saveAsPickleFile)

    def collect(self):
        result = []

        def get_output(rdd, time):
            r = rdd.collect()
            result.append(r)
        self.foreachRDD(get_output)
        return result

    def transform(self, func):
        return TransformedRDD(self, lambda a, b, t: func(a), cache=True)

    def transformWith(self, func, other):
        return TransformedRDD(self, lambda a, b, t: func(a, b), other)

    def transformWithTime(self, func):
        return TransformedRDD(self, lambda a, b, t: func(a, t))

    def repartitions(self, numPartitions):
        return self.transform(lambda rdd: rdd.repartition(numPartitions))

    def union(self, other):
        return self.transformWith(lambda a, b: a.union(b), other)

    def cogroup(self, other):
        return self.transformWith(lambda a, b: a.cogroup(b), other)

    def leftOuterJoin(self, other):
        return self.transformWith(lambda a, b: a.leftOuterJion(b), other)

    def rightOuterJoin(self, other):
        return self.transformWith(lambda a, b: a.rightOuterJoin(b), other)

    def slice(self, fromTime, toTime):
        jrdds = self._jdstream.slice(fromTime._jtime, toTime._jtime)
        # FIXME: serializer
        return [RDD(jrdd, self.ctx, self.ctx.serializer) for jrdd in jrdds]

    def updateStateByKey(self, updateFunc):
        # FIXME: convert updateFunc to java JFunction2
        jFunc = updateFunc
        return self._jdstream.updateStateByKey(jFunc)


# Window Operations
# TODO: implement window
# TODO: implement groupByKeyAndWindow
# TODO: implement reduceByKeyAndWindow
# TODO: implement countByValueAndWindow
# TODO: implement countByWindow
# TODO: implement reduceByWindow


class TransformedRDD(DStream):
    # TODO: better name for cache
    def __init__(self, prev, func, other=None, cache=False):
        # TODO: combine transformed RDD
        ssc = prev._ssc
        t = RDDFunction(ssc._sc, func, prev._jrdd_deserializer)
        jdstream = ssc._jvm.PythonTransformedDStream(prev._jdstream.dstream(),
                                                     other and other._jdstream, t, cache)
        DStream.__init__(self, jdstream.asJavaDStream(), ssc, ssc._sc.serializer)
