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
        return self.mapPartitions(lambda i: [sum(1 for _ in i)])._sum()

    def _sum(self):
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
        return self.mapPartitions(func)

    def flatMap(self, f, preservesPartitioning=False):
        """
        Pass each value in the key-value pair DStream through flatMap function
        without changing the keys: this also retains the original RDD's partition.
        """
        def func(s, iterator):
            return chain.from_iterable(imap(f, iterator))
        return self._mapPartitionsWithIndex(func, preservesPartitioning)

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
        return self._mapPartitionsWithIndex(func, preservesPartitioning)

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
                     numPartitions=None):
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
        locally_combined = self.mapPartitions(combineLocally)
        shuffled = locally_combined.partitionBy(numPartitions)

        def _mergeCombiners(iterator):
            combiners = {}
            for (k, v) in iterator:
                if k not in combiners:
                    combiners[k] = v
                else:
                    combiners[k] = mergeCombiners(combiners[k], v)
            return combiners.iteritems()

        return shuffled.mapPartitions(_mergeCombiners)

    def partitionBy(self, numPartitions, partitionFunc=portable_hash):
        """
        Return a copy of the DStream partitioned using the specified partitioner.
        """
        if numPartitions is None:
            numPartitions = self.ctx._defaultReducePartitions()

        # Transferring O(n) objects to Java is too expensive.  Instead, we'll
        # form the hash buckets in Python, transferring O(numPartitions) objects
        # to Java.  Each object is a (splitNumber, [objects]) pair.

        outputSerializer = self.ctx._unbatched_serializer
#
#        def add_shuffle_key(split, iterator):
#            buckets = defaultdict(list)
#
#            for (k, v) in iterator:
#                buckets[partitionFunc(k) % numPartitions].append((k, v))
#            for (split, items) in buckets.iteritems():
#                yield pack_long(split)
#                yield outputSerializer.dumps(items)
#        keyed = PipelinedDStream(self, add_shuffle_key)

        limit = (_parse_memory(self.ctx._conf.get(
            "spark.python.worker.memory", "512m")) / 2)

        def add_shuffle_key(split, iterator):

            buckets = defaultdict(list)
            c, batch = 0, min(10 * numPartitions, 1000)

            for k, v in iterator:
                buckets[partitionFunc(k) % numPartitions].append((k, v))
                c += 1

                # check used memory and avg size of chunk of objects
                if (c % 1000 == 0 and get_used_memory() > limit
                        or c > batch):
                    n, size = len(buckets), 0
                    for split in buckets.keys():
                        yield pack_long(split)
                        d = outputSerializer.dumps(buckets[split])
                        del buckets[split]
                        yield d
                        size += len(d)

                    avg = (size / n) >> 20
                    # let 1M < avg < 10M
                    if avg < 1:
                        batch *= 1.5
                    elif avg > 10:
                        batch = max(batch / 1.5, 1)
                    c = 0

            for split, items in buckets.iteritems():
                yield pack_long(split)
                yield outputSerializer.dumps(items)

        keyed = self._mapPartitionsWithIndex(add_shuffle_key)




        keyed._bypass_serializer = True
        with SCCallSiteSync(self.ctx) as css:
            partitioner = self.ctx._jvm.PythonPartitioner(numPartitions,
                                                          id(partitionFunc))
            jdstream = self.ctx._jvm.PythonPairwiseDStream(keyed._jdstream.dstream(),
                                                           partitioner).asJavaDStream()
        dstream = DStream(jdstream, self._ssc, BatchedSerializer(outputSerializer))
        # This is required so that id(partitionFunc) remains unique, even if
        # partitionFunc is a lambda:
        dstream._partitionFunc = partitionFunc
        return dstream

    def _defaultReducePartitions(self):
        """
        Returns the default number of partitions to use during reduce tasks (e.g., groupBy).
        If spark.default.parallelism is set, then we'll use the value from SparkContext
        defaultParallelism, otherwise we'll use the number of partitions in this RDD

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
        # TODO: remove hard coding. RDD has NumPartitions. How do we get the number of partition
        # through DStream?
        return 2

    def foreachRDD(self, func):
        """
        Apply userdefined function to all RDD in a DStream.
        This python implementation could be expensive because it uses callback server
        in order to apply function to RDD in DStream.
        This is an output operator, so this DStream will be registered as an output
        stream and there materialized.
        """
        wrapped_func = RDDFunction(self.ctx, self._jrdd_deserializer, func)
        self.ctx._jvm.PythonForeachDStream(self._jdstream.dstream(), wrapped_func)

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
        def createCombiner(x):
            return [x]

        def mergeValue(xs, x):
            xs.append(x)
            return xs

        def mergeCombiners(a, b):
            a.extend(b)
            return a

        return self.combineByKey(createCombiner, mergeValue, mergeCombiners,
                                 numPartitions).mapValues(lambda x: ResultIterable(x))

    def countByValue(self):
        """
        Return new DStream which contains the count of each unique value in this
        DStreeam as a (value, count) pairs.
        """
        def countPartition(iterator):
            counts = defaultdict(int)
            for obj in iterator:
                counts[obj] += 1
            yield counts

        def mergeMaps(m1, m2):
            for (k, v) in m2.iteritems():
                m1[k] += v
            return m1

        return self.mapPartitions(countPartition).reduce(mergeMaps).flatMap(lambda x: x.items())

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

    def _test_output(self, result):
        """
        This function is only for test case.
        Store data in a DStream to result to verify the result in test case
        """
        def get_output(rdd, time):
            """
            Closure to get element in RDD in the DStream.
            This closure is called by py4j callback server.
            """
            collected = rdd.collect()
            result.append(collected)

        self.foreachRDD(get_output)


# TODO: implement updateStateByKey
# TODO: implement slice

# Window Operations
# TODO: implement window
# TODO: implement groupByKeyAndWindow
# TODO: implement reduceByKeyAndWindow
# TODO: implement countByValueAndWindow
# TODO: implement countByWindow
# TODO: implement reduceByWindow

# Transform Operation
# TODO: implement transform
# TODO: implement transformWith
# Following operation has dependency with transform
# TODO: implement union
# TODO: implement repertitions
# TODO: implement cogroup
# TODO: implement join
# TODO: implement leftOuterJoin
# TODO: implement rightOuterJoin


class PipelinedDStream(DStream):
    """
    Since PipelinedDStream is same to PipelindRDD, if PipliedRDD is changed,
    this code should be changed in the same way.
    """
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
        self.is_checkpointed = False
        self._ssc = prev._ssc
        self.ctx = prev.ctx
        self.prev = prev
        self._jdstream_val = None
        self._jrdd_deserializer = self.ctx.serializer
        self._bypass_serializer = False
        self._partitionFunc = prev._partitionFunc if self.preservesPartitioning else None

    @property
    def _jdstream(self):
        if self._jdstream_val:
            return self._jdstream_val
        if self._bypass_serializer:
            self.jrdd_deserializer = NoOpSerializer()
        command = (self.func, self._prev_jrdd_deserializer,
                   self._jrdd_deserializer)
        # the serialized command will be compressed by broadcast
        ser = CloudPickleSerializer()
        pickled_command = ser.dumps(command)
        if pickled_command > (1 << 20):  # 1M
            broadcast = self.ctx.broadcast(pickled_command)
            pickled_command = ser.dumps(broadcast)
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
                                                     self.ctx.pythonExec,
                                                     broadcast_vars, self.ctx._javaAccumulator,
                                                     class_tag)
        self._jdstream_val = python_dstream.asJavaDStream()
        return self._jdstream_val

    def _is_pipelinable(self):
        return not (self.is_cached or self.is_checkpointed)
