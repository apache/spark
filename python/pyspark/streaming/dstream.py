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

from itertools import chain, ifilter, imap
import operator
import time
from datetime import datetime

from pyspark import RDD
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.util import rddToFileName, RDDFunction
from pyspark.rdd import portable_hash
from pyspark.resultiterable import ResultIterable

__all__ = ["DStream"]


class DStream(object):
    def __init__(self, jdstream, ssc, jrdd_deserializer):
        self._jdstream = jdstream
        self._ssc = ssc
        self.ctx = ssc._sc
        self._jrdd_deserializer = jrdd_deserializer
        self.is_cached = False
        self.is_checkpointed = False

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
        return self.foreachRDD(lambda _, rdd: rdd.foreach(func))

    def foreachRDD(self, func):
        """
        Apply userdefined function to all RDD in a DStream.
        This python implementation could be expensive because it uses callback server
        in order to apply function to RDD in DStream.
        This is an output operator, so this DStream will be registered as an output
        stream and there materialized.
        """
        jfunc = RDDFunction(self.ctx, func, self._jrdd_deserializer)
        api = self._ssc._jvm.PythonDStream
        api.callForeachRDD(self._jdstream, jfunc)

    def pprint(self):
        """
        Print the first ten elements of each RDD generated in this DStream. This is an output
        operator, so this DStream will be registered as an output stream and there materialized.
        """
        def takeAndPrint(timestamp, rdd):
            taken = rdd.take(11)
            print "-------------------------------------------"
            print "Time: %s" % datetime.fromtimestamp(timestamp / 1000.0)
            print "-------------------------------------------"
            for record in taken[:10]:
                print record
            if len(taken) > 10:
                print "..."
            print

        self.foreachRDD(takeAndPrint)

    def _first(self):
        """
        Return the first RDD in the stream.
        """
        return self._take(1)[0]

    def _take(self, n):
        """
        Return the first `n` RDDs in the stream (will start and stop).
        """
        results = []

        def take(_, rdd):
            if rdd and len(results) < n:
                results.extend(rdd.take(n - len(results)))

        self.foreachRDD(take)

        self._ssc.start()
        while len(results) < n:
            time.sleep(0.01)
        self._ssc.stop(False, True)
        return results

    def _collect(self):
        """
        Collect each RDDs into the returned list.

        :return: list, which will have the collected items.
        """
        result = []

        def get_output(_, rdd):
            r = rdd.collect()
            result.append(r)
        self.foreachRDD(get_output)
        return result

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
        Return a new DStream in which RDD is generated by applying glom()
        to RDD of this DStream. Applying glom() to an RDD coalesces all
        elements within each partition into an list.
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

        @param interval: time in seconds, after which generated RDD will
                         be checkpointed
        """
        self.is_checkpointed = True
        self._jdstream.checkpoint(self._ssc._jduration(interval))
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

        def saveAsTextFile(time, rdd):
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

        def saveAsPickleFile(time, rdd):
            """
            Closure to save element in RDD in the DStream as Pickled data in file.
            This closure is called by py4j callback server.
            """
            path = rddToFileName(prefix, suffix, time)
            rdd.saveAsPickleFile(path)

        return self.foreachRDD(saveAsPickleFile)

    def transform(self, func):
        """
        Return a new DStream in which each RDD is generated by applying a function
        on each RDD of 'this' DStream.
        """
        return TransformedDStream(self, lambda t, a: func(a), True)

    def transformWithTime(self, func):
        """
        Return a new DStream in which each RDD is generated by applying a function
        on each RDD of 'this' DStream.
        """
        return TransformedDStream(self, func, False)

    def transformWith(self, func, other, keepSerializer=False):
        """
        Return a new DStream in which each RDD is generated by applying a function
        on each RDD of 'this' DStream and 'other' DStream.
        """
        jfunc = RDDFunction(self.ctx, lambda t, a, b: func(a, b), self._jrdd_deserializer)
        dstream = self.ctx._jvm.PythonTransformed2DStream(self._jdstream.dstream(),
                                                          other._jdstream.dstream(), jfunc)
        jrdd_serializer = self._jrdd_deserializer if keepSerializer else self.ctx.serializer
        return DStream(dstream.asJavaDStream(), self._ssc, jrdd_serializer)

    def repartitions(self, numPartitions):
        """
        Return a new DStream with an increased or decreased level of parallelism. Each RDD in the
        returned DStream has exactly numPartitions partitions.
        """
        return self.transform(lambda rdd: rdd.repartition(numPartitions))

    @property
    def _slideDuration(self):
        """
        Return the slideDuration in seconds of this DStream
        """
        return self._jdstream.dstream().slideDuration().milliseconds() / 1000.0

    def union(self, other):
        """
        Return a new DStream by unifying data of another DStream with this DStream.
        @param other Another DStream having the same interval (i.e., slideDuration) as this DStream.
        """
        if self._slideDuration != other._slideDuration:
            raise ValueError("the two DStream should have same slide duration")
        return self.transformWith(lambda a, b: a.union(b), other, True)

    def cogroup(self, other, numPartitions=None):
        """
        Return a new DStream by applying 'cogroup' between RDDs of `this`
        DStream and `other` DStream.

        Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
        """
        return self.transformWith(lambda a, b: a.cogroup(b, numPartitions), other)

    def join(self, other, numPartitions=None):
        """
         Return a new DStream by applying 'join' between RDDs of `this` DStream and
        `other` DStream.

        Hash partitioning is used to generate the RDDs with `numPartitions`
         partitions.
        """
        return self.transformWith(lambda a, b: a.join(b, numPartitions), other)

    def leftOuterJoin(self, other, numPartitions=None):
        """
         Return a new DStream by applying 'left outer join' between RDDs of `this` DStream and
        `other` DStream.

        Hash partitioning is used to generate the RDDs with `numPartitions`
         partitions.
        """
        return self.transformWith(lambda a, b: a.leftOuterJoin(b, numPartitions), other)

    def rightOuterJoin(self, other, numPartitions=None):
        """
         Return a new DStream by applying 'right outer join' between RDDs of `this` DStream and
        `other` DStream.

        Hash partitioning is used to generate the RDDs with `numPartitions`
         partitions.
        """
        return self.transformWith(lambda a, b: a.rightOuterJoin(b, numPartitions), other)

    def fullOuterJoin(self, other, numPartitions=None):
        """
         Return a new DStream by applying 'full outer join' between RDDs of `this` DStream and
        `other` DStream.

        Hash partitioning is used to generate the RDDs with `numPartitions`
         partitions.
        """
        return self.transformWith(lambda a, b: a.fullOuterJoin(b, numPartitions), other)

    def _jtime(self, timestamp):
        """ Convert datetime or unix_timestamp into Time
        """
        if isinstance(timestamp, datetime):
            timestamp = time.mktime(timestamp.timetuple())
        return self.ctx._jvm.Time(long(timestamp * 1000))

    def slice(self, begin, end):
        """
        Return all the RDDs between 'begin' to 'end' (both included)

        `begin`, `end` could be datetime.datetime() or unix_timestamp
        """
        jrdds = self._jdstream.slice(self._jtime(begin), self._jtime(end))
        return [RDD(jrdd, self.ctx, self._jrdd_deserializer) for jrdd in jrdds]

    def _check_window(self, window, slide):
        duration = self._jdstream.dstream().slideDuration().milliseconds()
        if int(window * 1000) % duration != 0:
            raise ValueError("windowDuration must be multiple of the slide duration (%d ms)"
                             % duration)
        if slide and int(slide * 1000) % duration != 0:
            raise ValueError("slideDuration must be multiple of the slide duration (%d ms)"
                             % duration)

    def window(self, windowDuration, slideDuration=None):
        """
        Return a new DStream in which each RDD contains all the elements in seen in a
        sliding window of time over this DStream.

        @param windowDuration width of the window; must be a multiple of this DStream's
                              batching interval
        @param slideDuration  sliding interval of the window (i.e., the interval after which
                              the new DStream will generate RDDs); must be a multiple of this
                              DStream's batching interval
        """
        self._check_window(windowDuration, slideDuration)
        d = self._ssc._jduration(windowDuration)
        if slideDuration is None:
            return DStream(self._jdstream.window(d), self._ssc, self._jrdd_deserializer)
        s = self._ssc._jduration(slideDuration)
        return DStream(self._jdstream.window(d, s), self._ssc, self._jrdd_deserializer)

    def reduceByWindow(self, reduceFunc, invReduceFunc, windowDuration, slideDuration):
        """
        Return a new DStream in which each RDD has a single element generated by reducing all
        elements in a sliding window over this DStream.

        if `invReduceFunc` is not None, the reduction is done incrementally
        using the old window's reduced value :
         1. reduce the new values that entered the window (e.g., adding new counts)
         2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)
         This is more efficient than `invReduceFunc` is None.

        @param reduceFunc associative reduce function
        @param invReduceFunc inverse reduce function of `reduceFunc`
        @param windowDuration width of the window; must be a multiple of this DStream's
                              batching interval
        @param slideDuration  sliding interval of the window (i.e., the interval after which
                              the new DStream will generate RDDs); must be a multiple of this
                              DStream's batching interval
        """
        keyed = self.map(lambda x: (1, x))
        reduced = keyed.reduceByKeyAndWindow(reduceFunc, invReduceFunc,
                                             windowDuration, slideDuration, 1)
        return reduced.map(lambda (k, v): v)

    def countByWindow(self, windowDuration, slideDuration):
        """
        Return a new DStream in which each RDD has a single element generated
        by counting the number of elements in a window over this DStream.
        windowDuration and slideDuration are as defined in the window() operation.

        This is equivalent to window(windowDuration, slideDuration).count(),
        but will be more efficient if window is large.
        """
        return self.map(lambda x: 1).reduceByWindow(operator.add, operator.sub,
                                                    windowDuration, slideDuration)

    def countByValueAndWindow(self, windowDuration, slideDuration, numPartitions=None):
        """
        Return a new DStream in which each RDD contains the count of distinct elements in
        RDDs in a sliding window over this DStream.

        @param windowDuration width of the window; must be a multiple of this DStream's
                              batching interval
        @param slideDuration  sliding interval of the window (i.e., the interval after which
                              the new DStream will generate RDDs); must be a multiple of this
                              DStream's batching interval
        @param numPartitions  number of partitions of each RDD in the new DStream.
        """
        keyed = self.map(lambda x: (x, 1))
        counted = keyed.reduceByKeyAndWindow(operator.add, operator.sub,
                                             windowDuration, slideDuration, numPartitions)
        return counted.filter(lambda (k, v): v > 0).count()

    def groupByKeyAndWindow(self, windowDuration, slideDuration, numPartitions=None):
        """
        Return a new DStream by applying `groupByKey` over a sliding window.
        Similar to `DStream.groupByKey()`, but applies it over a sliding window.

        @param windowDuration width of the window; must be a multiple of this DStream's
                              batching interval
        @param slideDuration  sliding interval of the window (i.e., the interval after which
                              the new DStream will generate RDDs); must be a multiple of this
                              DStream's batching interval
        @param numPartitions  Number of partitions of each RDD in the new DStream.
        """
        ls = self.mapValues(lambda x: [x])
        grouped = ls.reduceByKeyAndWindow(lambda a, b: a.extend(b) or a, lambda a, b: a[len(b):],
                                          windowDuration, slideDuration, numPartitions)
        return grouped.mapValues(ResultIterable)

    def reduceByKeyAndWindow(self, func, invFunc, windowDuration, slideDuration=None,
                             numPartitions=None, filterFunc=None):
        """
        Return a new DStream by applying incremental `reduceByKey` over a sliding window.

        The reduced value of over a new window is calculated using the old window's reduce value :
         1. reduce the new values that entered the window (e.g., adding new counts)
         2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)

        `invFunc` can be None, then it will reduce all the RDDs in window, could be slower
        than having `invFunc`.

        @param reduceFunc     associative reduce function
        @param invReduceFunc  inverse function of `reduceFunc`
        @param windowDuration width of the window; must be a multiple of this DStream's
                              batching interval
        @param slideDuration  sliding interval of the window (i.e., the interval after which
                              the new DStream will generate RDDs); must be a multiple of this
                              DStream's batching interval
        @param numPartitions  number of partitions of each RDD in the new DStream.
        @param filterFunc     function to filter expired key-value pairs;
                              only pairs that satisfy the function are retained
                              set this to null if you do not want to filter
        """
        self._check_window(windowDuration, slideDuration)
        reduced = self.reduceByKey(func)

        def reduceFunc(t, a, b):
            b = b.reduceByKey(func, numPartitions)
            r = a.union(b).reduceByKey(func, numPartitions) if a else b
            if filterFunc:
                r = r.filter(filterFunc)
            return r

        def invReduceFunc(t, a, b):
            b = b.reduceByKey(func, numPartitions)
            joined = a.leftOuterJoin(b, numPartitions)
            return joined.mapValues(lambda (v1, v2): invFunc(v1, v2) if v2 is not None else v1)

        jreduceFunc = RDDFunction(self.ctx, reduceFunc, reduced._jrdd_deserializer)
        if invReduceFunc:
            jinvReduceFunc = RDDFunction(self.ctx, invReduceFunc, reduced._jrdd_deserializer)
        else:
            jinvReduceFunc = None
        if slideDuration is None:
            slideDuration = self._slideDuration
        dstream = self.ctx._jvm.PythonReducedWindowedDStream(reduced._jdstream.dstream(),
                                                             jreduceFunc, jinvReduceFunc,
                                                             self._ssc._jduration(windowDuration),
                                                             self._ssc._jduration(slideDuration))
        return DStream(dstream.asJavaDStream(), self._ssc, self.ctx.serializer)

    def updateStateByKey(self, updateFunc, numPartitions=None):
        """
        Return a new "state" DStream where the state for each key is updated by applying
        the given function on the previous state of the key and the new values of the key.

        @param updateFunc State update function ([(k, vs, s)] -> [(k, s)]).
                          If `s` is None, then `k` will be eliminated.
        """
        def reduceFunc(t, a, b):
            if a is None:
                g = b.groupByKey(numPartitions).map(lambda (k, vs): (k, list(vs), None))
            else:
                g = a.cogroup(b, numPartitions)
                g = g.map(lambda (k, (va, vb)): (k, list(vb), list(va)[0] if len(va) else None))
            state = g.mapPartitions(lambda x: updateFunc(x))
            return state.filter(lambda (k, v): v is not None)

        jreduceFunc = RDDFunction(self.ctx, reduceFunc,
                                  self.ctx.serializer, self._jrdd_deserializer)
        dstream = self.ctx._jvm.PythonStateDStream(self._jdstream.dstream(), jreduceFunc)
        return DStream(dstream.asJavaDStream(), self._ssc, self.ctx.serializer)


class TransformedDStream(DStream):
    def __init__(self, prev, func, reuse=False):
        ssc = prev._ssc
        self._ssc = ssc
        self.ctx = ssc._sc
        self._jrdd_deserializer = self.ctx.serializer
        self.is_cached = False
        self.is_checkpointed = False

        if (isinstance(prev, TransformedDStream) and
                not prev.is_cached and not prev.is_checkpointed):
            prev_func = prev.func
            old_func = func
            func = lambda t, rdd: old_func(t, prev_func(t, rdd))
            reuse = reuse and prev.reuse
            prev = prev.prev

        self.prev = prev
        self.func = func
        self.reuse = reuse
        self._jdstream_val = None

    @property
    def _jdstream(self):
        if self._jdstream_val is not None:
            return self._jdstream_val

        func = self.func
        jfunc = RDDFunction(self.ctx, func, self.prev._jrdd_deserializer)
        jdstream = self.ctx._jvm.PythonTransformedDStream(self.prev._jdstream.dstream(),
                                                          jfunc, self.reuse).asJavaDStream()
        self._jdstream_val = jdstream
        return jdstream
