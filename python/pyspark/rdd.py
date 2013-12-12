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

from base64 import standard_b64encode as b64enc
import copy
from collections import defaultdict
from itertools import chain, ifilter, imap
import operator
import os
import sys
import shlex
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from threading import Thread

from pyspark.serializers import NoOpSerializer, CartesianDeserializer, \
    BatchedSerializer, CloudPickleSerializer, pack_long
from pyspark.join import python_join, python_left_outer_join, \
    python_right_outer_join, python_cogroup
from pyspark.statcounter import StatCounter
from pyspark.rddsampler import RDDSampler

from py4j.java_collections import ListConverter, MapConverter


__all__ = ["RDD"]


class RDD(object):
    """
    A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.
    Represents an immutable, partitioned collection of elements that can be
    operated on in parallel.
    """

    def __init__(self, jrdd, ctx, jrdd_deserializer):
        self._jrdd = jrdd
        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = ctx
        self._jrdd_deserializer = jrdd_deserializer

    @property
    def context(self):
        """
        The L{SparkContext} that this RDD was created on.
        """
        return self.ctx

    def cache(self):
        """
        Persist this RDD with the default storage level (C{MEMORY_ONLY}).
        """
        self.is_cached = True
        self._jrdd.cache()
        return self

    def persist(self, storageLevel):
        """
        Set this RDD's storage level to persist its values across operations after the first time
        it is computed. This can only be used to assign a new storage level if the RDD does not
        have a storage level set yet.
        """
        self.is_cached = True
        javaStorageLevel = self.ctx._getJavaStorageLevel(storageLevel)
        self._jrdd.persist(javaStorageLevel)
        return self

    def unpersist(self):
        """
        Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
        """
        self.is_cached = False
        self._jrdd.unpersist()
        return self

    def checkpoint(self):
        """
        Mark this RDD for checkpointing. It will be saved to a file inside the
        checkpoint directory set with L{SparkContext.setCheckpointDir()} and
        all references to its parent RDDs will be removed. This function must
        be called before any job has been executed on this RDD. It is strongly
        recommended that this RDD is persisted in memory, otherwise saving it
        on a file will require recomputation.
        """
        self.is_checkpointed = True
        self._jrdd.rdd().checkpoint()

    def isCheckpointed(self):
        """
        Return whether this RDD has been checkpointed or not
        """
        return self._jrdd.rdd().isCheckpointed()

    def getCheckpointFile(self):
        """
        Gets the name of the file to which this RDD was checkpointed
        """
        checkpointFile = self._jrdd.rdd().getCheckpointFile()
        if checkpointFile.isDefined():
            return checkpointFile.get()
        else:
            return None

    def map(self, f, preservesPartitioning=False):
        """
        Return a new RDD containing the distinct elements in this RDD.
        """
        def func(split, iterator): return imap(f, iterator)
        return PipelinedRDD(self, func, preservesPartitioning)

    def flatMap(self, f, preservesPartitioning=False):
        """
        Return a new RDD by first applying a function to all elements of this
        RDD, and then flattening the results.

        >>> rdd = sc.parallelize([2, 3, 4])
        >>> sorted(rdd.flatMap(lambda x: range(1, x)).collect())
        [1, 1, 1, 2, 2, 3]
        >>> sorted(rdd.flatMap(lambda x: [(x, x), (x, x)]).collect())
        [(2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)]
        """
        def func(s, iterator): return chain.from_iterable(imap(f, iterator))
        return self.mapPartitionsWithSplit(func, preservesPartitioning)

    def mapPartitions(self, f, preservesPartitioning=False):
        """
        Return a new RDD by applying a function to each partition of this RDD.

        >>> rdd = sc.parallelize([1, 2, 3, 4], 2)
        >>> def f(iterator): yield sum(iterator)
        >>> rdd.mapPartitions(f).collect()
        [3, 7]
        """
        def func(s, iterator): return f(iterator)
        return self.mapPartitionsWithSplit(func)

    def mapPartitionsWithSplit(self, f, preservesPartitioning=False):
        """
        Return a new RDD by applying a function to each partition of this RDD,
        while tracking the index of the original partition.

        >>> rdd = sc.parallelize([1, 2, 3, 4], 4)
        >>> def f(splitIndex, iterator): yield splitIndex
        >>> rdd.mapPartitionsWithSplit(f).sum()
        6
        """
        return PipelinedRDD(self, f, preservesPartitioning)

    def filter(self, f):
        """
        Return a new RDD containing only the elements that satisfy a predicate.

        >>> rdd = sc.parallelize([1, 2, 3, 4, 5])
        >>> rdd.filter(lambda x: x % 2 == 0).collect()
        [2, 4]
        """
        def func(iterator): return ifilter(f, iterator)
        return self.mapPartitions(func)

    def distinct(self):
        """
        Return a new RDD containing the distinct elements in this RDD.

        >>> sorted(sc.parallelize([1, 1, 2, 3]).distinct().collect())
        [1, 2, 3]
        """
        return self.map(lambda x: (x, None)) \
                   .reduceByKey(lambda x, _: x) \
                   .map(lambda (x, _): x)

    def sample(self, withReplacement, fraction, seed):
        """
        Return a sampled subset of this RDD (relies on numpy and falls back
        on default random generator if numpy is unavailable).

        >>> sc.parallelize(range(0, 100)).sample(False, 0.1, 2).collect() #doctest: +SKIP
        [2, 3, 20, 21, 24, 41, 42, 66, 67, 89, 90, 98]
        """
        return self.mapPartitionsWithSplit(RDDSampler(withReplacement, fraction, seed).func, True)

    # this is ported from scala/spark/RDD.scala
    def takeSample(self, withReplacement, num, seed):
        """
        Return a fixed-size sampled subset of this RDD (currently requires numpy).

        >>> sc.parallelize(range(0, 10)).takeSample(True, 10, 1) #doctest: +SKIP
        [4, 2, 1, 8, 2, 7, 0, 4, 1, 4]
        """

        fraction = 0.0
        total = 0
        multiplier = 3.0
        initialCount = self.count()
        maxSelected = 0

        if (num < 0):
            raise ValueError

        if initialCount > sys.maxint - 1:
            maxSelected = sys.maxint - 1
        else:
            maxSelected = initialCount

        if num > initialCount and not withReplacement:
            total = maxSelected
            fraction = multiplier * (maxSelected + 1) / initialCount
        else:
            fraction = multiplier * (num + 1) / initialCount
            total = num

        samples = self.sample(withReplacement, fraction, seed).collect()

        # If the first sample didn't turn out large enough, keep trying to take samples;
        # this shouldn't happen often because we use a big multiplier for their initial size.
        # See: scala/spark/RDD.scala
        while len(samples) < total:
            if seed > sys.maxint - 2:
                seed = -1
            seed += 1
            samples = self.sample(withReplacement, fraction, seed).collect()

        sampler = RDDSampler(withReplacement, fraction, seed+1)
        sampler.shuffle(samples)
        return samples[0:total]

    def union(self, other):
        """
        Return the union of this RDD and another one.

        >>> rdd = sc.parallelize([1, 1, 2, 3])
        >>> rdd.union(rdd).collect()
        [1, 1, 2, 3, 1, 1, 2, 3]
        """
        if self._jrdd_deserializer == other._jrdd_deserializer:
            rdd = RDD(self._jrdd.union(other._jrdd), self.ctx,
                      self._jrdd_deserializer)
            return rdd
        else:
            # These RDDs contain data in different serialized formats, so we
            # must normalize them to the default serializer.
            self_copy = self._reserialize()
            other_copy = other._reserialize()
            return RDD(self_copy._jrdd.union(other_copy._jrdd), self.ctx,
                       self.ctx.serializer)

    def _reserialize(self):
        if self._jrdd_deserializer == self.ctx.serializer:
            return self
        else:
            return self.map(lambda x: x, preservesPartitioning=True)

    def __add__(self, other):
        """
        Return the union of this RDD and another one.

        >>> rdd = sc.parallelize([1, 1, 2, 3])
        >>> (rdd + rdd).collect()
        [1, 1, 2, 3, 1, 1, 2, 3]
        """
        if not isinstance(other, RDD):
            raise TypeError
        return self.union(other)

    def sortByKey(self, ascending=True, numPartitions=None, keyfunc = lambda x: x):
        """
        Sorts this RDD, which is assumed to consist of (key, value) pairs.

        >>> tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
        >>> sc.parallelize(tmp).sortByKey(True, 2).collect()
        [('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
        >>> tmp2 = [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]
        >>> tmp2.extend([('whose', 6), ('fleece', 7), ('was', 8), ('white', 9)])
        >>> sc.parallelize(tmp2).sortByKey(True, 3, keyfunc=lambda k: k.lower()).collect()
        [('a', 3), ('fleece', 7), ('had', 2), ('lamb', 5), ('little', 4), ('Mary', 1), ('was', 8), ('white', 9), ('whose', 6)]
        """
        if numPartitions is None:
            numPartitions = self.ctx.defaultParallelism

        bounds = list()

        # first compute the boundary of each part via sampling: we want to partition
        # the key-space into bins such that the bins have roughly the same
        # number of (key, value) pairs falling into them
        if numPartitions > 1:
            rddSize = self.count()
            maxSampleSize = numPartitions * 20.0 # constant from Spark's RangePartitioner
            fraction = min(maxSampleSize / max(rddSize, 1), 1.0)

            samples = self.sample(False, fraction, 1).map(lambda (k, v): k).collect()
            samples = sorted(samples, reverse=(not ascending), key=keyfunc)

            # we have numPartitions many parts but one of the them has
            # an implicit boundary
            for i in range(0, numPartitions - 1):
                index = (len(samples) - 1) * (i + 1) / numPartitions
                bounds.append(samples[index])

        def rangePartitionFunc(k):
            p = 0
            while p < len(bounds) and keyfunc(k) > bounds[p]:
                p += 1
            if ascending:
                return p
            else:
                return numPartitions-1-p

        def mapFunc(iterator):
            yield sorted(iterator, reverse=(not ascending), key=lambda (k, v): keyfunc(k))

        return (self.partitionBy(numPartitions, partitionFunc=rangePartitionFunc)
                    .mapPartitions(mapFunc,preservesPartitioning=True)
                    .flatMap(lambda x: x, preservesPartitioning=True))

    def glom(self):
        """
        Return an RDD created by coalescing all elements within each partition
        into a list.

        >>> rdd = sc.parallelize([1, 2, 3, 4], 2)
        >>> sorted(rdd.glom().collect())
        [[1, 2], [3, 4]]
        """
        def func(iterator): yield list(iterator)
        return self.mapPartitions(func)

    def cartesian(self, other):
        """
        Return the Cartesian product of this RDD and another one, that is, the
        RDD of all pairs of elements C{(a, b)} where C{a} is in C{self} and
        C{b} is in C{other}.

        >>> rdd = sc.parallelize([1, 2])
        >>> sorted(rdd.cartesian(rdd).collect())
        [(1, 1), (1, 2), (2, 1), (2, 2)]
        """
        # Due to batching, we can't use the Java cartesian method.
        deserializer = CartesianDeserializer(self._jrdd_deserializer,
                                             other._jrdd_deserializer)
        return RDD(self._jrdd.cartesian(other._jrdd), self.ctx, deserializer)

    def groupBy(self, f, numPartitions=None):
        """
        Return an RDD of grouped items.

        >>> rdd = sc.parallelize([1, 1, 2, 3, 5, 8])
        >>> result = rdd.groupBy(lambda x: x % 2).collect()
        >>> sorted([(x, sorted(y)) for (x, y) in result])
        [(0, [2, 8]), (1, [1, 1, 3, 5])]
        """
        return self.map(lambda x: (f(x), x)).groupByKey(numPartitions)

    def pipe(self, command, env={}):
        """
        Return an RDD created by piping elements to a forked external process.

        >>> sc.parallelize([1, 2, 3]).pipe('cat').collect()
        ['1', '2', '3']
        """
        def func(iterator):
            pipe = Popen(shlex.split(command), env=env, stdin=PIPE, stdout=PIPE)
            def pipe_objs(out):
                for obj in iterator:
                    out.write(str(obj).rstrip('\n') + '\n')
                out.close()
            Thread(target=pipe_objs, args=[pipe.stdin]).start()
            return (x.rstrip('\n') for x in pipe.stdout)
        return self.mapPartitions(func)

    def foreach(self, f):
        """
        Applies a function to all elements of this RDD.

        >>> def f(x): print x
        >>> sc.parallelize([1, 2, 3, 4, 5]).foreach(f)
        """
        def processPartition(iterator):
            for x in iterator:
                f(x)
            yield None
        self.mapPartitions(processPartition).collect()  # Force evaluation

    def collect(self):
        """
        Return a list that contains all of the elements in this RDD.
        """
        bytesInJava = self._jrdd.collect().iterator()
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
            for item in self._jrdd_deserializer.load_stream(tempFile):
                yield item
        os.unlink(tempFile.name)

    def reduce(self, f):
        """
        Reduces the elements of this RDD using the specified commutative and
        associative binary operator.

        >>> from operator import add
        >>> sc.parallelize([1, 2, 3, 4, 5]).reduce(add)
        15
        >>> sc.parallelize((2 for _ in range(10))).map(lambda x: 1).cache().reduce(add)
        10
        """
        def func(iterator):
            acc = None
            for obj in iterator:
                if acc is None:
                    acc = obj
                else:
                    acc = f(obj, acc)
            if acc is not None:
                yield acc
        vals = self.mapPartitions(func).collect()
        return reduce(f, vals)

    def fold(self, zeroValue, op):
        """
        Aggregate the elements of each partition, and then the results for all
        the partitions, using a given associative function and a neutral "zero
        value."

        The function C{op(t1, t2)} is allowed to modify C{t1} and return it
        as its result value to avoid object allocation; however, it should not
        modify C{t2}.

        >>> from operator import add
        >>> sc.parallelize([1, 2, 3, 4, 5]).fold(0, add)
        15
        """
        def func(iterator):
            acc = zeroValue
            for obj in iterator:
                acc = op(obj, acc)
            yield acc
        vals = self.mapPartitions(func).collect()
        return reduce(op, vals, zeroValue)

    # TODO: aggregate

    def sum(self):
        """
        Add up the elements in this RDD.

        >>> sc.parallelize([1.0, 2.0, 3.0]).sum()
        6.0
        """
        return self.mapPartitions(lambda x: [sum(x)]).reduce(operator.add)

    def count(self):
        """
        Return the number of elements in this RDD.

        >>> sc.parallelize([2, 3, 4]).count()
        3
        """
        return self.mapPartitions(lambda i: [sum(1 for _ in i)]).sum()

    def stats(self):
        """
        Return a L{StatCounter} object that captures the mean, variance
        and count of the RDD's elements in one operation.
        """
        def redFunc(left_counter, right_counter):
            return left_counter.mergeStats(right_counter)

        return self.mapPartitions(lambda i: [StatCounter(i)]).reduce(redFunc)

    def mean(self):
        """
        Compute the mean of this RDD's elements.

        >>> sc.parallelize([1, 2, 3]).mean()
        2.0
        """
        return self.stats().mean()

    def variance(self):
        """
        Compute the variance of this RDD's elements.

        >>> sc.parallelize([1, 2, 3]).variance()
        0.666...
        """
        return self.stats().variance()

    def stdev(self):
        """
        Compute the standard deviation of this RDD's elements.

        >>> sc.parallelize([1, 2, 3]).stdev()
        0.816...
        """
        return self.stats().stdev()

    def sampleStdev(self):
        """
        Compute the sample standard deviation of this RDD's elements (which corrects for bias in
        estimating the standard deviation by dividing by N-1 instead of N).

        >>> sc.parallelize([1, 2, 3]).sampleStdev()
        1.0
        """
        return self.stats().sampleStdev()

    def sampleVariance(self):
        """
        Compute the sample variance of this RDD's elements (which corrects for bias in
        estimating the variance by dividing by N-1 instead of N).

        >>> sc.parallelize([1, 2, 3]).sampleVariance()
        1.0
        """
        return self.stats().sampleVariance()

    def countByValue(self):
        """
        Return the count of each unique value in this RDD as a dictionary of
        (value, count) pairs.

        >>> sorted(sc.parallelize([1, 2, 1, 2, 2], 2).countByValue().items())
        [(1, 2), (2, 3)]
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
        return self.mapPartitions(countPartition).reduce(mergeMaps)

    def take(self, num):
        """
        Take the first num elements of the RDD.

        This currently scans the partitions *one by one*, so it will be slow if
        a lot of partitions are required. In that case, use L{collect} to get
        the whole RDD instead.

        >>> sc.parallelize([2, 3, 4, 5, 6]).cache().take(2)
        [2, 3]
        >>> sc.parallelize([2, 3, 4, 5, 6]).take(10)
        [2, 3, 4, 5, 6]
        """
        def takeUpToNum(iterator):
            taken = 0
            while taken < num:
                yield next(iterator)
                taken += 1
        # Take only up to num elements from each partition we try
        mapped = self.mapPartitions(takeUpToNum)
        items = []
        for partition in range(mapped._jrdd.splits().size()):
            iterator = self.ctx._takePartition(mapped._jrdd.rdd(), partition)
            items.extend(mapped._collect_iterator_through_file(iterator))
            if len(items) >= num:
                break
        return items[:num]

    def first(self):
        """
        Return the first element in this RDD.

        >>> sc.parallelize([2, 3, 4]).first()
        2
        """
        return self.take(1)[0]

    def saveAsTextFile(self, path):
        """
        Save this RDD as a text file, using string representations of elements.

        >>> tempFile = NamedTemporaryFile(delete=True)
        >>> tempFile.close()
        >>> sc.parallelize(range(10)).saveAsTextFile(tempFile.name)
        >>> from fileinput import input
        >>> from glob import glob
        >>> ''.join(sorted(input(glob(tempFile.name + "/part-0000*"))))
        '0\\n1\\n2\\n3\\n4\\n5\\n6\\n7\\n8\\n9\\n'
        """
        def func(split, iterator):
            for x in iterator:
                if not isinstance(x, basestring):
                    x = unicode(x)
                yield x.encode("utf-8")
        keyed = PipelinedRDD(self, func)
        keyed._bypass_serializer = True
        keyed._jrdd.map(self.ctx._jvm.BytesToString()).saveAsTextFile(path)

    # Pair functions

    def collectAsMap(self):
        """
        Return the key-value pairs in this RDD to the master as a dictionary.

        >>> m = sc.parallelize([(1, 2), (3, 4)]).collectAsMap()
        >>> m[1]
        2
        >>> m[3]
        4
        """
        return dict(self.collect())

    def reduceByKey(self, func, numPartitions=None):
        """
        Merge the values for each key using an associative reduce function.

        This will also perform the merging locally on each mapper before
        sending results to a reducer, similarly to a "combiner" in MapReduce.

        Output will be hash-partitioned with C{numPartitions} partitions, or
        the default parallelism level if C{numPartitions} is not specified.

        >>> from operator import add
        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> sorted(rdd.reduceByKey(add).collect())
        [('a', 2), ('b', 1)]
        """
        return self.combineByKey(lambda x: x, func, func, numPartitions)

    def reduceByKeyLocally(self, func):
        """
        Merge the values for each key using an associative reduce function, but
        return the results immediately to the master as a dictionary.

        This will also perform the merging locally on each mapper before
        sending results to a reducer, similarly to a "combiner" in MapReduce.

        >>> from operator import add
        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> sorted(rdd.reduceByKeyLocally(add).items())
        [('a', 2), ('b', 1)]
        """
        def reducePartition(iterator):
            m = {}
            for (k, v) in iterator:
                m[k] = v if k not in m else func(m[k], v)
            yield m
        def mergeMaps(m1, m2):
            for (k, v) in m2.iteritems():
                m1[k] = v if k not in m1 else func(m1[k], v)
            return m1
        return self.mapPartitions(reducePartition).reduce(mergeMaps)

    def countByKey(self):
        """
        Count the number of elements for each key, and return the result to the
        master as a dictionary.

        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> sorted(rdd.countByKey().items())
        [('a', 2), ('b', 1)]
        """
        return self.map(lambda x: x[0]).countByValue()

    def join(self, other, numPartitions=None):
        """
        Return an RDD containing all pairs of elements with matching keys in
        C{self} and C{other}.

        Each pair of elements will be returned as a (k, (v1, v2)) tuple, where
        (k, v1) is in C{self} and (k, v2) is in C{other}.

        Performs a hash join across the cluster.

        >>> x = sc.parallelize([("a", 1), ("b", 4)])
        >>> y = sc.parallelize([("a", 2), ("a", 3)])
        >>> sorted(x.join(y).collect())
        [('a', (1, 2)), ('a', (1, 3))]
        """
        return python_join(self, other, numPartitions)

    def leftOuterJoin(self, other, numPartitions=None):
        """
        Perform a left outer join of C{self} and C{other}.

        For each element (k, v) in C{self}, the resulting RDD will either
        contain all pairs (k, (v, w)) for w in C{other}, or the pair
        (k, (v, None)) if no elements in other have key k.

        Hash-partitions the resulting RDD into the given number of partitions.

        >>> x = sc.parallelize([("a", 1), ("b", 4)])
        >>> y = sc.parallelize([("a", 2)])
        >>> sorted(x.leftOuterJoin(y).collect())
        [('a', (1, 2)), ('b', (4, None))]
        """
        return python_left_outer_join(self, other, numPartitions)

    def rightOuterJoin(self, other, numPartitions=None):
        """
        Perform a right outer join of C{self} and C{other}.

        For each element (k, w) in C{other}, the resulting RDD will either
        contain all pairs (k, (v, w)) for v in this, or the pair (k, (None, w))
        if no elements in C{self} have key k.

        Hash-partitions the resulting RDD into the given number of partitions.

        >>> x = sc.parallelize([("a", 1), ("b", 4)])
        >>> y = sc.parallelize([("a", 2)])
        >>> sorted(y.rightOuterJoin(x).collect())
        [('a', (2, 1)), ('b', (None, 4))]
        """
        return python_right_outer_join(self, other, numPartitions)

    # TODO: add option to control map-side combining
    def partitionBy(self, numPartitions, partitionFunc=hash):
        """
        Return a copy of the RDD partitioned using the specified partitioner.

        >>> pairs = sc.parallelize([1, 2, 3, 4, 2, 4, 1]).map(lambda x: (x, x))
        >>> sets = pairs.partitionBy(2).glom().collect()
        >>> set(sets[0]).intersection(set(sets[1]))
        set([])
        """
        if numPartitions is None:
            numPartitions = self.ctx.defaultParallelism
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
        keyed = PipelinedRDD(self, add_shuffle_key)
        keyed._bypass_serializer = True
        pairRDD = self.ctx._jvm.PairwiseRDD(keyed._jrdd.rdd()).asJavaPairRDD()
        partitioner = self.ctx._jvm.PythonPartitioner(numPartitions,
                                                     id(partitionFunc))
        jrdd = pairRDD.partitionBy(partitioner).values()
        rdd = RDD(jrdd, self.ctx, BatchedSerializer(outputSerializer))
        # This is required so that id(partitionFunc) remains unique, even if
        # partitionFunc is a lambda:
        rdd._partitionFunc = partitionFunc
        return rdd

    # TODO: add control over map-side aggregation
    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                     numPartitions=None):
        """
        Generic function to combine the elements for each key using a custom
        set of aggregation functions.

        Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined
        type" C.  Note that V and C can be different -- for example, one might
        group an RDD of type (Int, Int) into an RDD of type (Int, List[Int]).

        Users provide three functions:

            - C{createCombiner}, which turns a V into a C (e.g., creates
              a one-element list)
            - C{mergeValue}, to merge a V into a C (e.g., adds it to the end of
              a list)
            - C{mergeCombiners}, to combine two C's into a single one.

        In addition, users can control the partitioning of the output RDD.

        >>> x = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> def f(x): return x
        >>> def add(a, b): return a + str(b)
        >>> sorted(x.combineByKey(str, add, add).collect())
        [('a', '11'), ('b', '1')]
        """
        if numPartitions is None:
            numPartitions = self.ctx.defaultParallelism
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
                if not k in combiners:
                    combiners[k] = v
                else:
                    combiners[k] = mergeCombiners(combiners[k], v)
            return combiners.iteritems()
        return shuffled.mapPartitions(_mergeCombiners)

    # TODO: support variant with custom partitioner
    def groupByKey(self, numPartitions=None):
        """
        Group the values for each key in the RDD into a single sequence.
        Hash-partitions the resulting RDD with into numPartitions partitions.

        >>> x = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> sorted(x.groupByKey().collect())
        [('a', [1, 1]), ('b', [1])]
        """

        def createCombiner(x):
            return [x]

        def mergeValue(xs, x):
            xs.append(x)
            return xs

        def mergeCombiners(a, b):
            return a + b

        return self.combineByKey(createCombiner, mergeValue, mergeCombiners,
                numPartitions)

    # TODO: add tests
    def flatMapValues(self, f):
        """
        Pass each value in the key-value pair RDD through a flatMap function
        without changing the keys; this also retains the original RDD's
        partitioning.
        """
        flat_map_fn = lambda (k, v): ((k, x) for x in f(v))
        return self.flatMap(flat_map_fn, preservesPartitioning=True)

    def mapValues(self, f):
        """
        Pass each value in the key-value pair RDD through a map function
        without changing the keys; this also retains the original RDD's
        partitioning.
        """
        map_values_fn = lambda (k, v): (k, f(v))
        return self.map(map_values_fn, preservesPartitioning=True)

    # TODO: support varargs cogroup of several RDDs.
    def groupWith(self, other):
        """
        Alias for cogroup.
        """
        return self.cogroup(other)

    # TODO: add variant with custom parittioner
    def cogroup(self, other, numPartitions=None):
        """
        For each key k in C{self} or C{other}, return a resulting RDD that
        contains a tuple with the list of values for that key in C{self} as well
        as C{other}.

        >>> x = sc.parallelize([("a", 1), ("b", 4)])
        >>> y = sc.parallelize([("a", 2)])
        >>> sorted(x.cogroup(y).collect())
        [('a', ([1], [2])), ('b', ([4], []))]
        """
        return python_cogroup(self, other, numPartitions)

    def subtractByKey(self, other, numPartitions=None):
        """
        Return each (key, value) pair in C{self} that has no pair with matching key
        in C{other}.

        >>> x = sc.parallelize([("a", 1), ("b", 4), ("b", 5), ("a", 2)])
        >>> y = sc.parallelize([("a", 3), ("c", None)])
        >>> sorted(x.subtractByKey(y).collect())
        [('b', 4), ('b', 5)]
        """
        filter_func = lambda (key, vals): len(vals[0]) > 0 and len(vals[1]) == 0
        map_func = lambda (key, vals): [(key, val) for val in vals[0]]
        return self.cogroup(other, numPartitions).filter(filter_func).flatMap(map_func)

    def subtract(self, other, numPartitions=None):
        """
        Return each value in C{self} that is not contained in C{other}.

        >>> x = sc.parallelize([("a", 1), ("b", 4), ("b", 5), ("a", 3)])
        >>> y = sc.parallelize([("a", 3), ("c", None)])
        >>> sorted(x.subtract(y).collect())
        [('a', 1), ('b', 4), ('b', 5)]
        """
        rdd = other.map(lambda x: (x, True)) # note: here 'True' is just a placeholder
        return self.map(lambda x: (x, True)).subtractByKey(rdd).map(lambda tpl: tpl[0]) # note: here 'True' is just a placeholder

    def keyBy(self, f):
        """
        Creates tuples of the elements in this RDD by applying C{f}.

        >>> x = sc.parallelize(range(0,3)).keyBy(lambda x: x*x)
        >>> y = sc.parallelize(zip(range(0,5), range(0,5)))
        >>> sorted(x.cogroup(y).collect())
        [(0, ([0], [0])), (1, ([1], [1])), (2, ([], [2])), (3, ([], [3])), (4, ([2], [4]))]
        """
        return self.map(lambda x: (f(x), x))

    # TODO: `lookup` is disabled because we can't make direct comparisons based
    # on the key; we need to compare the hash of the key to the hash of the
    # keys in the pairs.  This could be an expensive operation, since those
    # hashes aren't retained.


class PipelinedRDD(RDD):
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
        if not isinstance(prev, PipelinedRDD) or not prev._is_pipelinable():
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
        self.ctx = prev.ctx
        self.prev = prev
        self._jrdd_val = None
        self._jrdd_deserializer = self.ctx.serializer
        self._bypass_serializer = False

    @property
    def _jrdd(self):
        if self._jrdd_val:
            return self._jrdd_val
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
        class_tag = self._prev_jrdd.classTag()
        env = MapConverter().convert(self.ctx.environment,
                                     self.ctx._gateway._gateway_client)
        includes = ListConverter().convert(self.ctx._python_includes,
                                     self.ctx._gateway._gateway_client)
        python_rdd = self.ctx._jvm.PythonRDD(self._prev_jrdd.rdd(),
            bytearray(pickled_command), env, includes, self.preservesPartitioning,
            self.ctx.pythonExec, broadcast_vars, self.ctx._javaAccumulator,
            class_tag)
        self._jrdd_val = python_rdd.asJavaRDD()
        return self._jrdd_val

    def _is_pipelinable(self):
        return not (self.is_cached or self.is_checkpointed)


def _test():
    import doctest
    from pyspark.context import SparkContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs,optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
