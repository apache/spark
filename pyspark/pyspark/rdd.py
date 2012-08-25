from base64 import standard_b64encode as b64enc
from collections import Counter
from itertools import chain, ifilter, imap

from pyspark import cloudpickle
from pyspark.serializers import dump_pickle, load_pickle
from pyspark.join import python_join, python_left_outer_join, \
    python_right_outer_join, python_cogroup

from py4j.java_collections import ListConverter


class RDD(object):

    def __init__(self, jrdd, ctx):
        self._jrdd = jrdd
        self.is_cached = False
        self.ctx = ctx

    def cache(self):
        self.is_cached = True
        self._jrdd.cache()
        return self

    def map(self, f, preservesPartitioning=False):
        def func(iterator): return imap(f, iterator)
        return PipelinedRDD(self, func, preservesPartitioning)

    def flatMap(self, f):
        """
        >>> rdd = sc.parallelize([2, 3, 4])
        >>> sorted(rdd.flatMap(lambda x: range(1, x)).collect())
        [1, 1, 1, 2, 2, 3]
        >>> sorted(rdd.flatMap(lambda x: [(x, x), (x, x)]).collect())
        [(2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)]
        """
        def func(iterator): return chain.from_iterable(imap(f, iterator))
        return self.mapPartitions(func)

    def mapPartitions(self, f):
        """
        >>> rdd = sc.parallelize([1, 2, 3, 4], 2)
        >>> def f(iterator): yield sum(iterator)
        >>> rdd.mapPartitions(f).collect()
        [3, 7]
        """
        return PipelinedRDD(self, f)

    def filter(self, f):
        """
        >>> rdd = sc.parallelize([1, 2, 3, 4, 5])
        >>> rdd.filter(lambda x: x % 2 == 0).collect()
        [2, 4]
        """
        def func(iterator): return ifilter(f, iterator)
        return self.mapPartitions(func)

    def distinct(self):
        """
        >>> sorted(sc.parallelize([1, 1, 2, 3]).distinct().collect())
        [1, 2, 3]
        """
        return self.map(lambda x: (x, "")) \
                   .reduceByKey(lambda x, _: x) \
                   .map(lambda (x, _): x)

    def sample(self, withReplacement, fraction, seed):
        jrdd = self._jrdd.sample(withReplacement, fraction, seed)
        return RDD(jrdd, self.ctx)

    def takeSample(self, withReplacement, num, seed):
        vals = self._jrdd.takeSample(withReplacement, num, seed)
        return [load_pickle(bytes(x)) for x in vals]

    def union(self, other):
        """
        >>> rdd = sc.parallelize([1, 1, 2, 3])
        >>> rdd.union(rdd).collect()
        [1, 1, 2, 3, 1, 1, 2, 3]
        """
        return RDD(self._jrdd.union(other._jrdd), self.ctx)

    def __add__(self, other):
        """
        >>> rdd = sc.parallelize([1, 1, 2, 3])
        >>> (rdd + rdd).collect()
        [1, 1, 2, 3, 1, 1, 2, 3]
        """
        if not isinstance(other, RDD):
            raise TypeError
        return self.union(other)

    # TODO: sort

    def glom(self):
        """
        >>> rdd = sc.parallelize([1, 2, 3, 4], 2)
        >>> rdd.glom().first()
        [1, 2]
        """
        def func(iterator): yield list(iterator)
        return self.mapPartitions(func)

    def cartesian(self, other):
        """
        >>> rdd = sc.parallelize([1, 2])
        >>> sorted(rdd.cartesian(rdd).collect())
        [(1, 1), (1, 2), (2, 1), (2, 2)]
        """
        return RDD(self._jrdd.cartesian(other._jrdd), self.ctx)

    def groupBy(self, f, numSplits=None):
        """
        >>> rdd = sc.parallelize([1, 1, 2, 3, 5, 8])
        >>> result = rdd.groupBy(lambda x: x % 2).collect()
        >>> sorted([(x, sorted(y)) for (x, y) in result])
        [(0, [2, 8]), (1, [1, 1, 3, 5])]
        """
        return self.map(lambda x: (f(x), x)).groupByKey(numSplits)

    # TODO: pipe

    def foreach(self, f):
        """
        >>> def f(x): print x
        >>> sc.parallelize([1, 2, 3, 4, 5]).foreach(f)
        """
        self.map(f).collect()  # Force evaluation

    def collect(self):
        pickle = self.ctx.arrayAsPickle(self._jrdd.rdd().collect())
        return load_pickle(bytes(pickle))

    def reduce(self, f):
        """
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
        value." The function op(t1, t2) is allowed to modify t1 and return it
        as its result value to avoid object allocation; however, it should not
        modify t2.

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

    def count(self):
        """
        >>> sc.parallelize([2, 3, 4]).count()
        3L
        """
        return self._jrdd.count()

    def countByValue(self):
        """
        >>> sc.parallelize([1, 2, 1, 2, 2]).countByValue().most_common()
        [(2, 3), (1, 2)]
        """
        def countPartition(iterator):
            yield Counter(iterator)
        def mergeMaps(m1, m2):
            return m1 + m2
        return self.mapPartitions(countPartition).reduce(mergeMaps)

    def take(self, num):
        """
        >>> sc.parallelize([2, 3, 4]).take(2)
        [2, 3]
        """
        pickle = self.ctx.arrayAsPickle(self._jrdd.rdd().take(num))
        return load_pickle(bytes(pickle))

    def first(self):
        """
        >>> sc.parallelize([2, 3, 4]).first()
        2
        """
        return load_pickle(bytes(self.ctx.asPickle(self._jrdd.first())))

    # TODO: saveAsTextFile

    # Pair functions

    def collectAsMap(self):
        """
        >>> m = sc.parallelize([(1, 2), (3, 4)]).collectAsMap()
        >>> m[1]
        2
        >>> m[3]
        4
        """
        return dict(self.collect())

    def reduceByKey(self, func, numSplits=None):
        """
        >>> from operator import add
        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> sorted(rdd.reduceByKey(add).collect())
        [('a', 2), ('b', 1)]
        """
        return self.combineByKey(lambda x: x, func, func, numSplits)

    def reduceByKeyLocally(self, func):
        """
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
        >>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> rdd.countByKey().most_common()
        [('a', 2), ('b', 1)]
        """
        return self.map(lambda x: x[0]).countByValue()

    def join(self, other, numSplits=None):
        """
        >>> x = sc.parallelize([("a", 1), ("b", 4)])
        >>> y = sc.parallelize([("a", 2), ("a", 3)])
        >>> sorted(x.join(y).collect())
        [('a', (1, 2)), ('a', (1, 3))]
        """
        return python_join(self, other, numSplits)

    def leftOuterJoin(self, other, numSplits=None):
        """
        >>> x = sc.parallelize([("a", 1), ("b", 4)])
        >>> y = sc.parallelize([("a", 2)])
        >>> sorted(x.leftOuterJoin(y).collect())
        [('a', (1, 2)), ('b', (4, None))]
        """
        return python_left_outer_join(self, other, numSplits)

    def rightOuterJoin(self, other, numSplits=None):
        """
        >>> x = sc.parallelize([("a", 1), ("b", 4)])
        >>> y = sc.parallelize([("a", 2)])
        >>> sorted(y.rightOuterJoin(x).collect())
        [('a', (2, 1)), ('b', (None, 4))]
        """
        return python_right_outer_join(self, other, numSplits)

    def partitionBy(self, numSplits, hashFunc=hash):
        if numSplits is None:
            numSplits = self.ctx.defaultParallelism
        def add_shuffle_key(iterator):
            for (k, v) in iterator:
                yield str(hashFunc(k))
                yield dump_pickle((k, v))
        keyed = PipelinedRDD(self, add_shuffle_key)
        keyed._bypass_serializer = True
        pairRDD = self.ctx.jvm.PairwiseRDD(keyed._jrdd.rdd()).asJavaPairRDD()
        partitioner = self.ctx.jvm.spark.HashPartitioner(numSplits)
        jrdd = pairRDD.partitionBy(partitioner)
        jrdd = jrdd.map(self.ctx.jvm.ExtractValue())
        return RDD(jrdd, self.ctx)

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                     numSplits=None):
        """
        >>> x = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> def f(x): return x
        >>> def add(a, b): return a + str(b)
        >>> sorted(x.combineByKey(str, add, add).collect())
        [('a', '11'), ('b', '1')]
        """
        if numSplits is None:
            numSplits = self.ctx.defaultParallelism
        def combineLocally(iterator):
            combiners = {}
            for (k, v) in iterator:
                if k not in combiners:
                    combiners[k] = createCombiner(v)
                else:
                    combiners[k] = mergeValue(combiners[k], v)
            return combiners.iteritems()
        locally_combined = self.mapPartitions(combineLocally)
        shuffled = locally_combined.partitionBy(numSplits)
        def _mergeCombiners(iterator):
            combiners = {}
            for (k, v) in iterator:
                if not k in combiners:
                    combiners[k] = v
                else:
                    combiners[k] = mergeCombiners(combiners[k], v)
            return combiners.iteritems()
        return shuffled.mapPartitions(_mergeCombiners)

    def groupByKey(self, numSplits=None):
        """
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
                numSplits)

    def flatMapValues(self, f):
        flat_map_fn = lambda (k, v): ((k, x) for x in f(v))
        return self.flatMap(flat_map_fn)

    def mapValues(self, f):
        map_values_fn = lambda (k, v): (k, f(v))
        return self.map(map_values_fn, preservesPartitioning=True)

    # TODO: support varargs cogroup of several RDDs.
    def groupWith(self, other):
        return self.cogroup(other)

    def cogroup(self, other, numSplits=None):
        """
        >>> x = sc.parallelize([("a", 1), ("b", 4)])
        >>> y = sc.parallelize([("a", 2)])
        >>> x.cogroup(y).collect()
        [('a', ([1], [2])), ('b', ([4], []))]
        """
        return python_cogroup(self, other, numSplits)

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
        if isinstance(prev, PipelinedRDD) and not prev.is_cached:
            prev_func = prev.func
            def pipeline_func(iterator):
                return func(prev_func(iterator))
            self.func = pipeline_func
            self.preservesPartitioning = \
                prev.preservesPartitioning and preservesPartitioning
            self._prev_jrdd = prev._prev_jrdd
        else:
            self.func = func
            self.preservesPartitioning = preservesPartitioning
            self._prev_jrdd = prev._jrdd
        self.is_cached = False
        self.ctx = prev.ctx
        self.prev = prev
        self._jrdd_val = None
        self._bypass_serializer = False

    @property
    def _jrdd(self):
        if self._jrdd_val:
            return self._jrdd_val
        funcs = [self.func, self._bypass_serializer]
        pipe_command = ' '.join(b64enc(cloudpickle.dumps(f)) for f in funcs)
        broadcast_vars = ListConverter().convert(
            [x._jbroadcast for x in self.ctx._pickled_broadcast_vars],
            self.ctx.gateway._gateway_client)
        self.ctx._pickled_broadcast_vars.clear()
        class_manifest = self._prev_jrdd.classManifest()
        python_rdd = self.ctx.jvm.PythonRDD(self._prev_jrdd.rdd(),
            pipe_command, self.preservesPartitioning, self.ctx.pythonExec,
            broadcast_vars, class_manifest)
        self._jrdd_val = python_rdd.asJavaRDD()
        return self._jrdd_val


def _test():
    import doctest
    from pyspark.context import SparkContext
    globs = globals().copy()
    globs['sc'] = SparkContext('local', 'PythonTest')
    doctest.testmod(globs=globs)
    globs['sc'].stop()


if __name__ == "__main__":
    _test()
