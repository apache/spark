from base64 import standard_b64encode as b64enc
from pyspark import cloudpickle
from itertools import chain

from pyspark.serializers import PairSerializer, NopSerializer, \
    OptionSerializer, ArraySerializer
from pyspark.join import python_join, python_left_outer_join, \
    python_right_outer_join, python_cogroup


class RDD(object):

    def __init__(self, jrdd, ctx, serializer=None):
        self._jrdd = jrdd
        self.is_cached = False
        self.ctx = ctx
        self.serializer = serializer or ctx.defaultSerializer

    def _builder(self, jrdd, ctx):
        return RDD(jrdd, ctx, self.serializer)

    @property
    def id(self):
        return self._jrdd.id()

    @property
    def splits(self):
        return self._jrdd.splits()

    @classmethod
    def _get_pipe_command(cls, command, functions):
        if functions and not isinstance(functions, (list, tuple)):
            functions = [functions]
        worker_args = [command]
        for f in functions:
            worker_args.append(b64enc(cloudpickle.dumps(f)))
        return " ".join(worker_args)

    def cache(self):
        self.is_cached = True
        self._jrdd.cache()
        return self

    def map(self, f, serializer=None, preservesPartitioning=False):
        return MappedRDD(self, f, serializer, preservesPartitioning)

    def mapPairs(self, f, keySerializer=None, valSerializer=None,
                 preservesPartitioning=False):
        return PairMappedRDD(self, f, keySerializer, valSerializer,
                             preservesPartitioning)

    def flatMap(self, f, serializer=None):
        """
        >>> rdd = sc.parallelize([2, 3, 4])
        >>> sorted(rdd.flatMap(lambda x: range(1, x)).collect())
        [1, 1, 1, 2, 2, 3]
        """
        serializer = serializer or self.ctx.defaultSerializer
        dumps = serializer.dumps
        loads = self.serializer.loads
        def func(x):
            pickled_elems = (dumps(y) for y in f(loads(x)))
            return "\n".join(pickled_elems) or None
        pipe_command = RDD._get_pipe_command("map", [func])
        class_manifest = self._jrdd.classManifest()
        jrdd = self.ctx.jvm.PythonRDD(self._jrdd.rdd(), pipe_command,
                                      False, self.ctx.pythonExec,
                                      class_manifest).asJavaRDD()
        return RDD(jrdd, self.ctx, serializer)

    def flatMapPairs(self, f, keySerializer=None, valSerializer=None,
                     preservesPartitioning=False):
        """
        >>> rdd = sc.parallelize([2, 3, 4])
        >>> sorted(rdd.flatMapPairs(lambda x: [(x, x), (x, x)]).collect())
        [(2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)]
        """
        keySerializer = keySerializer or self.ctx.defaultSerializer
        valSerializer = valSerializer or self.ctx.defaultSerializer
        dumpk = keySerializer.dumps
        dumpv = valSerializer.dumps
        loads = self.serializer.loads
        def func(x):
            pairs = f(loads(x))
            pickled_pairs = ((dumpk(k), dumpv(v)) for (k, v) in pairs)
            return "\n".join(chain.from_iterable(pickled_pairs)) or None
        pipe_command = RDD._get_pipe_command("map", [func])
        class_manifest = self._jrdd.classManifest()
        python_rdd = self.ctx.jvm.PythonPairRDD(self._jrdd.rdd(), pipe_command,
            preservesPartitioning, self.ctx.pythonExec, class_manifest)
        return PairRDD(python_rdd.asJavaPairRDD(), self.ctx, keySerializer,
           valSerializer)

    def filter(self, f):
        """
        >>> rdd = sc.parallelize([1, 2, 3, 4, 5])
        >>> rdd.filter(lambda x: x % 2 == 0).collect()
        [2, 4]
        """
        loads = self.serializer.loads
        def filter_func(x): return x if f(loads(x)) else None
        return self._builder(self._pipe(filter_func), self.ctx)

    def _pipe(self, functions, command="map"):
        class_manifest = self._jrdd.classManifest()
        pipe_command = RDD._get_pipe_command(command, functions)
        python_rdd = self.ctx.jvm.PythonRDD(self._jrdd.rdd(), pipe_command,
            False, self.ctx.pythonExec, class_manifest)
        return python_rdd.asJavaRDD()

    def _pipePairs(self, functions, command="mapPairs",
            preservesPartitioning=False):
        class_manifest = self._jrdd.classManifest()
        pipe_command = RDD._get_pipe_command(command, functions)
        python_rdd = self.ctx.jvm.PythonPairRDD(self._jrdd.rdd(), pipe_command,
                preservesPartitioning, self.ctx.pythonExec, class_manifest)
        return python_rdd.asJavaPairRDD()

    def distinct(self):
        """
        >>> sorted(sc.parallelize([1, 1, 2, 3]).distinct().collect())
        [1, 2, 3]
        """
        if self.serializer.is_comparable:
            return self._builder(self._jrdd.distinct(), self.ctx)
        return self.mapPairs(lambda x: (x, "")) \
                   .reduceByKey(lambda x, _: x) \
                   .map(lambda (x, _): x)

    def sample(self, withReplacement, fraction, seed):
        jrdd = self._jrdd.sample(withReplacement, fraction, seed)
        return self._builder(jrdd, self.ctx)

    def takeSample(self, withReplacement, num, seed):
        vals = self._jrdd.takeSample(withReplacement, num, seed)
        return [self.serializer.loads(self.ctx.python_dump(x)) for x in vals]

    def union(self, other):
        """
        >>> rdd = sc.parallelize([1, 1, 2, 3])
        >>> rdd.union(rdd).collect()
        [1, 1, 2, 3, 1, 1, 2, 3]
        """
        return self._builder(self._jrdd.union(other._jrdd), self.ctx)

    # TODO: sort

    # TODO: Overload __add___?

    # TODO: glom

    def cartesian(self, other):
        """
        >>> rdd = sc.parallelize([1, 2])
        >>> sorted(rdd.cartesian(rdd).collect())
        [(1, 1), (1, 2), (2, 1), (2, 2)]
        """
        return PairRDD(self._jrdd.cartesian(other._jrdd), self.ctx)

    # numsplits
    def groupBy(self, f, numSplits=None):
        """
        >>> rdd = sc.parallelize([1, 1, 2, 3, 5, 8])
        >>> sorted(rdd.groupBy(lambda x: x % 2).collect())
        [(0, [2, 8]), (1, [1, 1, 3, 5])]
        """
        return self.mapPairs(lambda x: (f(x), x)).groupByKey(numSplits)

    # TODO: pipe

    # TODO: mapPartitions

    def foreach(self, f):
        """
        >>> def f(x): print x
        >>> sc.parallelize([1, 2, 3, 4, 5]).foreach(f)
        """
        self.map(f).collect()  # Force evaluation

    def collect(self):
        vals = self._jrdd.collect()
        return [self.serializer.loads(self.ctx.python_dump(x)) for x in vals]

    def reduce(self, f, serializer=None):
        """
        >>> import operator
        >>> sc.parallelize([1, 2, 3, 4, 5]).reduce(operator.add)
        15
        """
        serializer = serializer or self.ctx.defaultSerializer
        loads = self.serializer.loads
        dumps = serializer.dumps
        def reduceFunction(x, acc):
            if acc is None:
                return loads(x)
            else:
                return f(loads(x), acc)
        vals = self._pipe([reduceFunction, dumps], command="reduce").collect()
        return reduce(f, (serializer.loads(x) for x in vals))

    # TODO: fold

    # TODO: aggregate

    def count(self):
        """
        >>> sc.parallelize([2, 3, 4]).count()
        3L
        """
        return self._jrdd.count()

    # TODO: count approx methods

    def take(self, num):
        """
        >>> sc.parallelize([2, 3, 4]).take(2)
        [2, 3]
        """
        vals = self._jrdd.take(num)
        return [self.serializer.loads(self.ctx.python_dump(x)) for x in vals]

    def first(self):
        """
        >>> sc.parallelize([2, 3, 4]).first()
        2
        """
        return self.serializer.loads(self.ctx.python_dump(self._jrdd.first()))

    # TODO: saveAsTextFile

    # TODO: saveAsObjectFile


class PairRDD(RDD):

    def __init__(self, jrdd, ctx, keySerializer=None, valSerializer=None):
        RDD.__init__(self, jrdd, ctx)
        self.keySerializer = keySerializer or ctx.defaultSerializer
        self.valSerializer = valSerializer or ctx.defaultSerializer
        self.serializer = \
            PairSerializer(self.keySerializer, self.valSerializer)

    def _builder(self, jrdd, ctx):
        return PairRDD(jrdd, ctx, self.keySerializer, self.valSerializer)

    def reduceByKey(self, func, numSplits=None):
        """
        >>> x = sc.parallelizePairs([("a", 1), ("b", 1), ("a", 1)])
        >>> sorted(x.reduceByKey(lambda a, b: a + b).collect())
        [('a', 2), ('b', 1)]
        """
        return self.combineByKey(lambda x: x, func, func, numSplits)

    # TODO: reduceByKeyLocally()

    # TODO: countByKey()

    # TODO: partitionBy

    def join(self, other, numSplits=None):
        """
        >>> x = sc.parallelizePairs([("a", 1), ("b", 4)])
        >>> y = sc.parallelizePairs([("a", 2), ("a", 3)])
        >>> x.join(y).collect()
        [('a', (1, 2)), ('a', (1, 3))]

        Check that we get a PairRDD-like object back:
        >>> assert x.join(y).join
        """
        assert self.keySerializer.name == other.keySerializer.name
        if self.keySerializer.is_comparable:
            return PairRDD(self._jrdd.join(other._jrdd),
                self.ctx, self.keySerializer,
                PairSerializer(self.valSerializer, other.valSerializer))
        else:
            return python_join(self, other, numSplits)

    def leftOuterJoin(self, other, numSplits=None):
        """
        >>> x = sc.parallelizePairs([("a", 1), ("b", 4)])
        >>> y = sc.parallelizePairs([("a", 2)])
        >>> sorted(x.leftOuterJoin(y).collect())
        [('a', (1, 2)), ('b', (4, None))]
        """
        assert self.keySerializer.name == other.keySerializer.name
        if self.keySerializer.is_comparable:
            return PairRDD(self._jrdd.leftOuterJoin(other._jrdd),
                self.ctx, self.keySerializer,
                PairSerializer(self.valSerializer,
                               OptionSerializer(other.valSerializer)))
        else:
            return python_left_outer_join(self, other, numSplits)

    def rightOuterJoin(self, other, numSplits=None):
        """
        >>> x = sc.parallelizePairs([("a", 1), ("b", 4)])
        >>> y = sc.parallelizePairs([("a", 2)])
        >>> sorted(y.rightOuterJoin(x).collect())
        [('a', (2, 1)), ('b', (None, 4))]
        """
        assert self.keySerializer.name == other.keySerializer.name
        if self.keySerializer.is_comparable:
            return PairRDD(self._jrdd.rightOuterJoin(other._jrdd),
                self.ctx, self.keySerializer,
                PairSerializer(OptionSerializer(self.valSerializer),
                               other.valSerializer))
        else:
            return python_right_outer_join(self, other, numSplits)

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                     numSplits=None, serializer=None):
        """
        >>> x = sc.parallelizePairs([("a", 1), ("b", 1), ("a", 1)])
        >>> def f(x): return x
        >>> def add(a, b): return a + str(b)
        >>> sorted(x.combineByKey(str, add, add).collect())
        [('a', '11'), ('b', '1')]
        """
        serializer = serializer or self.ctx.defaultSerializer
        if numSplits is None:
            numSplits = self.ctx.defaultParallelism
        # Use hash() to create keys that are comparable in Java.
        loadkv = self.serializer.loads
        def pairify(kv):
            # TODO: add method to deserialize only the key or value from
            # a PairSerializer?
            key = loadkv(kv)[0]
            return (str(hash(key)), kv)
        partitioner = self.ctx.jvm.spark.HashPartitioner(numSplits)
        jrdd = self._pipePairs(pairify).partitionBy(partitioner)
        pairified = PairRDD(jrdd, self.ctx, NopSerializer, self.serializer)

        loads = PairSerializer(NopSerializer, self.serializer).loads
        dumpk = self.keySerializer.dumps
        dumpc = serializer.dumps

        functions = [createCombiner, mergeValue, mergeCombiners, loads, dumpk,
                     dumpc]
        jpairs = pairified._pipePairs(functions, "combine_by_key",
                                      preservesPartitioning=True)
        return PairRDD(jpairs, self.ctx, self.keySerializer, serializer)

    def groupByKey(self, numSplits=None):
        """
        >>> x = sc.parallelizePairs([("a", 1), ("b", 1), ("a", 1)])
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

    def collectAsMap(self):
        """
        >>> m = sc.parallelizePairs([(1, 2), (3, 4)]).collectAsMap()
        >>> m[1]
        2
        >>> m[3]
        4
        """
        m = self._jrdd.collectAsMap()
        def loads(x):
            (k, v) = x
            return (self.keySerializer.loads(k), self.valSerializer.loads(v))
        return dict(loads(x) for x in m.items())

    def flatMapValues(self, f, valSerializer=None):
        flat_map_fn = lambda (k, v): ((k, x) for x in f(v))
        return self.flatMapPairs(flat_map_fn, self.keySerializer,
                                 valSerializer, True)

    def mapValues(self, f, valSerializer=None):
        map_values_fn = lambda (k, v): (k, f(v))
        return self.mapPairs(map_values_fn, self.keySerializer, valSerializer,
                             True)

    # TODO: support varargs cogroup of several RDDs.
    def groupWith(self, other):
        return self.cogroup(other)

    def cogroup(self, other, numSplits=None):
        """
        >>> x = sc.parallelizePairs([("a", 1), ("b", 4)])
        >>> y = sc.parallelizePairs([("a", 2)])
        >>> x.cogroup(y).collect()
        [('a', ([1], [2])), ('b', ([4], []))]
        """
        assert self.keySerializer.name == other.keySerializer.name
        resultValSerializer = PairSerializer(
            ArraySerializer(self.valSerializer),
            ArraySerializer(other.valSerializer))
        if self.keySerializer.is_comparable:
            return PairRDD(self._jrdd.cogroup(other._jrdd),
                self.ctx, self.keySerializer, resultValSerializer)
        else:
            return python_cogroup(self, other, numSplits)

    # TODO: `lookup` is disabled because we can't make direct comparisons based
    # on the key; we need to compare the hash of the key to the hash of the
    # keys in the pairs.  This could be an expensive operation, since those
    # hashes aren't retained.

    # TODO: file saving


class MappedRDDBase(object):
    def __init__(self, prev, func, serializer, preservesPartitioning=False):
        if isinstance(prev, MappedRDDBase) and not prev.is_cached:
            prev_func = prev.func
            self.func = lambda x: func(prev_func(x))
            self.preservesPartitioning = \
                prev.preservesPartitioning and preservesPartitioning
            self._prev_jrdd = prev._prev_jrdd
            self._prev_serializer = prev._prev_serializer
        else:
            self.func = func
            self.preservesPartitioning = preservesPartitioning
            self._prev_jrdd = prev._jrdd
            self._prev_serializer = prev.serializer
        self.serializer = serializer or prev.ctx.defaultSerializer
        self.is_cached = False
        self.ctx = prev.ctx
        self.prev = prev
        self._jrdd_val = None


class MappedRDD(MappedRDDBase, RDD):
    """
    >>> rdd = sc.parallelize([1, 2, 3, 4])
    >>> rdd.map(lambda x: 2 * x).cache().map(lambda x: 2 * x).collect()
    [4, 8, 12, 16]
    >>> rdd.map(lambda x: 2 * x).map(lambda x: 2 * x).collect()
    [4, 8, 12, 16]
    """

    @property
    def _jrdd(self):
        if not self._jrdd_val:
            udf = self.func
            loads = self._prev_serializer.loads
            dumps = self.serializer.dumps
            func = lambda x: dumps(udf(loads(x)))
            pipe_command = RDD._get_pipe_command("map", [func])
            class_manifest = self._prev_jrdd.classManifest()
            python_rdd = self.ctx.jvm.PythonRDD(self._prev_jrdd.rdd(),
                pipe_command, self.preservesPartitioning, self.ctx.pythonExec,
                class_manifest)
            self._jrdd_val = python_rdd.asJavaRDD()
        return self._jrdd_val


class PairMappedRDD(MappedRDDBase, PairRDD):
    """
    >>> rdd = sc.parallelize([1, 2, 3, 4])
    >>> rdd.mapPairs(lambda x: (x, x)) \\
    ...    .mapPairs(lambda (x, y): (2*x, 2*y)) \\
    ...    .collect()
    [(2, 2), (4, 4), (6, 6), (8, 8)]
    >>> rdd.mapPairs(lambda x: (x, x)) \\
    ...    .mapPairs(lambda (x, y): (2*x, 2*y)) \\
    ...    .map(lambda (x, _): x).collect()
    [2, 4, 6, 8]
    """

    def __init__(self, prev, func, keySerializer=None, valSerializer=None,
                 preservesPartitioning=False):
        self.keySerializer = keySerializer or prev.ctx.defaultSerializer
        self.valSerializer = valSerializer or prev.ctx.defaultSerializer
        serializer = PairSerializer(self.keySerializer, self.valSerializer)
        MappedRDDBase.__init__(self, prev, func, serializer,
                               preservesPartitioning)

    @property
    def _jrdd(self):
        if not self._jrdd_val:
            udf = self.func
            loads = self._prev_serializer.loads
            dumpk = self.keySerializer.dumps
            dumpv = self.valSerializer.dumps
            def func(x):
                (k, v) = udf(loads(x))
                return (dumpk(k), dumpv(v))
            pipe_command = RDD._get_pipe_command("mapPairs", [func])
            class_manifest = self._prev_jrdd.classManifest()
            self._jrdd_val = self.ctx.jvm.PythonPairRDD(self._prev_jrdd.rdd(),
                pipe_command, self.preservesPartitioning, self.ctx.pythonExec,
                class_manifest).asJavaPairRDD()
        return self._jrdd_val


def _test():
    import doctest
    from pyspark.context import SparkContext
    from pyspark.serializers import PickleSerializer, JSONSerializer
    globs = globals().copy()
    globs['sc'] = SparkContext('local', 'PythonTest',
                               defaultSerializer=JSONSerializer)
    doctest.testmod(globs=globs)
    globs['sc'].stop()
    globs['sc'] = SparkContext('local', 'PythonTest',
                               defaultSerializer=PickleSerializer)
    doctest.testmod(globs=globs)
    globs['sc'].stop()


if __name__ == "__main__":
    _test()
