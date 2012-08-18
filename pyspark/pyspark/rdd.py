from base64 import standard_b64encode as b64enc

from pyspark import cloudpickle
from pyspark.serializers import PickleSerializer
from pyspark.join import python_join, python_left_outer_join, \
    python_right_outer_join, python_cogroup


class RDD(object):

    def __init__(self, jrdd, ctx):
        self._jrdd = jrdd
        self.is_cached = False
        self.ctx = ctx

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

    def map(self, f, preservesPartitioning=False):
        return MappedRDD(self, f, preservesPartitioning)

    def flatMap(self, f):
        """
        >>> rdd = sc.parallelize([2, 3, 4])
        >>> sorted(rdd.flatMap(lambda x: range(1, x)).collect())
        [1, 1, 1, 2, 2, 3]
        >>> sorted(rdd.flatMap(lambda x: [(x, x), (x, x)]).collect())
        [(2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)]
        """
        return MappedRDD(self, f, preservesPartitioning=False, command='flatmap')

    def filter(self, f):
        """
        >>> rdd = sc.parallelize([1, 2, 3, 4, 5])
        >>> rdd.filter(lambda x: x % 2 == 0).collect()
        [2, 4]
        """
        def filter_func(x): return x if f(x) else None
        return RDD(self._pipe(filter_func), self.ctx)

    def _pipe(self, functions, command="map"):
        class_manifest = self._jrdd.classManifest()
        pipe_command = RDD._get_pipe_command(command, functions)
        python_rdd = self.ctx.jvm.PythonRDD(self._jrdd.rdd(), pipe_command,
            False, self.ctx.pythonExec, class_manifest)
        return python_rdd.asJavaRDD()

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
        return [PickleSerializer.loads(x) for x in vals]

    def union(self, other):
        """
        >>> rdd = sc.parallelize([1, 1, 2, 3])
        >>> rdd.union(rdd).collect()
        [1, 1, 2, 3, 1, 1, 2, 3]
        """
        return RDD(self._jrdd.union(other._jrdd), self.ctx)

    # TODO: sort

    # TODO: Overload __add___?

    # TODO: glom

    def cartesian(self, other):
        """
        >>> rdd = sc.parallelize([1, 2])
        >>> sorted(rdd.cartesian(rdd).collect())
        [(1, 1), (1, 2), (2, 1), (2, 2)]
        """
        return RDD(self._jrdd.cartesian(other._jrdd), self.ctx)

    # numsplits
    def groupBy(self, f, numSplits=None):
        """
        >>> rdd = sc.parallelize([1, 1, 2, 3, 5, 8])
        >>> result = rdd.groupBy(lambda x: x % 2).collect()
        >>> sorted([(x, sorted(y)) for (x, y) in result])
        [(0, [2, 8]), (1, [1, 1, 3, 5])]
        """
        return self.map(lambda x: (f(x), x)).groupByKey(numSplits)

    # TODO: pipe

    # TODO: mapPartitions

    def foreach(self, f):
        """
        >>> def f(x): print x
        >>> sc.parallelize([1, 2, 3, 4, 5]).foreach(f)
        """
        self.map(f).collect()  # Force evaluation

    def collect(self):
        pickle = self.ctx.arrayAsPickle(self._jrdd.rdd().collect())
        return PickleSerializer.loads(bytes(pickle))

    def reduce(self, f):
        """
        >>> from operator import add
        >>> sc.parallelize([1, 2, 3, 4, 5]).reduce(add)
        15
        >>> sc.parallelize((2 for _ in range(10))).map(lambda x: 1).cache().reduce(add)
        10
        """
        vals = MappedRDD(self, f, command="reduce", preservesPartitioning=False).collect()
        return reduce(f, vals)

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
        pickle = self.ctx.arrayAsPickle(self._jrdd.rdd().take(num))
        return PickleSerializer.loads(bytes(pickle))

    def first(self):
        """
        >>> sc.parallelize([2, 3, 4]).first()
        2
        """
        return PickleSerializer.loads(bytes(self.ctx.asPickle(self._jrdd.first())))

    # TODO: saveAsTextFile

    # TODO: saveAsObjectFile

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
        >>> x = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        >>> sorted(x.reduceByKey(lambda a, b: a + b).collect())
        [('a', 2), ('b', 1)]
        """
        return self.combineByKey(lambda x: x, func, func, numSplits)

    # TODO: reduceByKeyLocally()

    # TODO: countByKey()

    # TODO: partitionBy

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

    # TODO: pipelining
    # TODO: optimizations
    def shuffle(self, numSplits):
        if numSplits is None:
            numSplits = self.ctx.defaultParallelism
        pipe_command = RDD._get_pipe_command('shuffle_map_step', [])
        class_manifest = self._jrdd.classManifest()
        python_rdd = self.ctx.jvm.PythonPairRDD(self._jrdd.rdd(),
            pipe_command, False, self.ctx.pythonExec, class_manifest)
        partitioner = self.ctx.jvm.spark.HashPartitioner(numSplits)
        jrdd = python_rdd.asJavaPairRDD().partitionBy(partitioner)
        jrdd = jrdd.map(self.ctx.jvm.ExtractValue())
        # TODO: extract second value.
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
        shuffled = self.shuffle(numSplits)
        functions = [createCombiner, mergeValue, mergeCombiners]
        jpairs = shuffled._pipe(functions, "combine_by_key")
        return RDD(jpairs, self.ctx)

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

    # TODO: implement shuffle.

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

    # TODO: file saving


class MappedRDD(RDD):
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
    def __init__(self, prev, func, preservesPartitioning=False, command='map'):
        if isinstance(prev, MappedRDD) and not prev.is_cached:
            prev_func = prev.func
            if command == 'reduce':
                if prev.command == 'flatmap':
                    def flatmap_reduce_func(x, acc):
                        values = prev_func(x)
                        if values is None:
                            return acc
                        if not acc:
                            if len(values) == 1:
                                return values[0]
                            else:
                                return reduce(func, values[1:], values[0])
                        else:
                            return reduce(func, values, acc)
                    self.func = flatmap_reduce_func
                else:
                    def reduce_func(x, acc):
                        val = prev_func(x)
                        if not val:
                            return acc
                        if acc is None:
                            return val
                        else:
                            return func(val, acc)
                    self.func = reduce_func
            else:
                if prev.command == 'flatmap':
                    command = 'flatmap'
                    self.func = lambda x: (func(y) for y in prev_func(x))
                else:
                    self.func = lambda x: func(prev_func(x))

            self.preservesPartitioning = \
                prev.preservesPartitioning and preservesPartitioning
            self._prev_jrdd = prev._prev_jrdd
            self.is_pipelined = True
        else:
            if command == 'reduce':
                def reduce_func(val, acc):
                    if acc is None:
                        return val
                    else:
                        return func(val, acc)
                self.func = reduce_func
            else:
                self.func = func
            self.preservesPartitioning = preservesPartitioning
            self._prev_jrdd = prev._jrdd
            self.is_pipelined = False
        self.is_cached = False
        self.ctx = prev.ctx
        self.prev = prev
        self._jrdd_val = None
        self.command = command

    @property
    def _jrdd(self):
        if not self._jrdd_val:
            funcs = [self.func]
            pipe_command = RDD._get_pipe_command(self.command, funcs)
            class_manifest = self._prev_jrdd.classManifest()
            python_rdd = self.ctx.jvm.PythonRDD(self._prev_jrdd.rdd(),
                pipe_command, self.preservesPartitioning, self.ctx.pythonExec,
                class_manifest)
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
