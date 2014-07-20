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

        """
        pass
        #TODO: make sure count implementation, thiis different from what pyspark does
        #return self._mapPartitions(lambda i: [sum(1 for _ in i)]).map(lambda x: (None, 1))

    def _sum(self):
        """
        """
        pass
        #return self._mapPartitions(lambda x: [sum(x)]).reduce(operator.add)

    def print_(self):
        """
        Since print is reserved name for python, we cannot make a print method function.
        This function prints serialized data in RDD in DStream because Scala and Java cannot
        deserialized pickled python object. Please use DStream.pyprint() instead to print result.

        Call DStream.print().
        """
        #hack to call print function in DStream
        getattr(self._jdstream, "print")()

    def pyprint(self):
        """
        Print the first ten elements of each RDD generated in this DStream. This is an output
        operator, so this DStream will be registered as an output stream and there materialized.

        """
        self._jdstream.pyprint()

    def filter(self, f):
        """
        Return DStream containing only the elements that satisfy predicate.
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
        Return DStream by applying a function to each element of DStream.
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
        While tracking the index of the original partition.
        """
        return PipelinedDStream(self, f, preservesPartitioning)

    def reduceByKey(self, func, numPartitions=None):
        """
        Merge the value for each key using an associative reduce function.

        This will also perform the merging locally on each mapper before
        sending resuls to reducer, similarly to a "combiner" in MapReduce.

        Output will be hash-partitioned with C{numPartitions} partitions, or
        the default parallelism level if C{numPartitions} is not specified.
        """
        return self.combineByKey(lambda x:x, func, func, numPartitions)

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

                #TODO for count operation make sure count implementation
                # This is different from what pyspark does
                #if isinstance(x, int):
                #    x = ("", x)

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
            return combiners.iteritems()
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
<<<<<<< HEAD
            return 2

    def getNumPartitions(self):
      """
      Returns the number of partitions in RDD
      >>> rdd = sc.parallelize([1, 2, 3, 4], 2)
      >>> rdd.getNumPartitions()
      2
      """
      return self._jdstream.partitions().size()
=======
            return self.getNumPartitions()

    def getNumPartitions(self):
        """
        Return the number of partitions in RDD
        """
        # TODO: remove hardcoding. RDD has NumPartitions but DStream does not have.
        return 2
>>>>>>> clean up code


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
        self.is_checkpointed = False
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
        return not (self.is_cached or self.is_checkpointed)
