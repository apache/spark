from base64 import standard_b64encode as b64enc
import copy
from collections import defaultdict
from collections import namedtuple
from itertools import chain, ifilter, imap
import operator
import os
import sys
import shlex
import traceback
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from threading import Thread
import warnings
import heapq
from random import Random

from pyspark.serializers import NoOpSerializer, CartesianDeserializer, \
    BatchedSerializer, CloudPickleSerializer, PairDeserializer, pack_long
from pyspark.join import python_join, python_left_outer_join, \
    python_right_outer_join, python_cogroup
from pyspark.statcounter import StatCounter
from pyspark.rddsampler import RDDSampler
from pyspark.storagelevel import StorageLevel
#from pyspark.resultiterable import ResultIterable
from pyspark.rdd import _JavaStackTrace

from py4j.java_collections import ListConverter, MapConverter

__all__ = ["DStream"]

class DStream(object):
    def __init__(self, jdstream, ssc, jrdd_deserializer):
        self._jdstream = jdstream
        self._ssc = ssc
        self.ctx = ssc._sc
        self._jrdd_deserializer = jrdd_deserializer

    def generatedRDDs(self):
        """
         // RDDs generated, marked as private[streaming] so that testsuites can access it
         @transient
        """
        pass

    def print_(self):
        """
        """
        # print is a resrved name of Python. We cannot give print to function name
        getattr(self._jdstream, "print")()

    def pyprint(self):
        """
        """
        self._jdstream.pyprint()

    def cache(self):
        """
        """
        raise NotImplementedError

    def checkpoint(self):
        """
        """
        raise NotImplementedError

    def compute(self, time):
        """
        """
        raise NotImplementedError

    def context(self):
        """
        """
        raise NotImplementedError

    def count(self):
        """
        """
        raise NotImplementedError

    def countByValue(self, numPartitions=None):
        """
        """
        raise NotImplementedError

    def countByValueAndWindow(self, duration, slideDuration=None):
        """
        """
        raise NotImplementedError

    def countByWindow(self, duration, slideDuration=None):
        """
        """
        raise NotImplementedError

    def dstream(self):
        """
        """
        raise NotImplementedError

    def filter(self, f):
        """
        """
        def func(iterator): return ifilter(f, iterator)
        return self.mapPartitions(func)

    def flatMap(self, f, preservesPartitioning=False):
        """
        """
        def func(s, iterator): return chain.from_iterable(imap(f, iterator))
        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def foreachRDD(self, f, time):
        """
        """
        raise NotImplementedError

    def glom(self):
        """
        """
        raise NotImplementedError

    def map(self, f, preservesPartitioning=False):
        """
        """
        def func(split, iterator): return imap(f, iterator)
        return PipelinedDStream(self, func, preservesPartitioning)

    def mapPartitions(self, f):
        """
        """
        def func(s, iterator): return f(iterator)
        return self.mapPartitionsWithIndex(func)

    def perist(self, storageLevel):
        """
        """
        raise NotImplementedError

    def reduce(self, func, numPartitions=None):
        """

        """
        return self._combineByKey(lambda x:x, func, func, numPartitions)

    def _combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                      numPartitions = None):
        """
        """
        if numPartitions is None:
            numPartitions = self.ctx._defaultParallelism()
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
            #JavaDStream
            #pairRDD = self.ctx._jvm.PairwiseDStream(keyed._jdstream.dstream()).asJavaPairRDD()
            pairDStream = self.ctx._jvm.PairwiseDStream(keyed._jdstream.dstream()).asJavaPairDStream()
            partitioner = self.ctx._jvm.PythonPartitioner(numPartitions,
                                                          id(partitionFunc))
        jdstream = pairDStream.partitionBy(partitioner).values()
        dstream = DStream(jdstream, self._ssc, BatchedSerializer(outputSerializer))
        # This is required so that id(partitionFunc) remains unique, even if
        # partitionFunc is a lambda:
        dstream._partitionFunc = partitionFunc
        return dstream



    def reduceByWindow(self, reduceFunc, windowDuration, slideDuration, inReduceTunc):
        """
        """

        raise NotImplementedError

    def repartition(self, numPartitions):
        """
        """
        raise NotImplementedError

    def slice(self, fromTime, toTime):
        """
        """
        raise NotImplementedError

    def transform(self, transformFunc):
        """
        """
        raise NotImplementedError

    def transformWith(self, other, transformFunc):
        """
        """
        raise NotImplementedError

    def union(self, that):
        """
        """
        raise NotImplementedError

    def window(self, windowDuration, slideDuration=None):
        """
        """
        raise NotImplementedError

    def wrapRDD(self, rdd):
        """
        """
        raise NotImplementedError

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        return PipelinedDStream(self, f, preservesPartitioning)


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
