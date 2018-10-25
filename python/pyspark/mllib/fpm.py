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

import sys

import numpy
from numpy import array
from collections import namedtuple

from pyspark import SparkContext, since
from pyspark.rdd import ignore_unicode_prefix
from pyspark.mllib.common import JavaModelWrapper, callMLlibFunc
from pyspark.mllib.util import JavaSaveable, JavaLoader, inherit_doc

__all__ = ['FPGrowth', 'FPGrowthModel', 'PrefixSpan', 'PrefixSpanModel']


@inherit_doc
@ignore_unicode_prefix
class FPGrowthModel(JavaModelWrapper, JavaSaveable, JavaLoader):
    """
    A FP-Growth model for mining frequent itemsets
    using the Parallel FP-Growth algorithm.

    >>> data = [["a", "b", "c"], ["a", "b", "d", "e"], ["a", "c", "e"], ["a", "c", "f"]]
    >>> rdd = sc.parallelize(data, 2)
    >>> model = FPGrowth.train(rdd, 0.6, 2)
    >>> sorted(model.freqItemsets().collect())
    [FreqItemset(items=[u'a'], freq=4), FreqItemset(items=[u'c'], freq=3), ...
    >>> model_path = temp_path + "/fpm"
    >>> model.save(sc, model_path)
    >>> sameModel = FPGrowthModel.load(sc, model_path)
    >>> sorted(model.freqItemsets().collect()) == sorted(sameModel.freqItemsets().collect())
    True

    .. versionadded:: 1.4.0
    """

    @since("1.4.0")
    def freqItemsets(self):
        """
        Returns the frequent itemsets of this model.
        """
        return self.call("getFreqItemsets").map(lambda x: (FPGrowth.FreqItemset(x[0], x[1])))

    @classmethod
    @since("2.0.0")
    def load(cls, sc, path):
        """
        Load a model from the given path.
        """
        model = cls._load_java(sc, path)
        wrapper = sc._jvm.org.apache.spark.mllib.api.python.FPGrowthModelWrapper(model)
        return FPGrowthModel(wrapper)


class FPGrowth(object):
    """
    A Parallel FP-growth algorithm to mine frequent itemsets.

    .. versionadded:: 1.4.0
    """

    @classmethod
    @since("1.4.0")
    def train(cls, data, minSupport=0.3, numPartitions=-1):
        """
        Computes an FP-Growth model that contains frequent itemsets.

        :param data:
          The input data set, each element contains a transaction.
        :param minSupport:
          The minimal support level.
          (default: 0.3)
        :param numPartitions:
          The number of partitions used by parallel FP-growth. A value
          of -1 will use the same number as input data.
          (default: -1)
        """
        model = callMLlibFunc("trainFPGrowthModel", data, float(minSupport), int(numPartitions))
        return FPGrowthModel(model)

    class FreqItemset(namedtuple("FreqItemset", ["items", "freq"])):
        """
        Represents an (items, freq) tuple.

        .. versionadded:: 1.4.0
        """


@inherit_doc
@ignore_unicode_prefix
class PrefixSpanModel(JavaModelWrapper):
    """
    Model fitted by PrefixSpan

    >>> data = [
    ...    [["a", "b"], ["c"]],
    ...    [["a"], ["c", "b"], ["a", "b"]],
    ...    [["a", "b"], ["e"]],
    ...    [["f"]]]
    >>> rdd = sc.parallelize(data, 2)
    >>> model = PrefixSpan.train(rdd)
    >>> sorted(model.freqSequences().collect())
    [FreqSequence(sequence=[[u'a']], freq=3), FreqSequence(sequence=[[u'a'], [u'a']], freq=1), ...

    .. versionadded:: 1.6.0
    """

    @since("1.6.0")
    def freqSequences(self):
        """Gets frequent sequences"""
        return self.call("getFreqSequences").map(lambda x: PrefixSpan.FreqSequence(x[0], x[1]))


class PrefixSpan(object):
    """
    A parallel PrefixSpan algorithm to mine frequent sequential patterns.
    The PrefixSpan algorithm is described in J. Pei, et al., PrefixSpan:
    Mining Sequential Patterns Efficiently by Prefix-Projected Pattern Growth
    ([[http://doi.org/10.1109/ICDE.2001.914830]]).

    .. versionadded:: 1.6.0
    """

    @classmethod
    @since("1.6.0")
    def train(cls, data, minSupport=0.1, maxPatternLength=10, maxLocalProjDBSize=32000000):
        """
        Finds the complete set of frequent sequential patterns in the
        input sequences of itemsets.

        :param data:
          The input data set, each element contains a sequence of
          itemsets.
        :param minSupport:
          The minimal support level of the sequential pattern, any
          pattern that appears more than (minSupport *
          size-of-the-dataset) times will be output.
          (default: 0.1)
        :param maxPatternLength:
          The maximal length of the sequential pattern, any pattern
          that appears less than maxPatternLength will be output.
          (default: 10)
        :param maxLocalProjDBSize:
          The maximum number of items (including delimiters used in the
          internal storage format) allowed in a projected database before
          local processing. If a projected database exceeds this size,
          another iteration of distributed prefix growth is run.
          (default: 32000000)
        """
        model = callMLlibFunc("trainPrefixSpanModel",
                              data, minSupport, maxPatternLength, maxLocalProjDBSize)
        return PrefixSpanModel(model)

    class FreqSequence(namedtuple("FreqSequence", ["sequence", "freq"])):
        """
        Represents a (sequence, freq) tuple.

        .. versionadded:: 1.6.0
        """


def _test():
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.mllib.fpm
    globs = pyspark.mllib.fpm.__dict__.copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("mllib.fpm tests")\
        .getOrCreate()
    globs['sc'] = spark.sparkContext
    import tempfile

    temp_path = tempfile.mkdtemp()
    globs['temp_path'] = temp_path
    try:
        (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
        spark.stop()
    finally:
        from shutil import rmtree
        try:
            rmtree(temp_path)
        except OSError:
            pass
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
