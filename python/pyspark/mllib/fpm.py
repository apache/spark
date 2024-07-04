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

from typing import Any, Generic, List, NamedTuple, TypeVar

from pyspark import since, SparkContext
from pyspark.mllib.common import JavaModelWrapper, callMLlibFunc
from pyspark.mllib.util import JavaSaveable, JavaLoader, inherit_doc
from pyspark.core.rdd import RDD

__all__ = ["FPGrowth", "FPGrowthModel", "PrefixSpan", "PrefixSpanModel"]

T = TypeVar("T")


@inherit_doc
class FPGrowthModel(JavaModelWrapper, JavaSaveable, JavaLoader["FPGrowthModel"]):
    """
    A FP-Growth model for mining frequent itemsets
    using the Parallel FP-Growth algorithm.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> data = [["a", "b", "c"], ["a", "b", "d", "e"], ["a", "c", "e"], ["a", "c", "f"]]
    >>> rdd = sc.parallelize(data, 2)
    >>> model = FPGrowth.train(rdd, 0.6, 2)
    >>> sorted(model.freqItemsets().collect())
    [FreqItemset(items=['a'], freq=4), FreqItemset(items=['c'], freq=3), ...
    >>> model_path = temp_path + "/fpm"
    >>> model.save(sc, model_path)
    >>> sameModel = FPGrowthModel.load(sc, model_path)
    >>> sorted(model.freqItemsets().collect()) == sorted(sameModel.freqItemsets().collect())
    True
    """

    @since("1.4.0")
    def freqItemsets(self) -> RDD["FPGrowth.FreqItemset"]:
        """
        Returns the frequent itemsets of this model.
        """
        return self.call("getFreqItemsets").map(lambda x: (FPGrowth.FreqItemset(x[0], x[1])))

    @classmethod
    @since("2.0.0")
    def load(cls, sc: SparkContext, path: str) -> "FPGrowthModel":
        """
        Load a model from the given path.
        """
        model = cls._load_java(sc, path)
        assert sc._jvm is not None
        wrapper = sc._jvm.org.apache.spark.mllib.api.python.FPGrowthModelWrapper(model)
        return FPGrowthModel(wrapper)


class FPGrowth:
    """
    A Parallel FP-growth algorithm to mine frequent itemsets.

    .. versionadded:: 1.4.0
    """

    @classmethod
    def train(
        cls, data: RDD[List[T]], minSupport: float = 0.3, numPartitions: int = -1
    ) -> "FPGrowthModel":
        """
        Computes an FP-Growth model that contains frequent itemsets.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            The input data set, each element contains a transaction.
        minSupport : float, optional
            The minimal support level.
            (default: 0.3)
        numPartitions : int, optional
            The number of partitions used by parallel FP-growth. A value
            of -1 will use the same number as input data.
            (default: -1)
        """
        model = callMLlibFunc("trainFPGrowthModel", data, float(minSupport), int(numPartitions))
        return FPGrowthModel(model)

    class FreqItemset(NamedTuple):
        """
        Represents an (items, freq) tuple.

        .. versionadded:: 1.4.0
        """

        items: List[Any]
        freq: int


@inherit_doc
class PrefixSpanModel(JavaModelWrapper, Generic[T]):
    """
    Model fitted by PrefixSpan

    .. versionadded:: 1.6.0

    Examples
    --------
    >>> data = [
    ...    [["a", "b"], ["c"]],
    ...    [["a"], ["c", "b"], ["a", "b"]],
    ...    [["a", "b"], ["e"]],
    ...    [["f"]]]
    >>> rdd = sc.parallelize(data, 2)
    >>> model = PrefixSpan.train(rdd)
    >>> sorted(model.freqSequences().collect())
    [FreqSequence(sequence=[['a']], freq=3), FreqSequence(sequence=[['a'], ['a']], freq=1), ...
    """

    @since("1.6.0")
    def freqSequences(self) -> RDD["PrefixSpan.FreqSequence"]:
        """Gets frequent sequences"""
        return self.call("getFreqSequences").map(lambda x: PrefixSpan.FreqSequence(x[0], x[1]))


class PrefixSpan:
    """
    A parallel PrefixSpan algorithm to mine frequent sequential patterns.
    The PrefixSpan algorithm is described in Jian Pei et al (2001) [1]_

    .. versionadded:: 1.6.0

    .. [1] Jian Pei et al.,
        "PrefixSpan,: mining sequential patterns efficiently by prefix-projected pattern growth,"
        Proceedings 17th International Conference on Data Engineering, Heidelberg,
        Germany, 2001, pp. 215-224,
        doi: https://doi.org/10.1109/ICDE.2001.914830
    """

    @classmethod
    def train(
        cls,
        data: RDD[List[List[T]]],
        minSupport: float = 0.1,
        maxPatternLength: int = 10,
        maxLocalProjDBSize: int = 32000000,
    ) -> PrefixSpanModel[T]:
        """
        Finds the complete set of frequent sequential patterns in the
        input sequences of itemsets.

        .. versionadded:: 1.6.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            The input data set, each element contains a sequence of
            itemsets.
        minSupport : float, optional
            The minimal support level of the sequential pattern, any
            pattern that appears more than (minSupport *
            size-of-the-dataset) times will be output.
            (default: 0.1)
        maxPatternLength : int, optional
            The maximal length of the sequential pattern, any pattern
            that appears less than maxPatternLength will be output.
            (default: 10)
        maxLocalProjDBSize : int, optional
            The maximum number of items (including delimiters used in the
            internal storage format) allowed in a projected database before
            local processing. If a projected database exceeds this size,
            another iteration of distributed prefix growth is run.
            (default: 32000000)
        """
        model = callMLlibFunc(
            "trainPrefixSpanModel", data, minSupport, maxPatternLength, maxLocalProjDBSize
        )
        return PrefixSpanModel(model)

    class FreqSequence(NamedTuple):
        """
        Represents a (sequence, freq) tuple.

        .. versionadded:: 1.6.0
        """

        sequence: List[List[Any]]
        freq: int


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.mllib.fpm

    globs = pyspark.mllib.fpm.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("mllib.fpm tests").getOrCreate()
    globs["sc"] = spark.sparkContext
    import tempfile

    temp_path = tempfile.mkdtemp()
    globs["temp_path"] = temp_path
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
