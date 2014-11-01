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

"""
Python package for random data generation.
"""

from functools import wraps

from pyspark.mllib.common import callMLlibFunc


__all__ = ['RandomRDDs', ]


def toArray(f):
    @wraps(f)
    def func(sc, *a, **kw):
        rdd = f(sc, *a, **kw)
        return rdd.map(lambda vec: vec.toArray())
    return func


class RandomRDDs(object):
    """
    Generator methods for creating RDDs comprised of i.i.d samples from
    some distribution.
    """

    @staticmethod
    def uniformRDD(sc, size, numPartitions=None, seed=None):
        """
        Generates an RDD comprised of i.i.d. samples from the
        uniform distribution U(0.0, 1.0).

        To transform the distribution in the generated RDD from U(0.0, 1.0)
        to U(a, b), use
        C{RandomRDDs.uniformRDD(sc, n, p, seed)\
          .map(lambda v: a + (b - a) * v)}

        >>> x = RandomRDDs.uniformRDD(sc, 100).collect()
        >>> len(x)
        100
        >>> max(x) <= 1.0 and min(x) >= 0.0
        True
        >>> RandomRDDs.uniformRDD(sc, 100, 4).getNumPartitions()
        4
        >>> parts = RandomRDDs.uniformRDD(sc, 100, seed=4).getNumPartitions()
        >>> parts == sc.defaultParallelism
        True
        """
        return callMLlibFunc("uniformRDD", sc._jsc, size, numPartitions, seed)

    @staticmethod
    def normalRDD(sc, size, numPartitions=None, seed=None):
        """
        Generates an RDD comprised of i.i.d. samples from the standard normal
        distribution.

        To transform the distribution in the generated RDD from standard normal
        to some other normal N(mean, sigma^2), use
        C{RandomRDDs.normal(sc, n, p, seed)\
          .map(lambda v: mean + sigma * v)}

        >>> x = RandomRDDs.normalRDD(sc, 1000, seed=1L)
        >>> stats = x.stats()
        >>> stats.count()
        1000L
        >>> abs(stats.mean() - 0.0) < 0.1
        True
        >>> abs(stats.stdev() - 1.0) < 0.1
        True
        """
        return callMLlibFunc("normalRDD", sc._jsc, size, numPartitions, seed)

    @staticmethod
    def poissonRDD(sc, mean, size, numPartitions=None, seed=None):
        """
        Generates an RDD comprised of i.i.d. samples from the Poisson
        distribution with the input mean.

        >>> mean = 100.0
        >>> x = RandomRDDs.poissonRDD(sc, mean, 1000, seed=2L)
        >>> stats = x.stats()
        >>> stats.count()
        1000L
        >>> abs(stats.mean() - mean) < 0.5
        True
        >>> from math import sqrt
        >>> abs(stats.stdev() - sqrt(mean)) < 0.5
        True
        """
        return callMLlibFunc("poissonRDD", sc._jsc, mean, size, numPartitions, seed)

    @staticmethod
    @toArray
    def uniformVectorRDD(sc, numRows, numCols, numPartitions=None, seed=None):
        """
        Generates an RDD comprised of vectors containing i.i.d. samples drawn
        from the uniform distribution U(0.0, 1.0).

        >>> import numpy as np
        >>> mat = np.matrix(RandomRDDs.uniformVectorRDD(sc, 10, 10).collect())
        >>> mat.shape
        (10, 10)
        >>> mat.max() <= 1.0 and mat.min() >= 0.0
        True
        >>> RandomRDDs.uniformVectorRDD(sc, 10, 10, 4).getNumPartitions()
        4
        """
        return callMLlibFunc("uniformVectorRDD", sc._jsc, numRows, numCols, numPartitions, seed)

    @staticmethod
    @toArray
    def normalVectorRDD(sc, numRows, numCols, numPartitions=None, seed=None):
        """
        Generates an RDD comprised of vectors containing i.i.d. samples drawn
        from the standard normal distribution.

        >>> import numpy as np
        >>> mat = np.matrix(RandomRDDs.normalVectorRDD(sc, 100, 100, seed=1L).collect())
        >>> mat.shape
        (100, 100)
        >>> abs(mat.mean() - 0.0) < 0.1
        True
        >>> abs(mat.std() - 1.0) < 0.1
        True
        """
        return callMLlibFunc("normalVectorRDD", sc._jsc, numRows, numCols, numPartitions, seed)

    @staticmethod
    @toArray
    def poissonVectorRDD(sc, mean, numRows, numCols, numPartitions=None, seed=None):
        """
        Generates an RDD comprised of vectors containing i.i.d. samples drawn
        from the Poisson distribution with the input mean.

        >>> import numpy as np
        >>> mean = 100.0
        >>> rdd = RandomRDDs.poissonVectorRDD(sc, mean, 100, 100, seed=1L)
        >>> mat = np.mat(rdd.collect())
        >>> mat.shape
        (100, 100)
        >>> abs(mat.mean() - mean) < 0.5
        True
        >>> from math import sqrt
        >>> abs(mat.std() - sqrt(mean)) < 0.5
        True
        """
        return callMLlibFunc("poissonVectorRDD", sc._jsc, mean, numRows, numCols,
                             numPartitions, seed)


def _test():
    import doctest
    from pyspark.context import SparkContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    globs['sc'] = SparkContext('local[2]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
