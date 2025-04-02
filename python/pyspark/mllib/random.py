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

import sys
from functools import wraps
from typing import Any, Callable, Optional

import numpy as np

from pyspark.mllib.common import callMLlibFunc
from pyspark.core.context import SparkContext
from pyspark.core.rdd import RDD
from pyspark.mllib.linalg import Vector


__all__ = [
    "RandomRDDs",
]


def toArray(f: Callable[..., RDD[Vector]]) -> Callable[..., RDD[np.ndarray]]:
    @wraps(f)
    def func(sc: SparkContext, *a: Any, **kw: Any) -> RDD[np.ndarray]:
        rdd = f(sc, *a, **kw)
        return rdd.map(lambda vec: vec.toArray())

    return func


class RandomRDDs:
    """
    Generator methods for creating RDDs comprised of i.i.d samples from
    some distribution.

    .. versionadded:: 1.1.0
    """

    @staticmethod
    def uniformRDD(
        sc: SparkContext, size: int, numPartitions: Optional[int] = None, seed: Optional[int] = None
    ) -> RDD[float]:
        """
        Generates an RDD comprised of i.i.d. samples from the
        uniform distribution U(0.0, 1.0).

        To transform the distribution in the generated RDD from U(0.0, 1.0)
        to U(a, b), use
        ``RandomRDDs.uniformRDD(sc, n, p, seed).map(lambda v: a + (b - a) * v)``

        .. versionadded:: 1.1.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            used to create the RDD.
        size : int
            Size of the RDD.
        numPartitions : int, optional
            Number of partitions in the RDD (default: `sc.defaultParallelism`).
        seed : int, optional
            Random seed (default: a random long integer).

        Returns
        -------
        :py:class:`pyspark.RDD`
            RDD of float comprised of i.i.d. samples ~ `U(0.0, 1.0)`.

        Examples
        --------
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
    def normalRDD(
        sc: SparkContext, size: int, numPartitions: Optional[int] = None, seed: Optional[int] = None
    ) -> RDD[float]:
        """
        Generates an RDD comprised of i.i.d. samples from the standard normal
        distribution.

        To transform the distribution in the generated RDD from standard normal
        to some other normal N(mean, sigma^2), use
        ``RandomRDDs.normal(sc, n, p, seed).map(lambda v: mean + sigma * v)``

        .. versionadded:: 1.1.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            used to create the RDD.
        size : int
            Size of the RDD.
        numPartitions : int, optional
            Number of partitions in the RDD (default: `sc.defaultParallelism`).
        seed : int, optional
            Random seed (default: a random long integer).

        Returns
        -------
        :py:class:`pyspark.RDD`
            RDD of float comprised of i.i.d. samples ~ N(0.0, 1.0).

        Examples
        --------
        >>> x = RandomRDDs.normalRDD(sc, 1000, seed=1)
        >>> stats = x.stats()
        >>> stats.count()
        1000
        >>> bool(abs(stats.mean() - 0.0) < 0.1)
        True
        >>> bool(abs(stats.stdev() - 1.0) < 0.1)
        True
        """
        return callMLlibFunc("normalRDD", sc._jsc, size, numPartitions, seed)

    @staticmethod
    def logNormalRDD(
        sc: SparkContext,
        mean: float,
        std: float,
        size: int,
        numPartitions: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> RDD[float]:
        """
        Generates an RDD comprised of i.i.d. samples from the log normal
        distribution with the input mean and standard distribution.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            used to create the RDD.
        mean : float
            mean for the log Normal distribution
        std : float
            std for the log Normal distribution
        size : int
            Size of the RDD.
        numPartitions : int, optional
            Number of partitions in the RDD (default: `sc.defaultParallelism`).
        seed : int, optional
            Random seed (default: a random long integer).

        Returns
        -------
        RDD of float comprised of i.i.d. samples ~ log N(mean, std).

        Examples
        --------
        >>> from math import sqrt, exp
        >>> mean = 0.0
        >>> std = 1.0
        >>> expMean = exp(mean + 0.5 * std * std)
        >>> expStd = sqrt((exp(std * std) - 1.0) * exp(2.0 * mean + std * std))
        >>> x = RandomRDDs.logNormalRDD(sc, mean, std, 1000, seed=2)
        >>> stats = x.stats()
        >>> stats.count()
        1000
        >>> bool(abs(stats.mean() - expMean) < 0.5)
        True
        >>> from math import sqrt
        >>> bool(abs(stats.stdev() - expStd) < 0.5)
        True
        """
        return callMLlibFunc(
            "logNormalRDD", sc._jsc, float(mean), float(std), size, numPartitions, seed
        )

    @staticmethod
    def poissonRDD(
        sc: SparkContext,
        mean: float,
        size: int,
        numPartitions: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> RDD[float]:
        """
        Generates an RDD comprised of i.i.d. samples from the Poisson
        distribution with the input mean.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            SparkContext used to create the RDD.
        mean : float
            Mean, or lambda, for the Poisson distribution.
        size : int
            Size of the RDD.
        numPartitions : int, optional
            Number of partitions in the RDD (default: `sc.defaultParallelism`).
        seed : int, optional
            Random seed (default: a random long integer).

        Returns
        -------
        :py:class:`pyspark.RDD`
            RDD of float comprised of i.i.d. samples ~ Pois(mean).

        Examples
        --------
        >>> mean = 100.0
        >>> x = RandomRDDs.poissonRDD(sc, mean, 1000, seed=2)
        >>> stats = x.stats()
        >>> stats.count()
        1000
        >>> abs(stats.mean() - mean) < 0.5
        True
        >>> from math import sqrt
        >>> bool(abs(stats.stdev() - sqrt(mean)) < 0.5)
        True
        """
        return callMLlibFunc("poissonRDD", sc._jsc, float(mean), size, numPartitions, seed)

    @staticmethod
    def exponentialRDD(
        sc: SparkContext,
        mean: float,
        size: int,
        numPartitions: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> RDD[float]:
        """
        Generates an RDD comprised of i.i.d. samples from the Exponential
        distribution with the input mean.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            SparkContext used to create the RDD.
        mean : float
            Mean, or 1 / lambda, for the Exponential distribution.
        size : int
            Size of the RDD.
        numPartitions : int, optional
            Number of partitions in the RDD (default: `sc.defaultParallelism`).
        seed : int, optional
            Random seed (default: a random long integer).

        Returns
        -------
        :py:class:`pyspark.RDD`
            RDD of float comprised of i.i.d. samples ~ Exp(mean).

        Examples
        --------
        >>> mean = 2.0
        >>> x = RandomRDDs.exponentialRDD(sc, mean, 1000, seed=2)
        >>> stats = x.stats()
        >>> stats.count()
        1000
        >>> abs(stats.mean() - mean) < 0.5
        True
        >>> from math import sqrt
        >>> bool(abs(stats.stdev() - sqrt(mean)) < 0.5)
        True
        """
        return callMLlibFunc("exponentialRDD", sc._jsc, float(mean), size, numPartitions, seed)

    @staticmethod
    def gammaRDD(
        sc: SparkContext,
        shape: float,
        scale: float,
        size: int,
        numPartitions: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> RDD[float]:
        """
        Generates an RDD comprised of i.i.d. samples from the Gamma
        distribution with the input shape and scale.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            SparkContext used to create the RDD.
        shape : float
            shape (> 0) parameter for the Gamma distribution
        scale : float
            scale (> 0) parameter for the Gamma distribution
        size : int
            Size of the RDD.
        numPartitions : int, optional
            Number of partitions in the RDD (default: `sc.defaultParallelism`).
        seed : int, optional
            Random seed (default: a random long integer).

        Returns
        -------
        :py:class:`pyspark.RDD`
            RDD of float comprised of i.i.d. samples ~ Gamma(shape, scale).

        Examples
        --------
        >>> from math import sqrt
        >>> shape = 1.0
        >>> scale = 2.0
        >>> expMean = shape * scale
        >>> expStd = sqrt(shape * scale * scale)
        >>> x = RandomRDDs.gammaRDD(sc, shape, scale, 1000, seed=2)
        >>> stats = x.stats()
        >>> stats.count()
        1000
        >>> bool(abs(stats.mean() - expMean) < 0.5)
        True
        >>> bool(abs(stats.stdev() - expStd) < 0.5)
        True
        """
        return callMLlibFunc(
            "gammaRDD", sc._jsc, float(shape), float(scale), size, numPartitions, seed
        )

    @staticmethod
    @toArray
    def uniformVectorRDD(
        sc: SparkContext,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> RDD[Vector]:
        """
        Generates an RDD comprised of vectors containing i.i.d. samples drawn
        from the uniform distribution U(0.0, 1.0).

        .. versionadded:: 1.1.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            SparkContext used to create the RDD.
        numRows : int
            Number of Vectors in the RDD.
        numCols : int
            Number of elements in each Vector.
        numPartitions : int, optional
            Number of partitions in the RDD.
        seed : int, optional
            Seed for the RNG that generates the seed for the generator in each partition.

        Returns
        -------
        :py:class:`pyspark.RDD`
            RDD of Vector with vectors containing i.i.d samples ~ `U(0.0, 1.0)`.

        Examples
        --------
        >>> import numpy as np
        >>> mat = np.matrix(RandomRDDs.uniformVectorRDD(sc, 10, 10).collect())
        >>> mat.shape
        (10, 10)
        >>> bool(mat.max() <= 1.0 and mat.min() >= 0.0)
        True
        >>> RandomRDDs.uniformVectorRDD(sc, 10, 10, 4).getNumPartitions()
        4
        """
        return callMLlibFunc("uniformVectorRDD", sc._jsc, numRows, numCols, numPartitions, seed)

    @staticmethod
    @toArray
    def normalVectorRDD(
        sc: SparkContext,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> RDD[Vector]:
        """
        Generates an RDD comprised of vectors containing i.i.d. samples drawn
        from the standard normal distribution.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            SparkContext used to create the RDD.
        numRows : int
            Number of Vectors in the RDD.
        numCols : int
            Number of elements in each Vector.
        numPartitions : int, optional
            Number of partitions in the RDD (default: `sc.defaultParallelism`).
        seed : int, optional
            Random seed (default: a random long integer).

        Returns
        -------
        :py:class:`pyspark.RDD`
            RDD of Vector with vectors containing i.i.d. samples ~ `N(0.0, 1.0)`.

        Examples
        --------
        >>> import numpy as np
        >>> mat = np.matrix(RandomRDDs.normalVectorRDD(sc, 100, 100, seed=1).collect())
        >>> mat.shape
        (100, 100)
        >>> bool(abs(mat.mean() - 0.0) < 0.1)
        True
        >>> bool(abs(mat.std() - 1.0) < 0.1)
        True
        """
        return callMLlibFunc("normalVectorRDD", sc._jsc, numRows, numCols, numPartitions, seed)

    @staticmethod
    @toArray
    def logNormalVectorRDD(
        sc: SparkContext,
        mean: float,
        std: float,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> RDD[Vector]:
        """
        Generates an RDD comprised of vectors containing i.i.d. samples drawn
        from the log normal distribution.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            SparkContext used to create the RDD.
        mean : float
            Mean of the log normal distribution
        std : float
            Standard Deviation of the log normal distribution
        numRows : int
            Number of Vectors in the RDD.
        numCols : int
            Number of elements in each Vector.
        numPartitions : int, optional
            Number of partitions in the RDD (default: `sc.defaultParallelism`).
        seed : int, optional
            Random seed (default: a random long integer).

        Returns
        -------
        :py:class:`pyspark.RDD`
            RDD of Vector with vectors containing i.i.d. samples ~ log `N(mean, std)`.

        Examples
        --------
        >>> import numpy as np
        >>> from math import sqrt, exp
        >>> mean = 0.0
        >>> std = 1.0
        >>> expMean = exp(mean + 0.5 * std * std)
        >>> expStd = sqrt((exp(std * std) - 1.0) * exp(2.0 * mean + std * std))
        >>> m = RandomRDDs.logNormalVectorRDD(sc, mean, std, 100, 100, seed=1).collect()
        >>> mat = np.matrix(m)
        >>> mat.shape
        (100, 100)
        >>> bool(abs(mat.mean() - expMean) < 0.1)
        True
        >>> bool(abs(mat.std() - expStd) < 0.1)
        True
        """
        return callMLlibFunc(
            "logNormalVectorRDD",
            sc._jsc,
            float(mean),
            float(std),
            numRows,
            numCols,
            numPartitions,
            seed,
        )

    @staticmethod
    @toArray
    def poissonVectorRDD(
        sc: SparkContext,
        mean: float,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> RDD[Vector]:
        """
        Generates an RDD comprised of vectors containing i.i.d. samples drawn
        from the Poisson distribution with the input mean.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            SparkContext used to create the RDD.
        mean : float
            Mean, or lambda, for the Poisson distribution.
        numRows : float
            Number of Vectors in the RDD.
        numCols : int
            Number of elements in each Vector.
        numPartitions : int, optional
            Number of partitions in the RDD (default: `sc.defaultParallelism`)
        seed : int, optional
            Random seed (default: a random long integer).

        Returns
        -------
        :py:class:`pyspark.RDD`
            RDD of Vector with vectors containing i.i.d. samples ~ Pois(mean).

        Examples
        --------
        >>> import numpy as np
        >>> mean = 100.0
        >>> rdd = RandomRDDs.poissonVectorRDD(sc, mean, 100, 100, seed=1)
        >>> mat = np.asmatrix(rdd.collect())
        >>> mat.shape
        (100, 100)
        >>> bool(abs(mat.mean() - mean) < 0.5)
        True
        >>> from math import sqrt
        >>> bool(abs(mat.std() - sqrt(mean)) < 0.5)
        True
        """
        return callMLlibFunc(
            "poissonVectorRDD", sc._jsc, float(mean), numRows, numCols, numPartitions, seed
        )

    @staticmethod
    @toArray
    def exponentialVectorRDD(
        sc: SparkContext,
        mean: float,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> RDD[Vector]:
        """
        Generates an RDD comprised of vectors containing i.i.d. samples drawn
        from the Exponential distribution with the input mean.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            SparkContext used to create the RDD.
        mean : float
            Mean, or 1 / lambda, for the Exponential distribution.
        numRows : int
            Number of Vectors in the RDD.
        numCols : int
            Number of elements in each Vector.
        numPartitions : int, optional
            Number of partitions in the RDD (default: `sc.defaultParallelism`)
        seed : int, optional
            Random seed (default: a random long integer).

        Returns
        -------
        :py:class:`pyspark.RDD`
            RDD of Vector with vectors containing i.i.d. samples ~ Exp(mean).

        Examples
        --------
        >>> import numpy as np
        >>> mean = 0.5
        >>> rdd = RandomRDDs.exponentialVectorRDD(sc, mean, 100, 100, seed=1)
        >>> mat = np.asmatrix(rdd.collect())
        >>> mat.shape
        (100, 100)
        >>> bool(abs(mat.mean() - mean) < 0.5)
        True
        >>> from math import sqrt
        >>> bool(abs(mat.std() - sqrt(mean)) < 0.5)
        True
        """
        return callMLlibFunc(
            "exponentialVectorRDD", sc._jsc, float(mean), numRows, numCols, numPartitions, seed
        )

    @staticmethod
    @toArray
    def gammaVectorRDD(
        sc: SparkContext,
        shape: float,
        scale: float,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> RDD[Vector]:
        """
        Generates an RDD comprised of vectors containing i.i.d. samples drawn
        from the Gamma distribution.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
            SparkContext used to create the RDD.
        shape : float
            Shape (> 0) of the Gamma distribution
        scale : float
            Scale (> 0) of the Gamma distribution
        numRows : int
            Number of Vectors in the RDD.
        numCols : int
            Number of elements in each Vector.
        numPartitions : int, optional
            Number of partitions in the RDD (default: `sc.defaultParallelism`).
        seed : int, optional,
            Random seed (default: a random long integer).

        Returns
        -------
        :py:class:`pyspark.RDD`
            RDD of Vector with vectors containing i.i.d. samples ~ Gamma(shape, scale).

        Examples
        --------
        >>> import numpy as np
        >>> from math import sqrt
        >>> shape = 1.0
        >>> scale = 2.0
        >>> expMean = shape * scale
        >>> expStd = sqrt(shape * scale * scale)
        >>> mat = np.matrix(RandomRDDs.gammaVectorRDD(sc, shape, scale, 100, 100, seed=1).collect())
        >>> mat.shape
        (100, 100)
        >>> bool(abs(mat.mean() - expMean) < 0.1)
        True
        >>> bool(abs(mat.std() - expStd) < 0.1)
        True
        """
        return callMLlibFunc(
            "gammaVectorRDD",
            sc._jsc,
            float(shape),
            float(scale),
            numRows,
            numCols,
            numPartitions,
            seed,
        )


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession

    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder.master("local[2]").appName("mllib.random tests").getOrCreate()
    globs["sc"] = spark.sparkContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
