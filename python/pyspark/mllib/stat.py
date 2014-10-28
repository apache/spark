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
Python package for statistical functions in MLlib.
"""

from functools import wraps

from pyspark import PickleSerializer
from pyspark.mllib.linalg import _convert_to_vector, _to_java_object_rdd


__all__ = ['MultivariateStatisticalSummary', 'Statistics']


def serialize(f):
    ser = PickleSerializer()

    @wraps(f)
    def func(self):
        jvec = f(self)
        bytes = self._sc._jvm.SerDe.dumps(jvec)
        return ser.loads(str(bytes)).toArray()

    return func


class MultivariateStatisticalSummary(object):

    """
    Trait for multivariate statistical summary of a data matrix.
    """

    def __init__(self, sc, java_summary):
        """
        :param sc:  Spark context
        :param java_summary:  Handle to Java summary object
        """
        self._sc = sc
        self._java_summary = java_summary

    def __del__(self):
        self._sc._gateway.detach(self._java_summary)

    @serialize
    def mean(self):
        return self._java_summary.mean()

    @serialize
    def variance(self):
        return self._java_summary.variance()

    def count(self):
        return self._java_summary.count()

    @serialize
    def numNonzeros(self):
        return self._java_summary.numNonzeros()

    @serialize
    def max(self):
        return self._java_summary.max()

    @serialize
    def min(self):
        return self._java_summary.min()


class Statistics(object):

    @staticmethod
    def colStats(rdd):
        """
        Computes column-wise summary statistics for the input RDD[Vector].

        >>> from pyspark.mllib.linalg import Vectors
        >>> rdd = sc.parallelize([Vectors.dense([2, 0, 0, -2]),
        ...                       Vectors.dense([4, 5, 0,  3]),
        ...                       Vectors.dense([6, 7, 0,  8])])
        >>> cStats = Statistics.colStats(rdd)
        >>> cStats.mean()
        array([ 4.,  4.,  0.,  3.])
        >>> cStats.variance()
        array([  4.,  13.,   0.,  25.])
        >>> cStats.count()
        3L
        >>> cStats.numNonzeros()
        array([ 3.,  2.,  0.,  3.])
        >>> cStats.max()
        array([ 6.,  7.,  0.,  8.])
        >>> cStats.min()
        array([ 2.,  0.,  0., -2.])
        """
        sc = rdd.ctx
        jrdd = _to_java_object_rdd(rdd.map(_convert_to_vector))
        cStats = sc._jvm.PythonMLLibAPI().colStats(jrdd)
        return MultivariateStatisticalSummary(sc, cStats)

    @staticmethod
    def corr(x, y=None, method=None):
        """
        Compute the correlation (matrix) for the input RDD(s) using the
        specified method.
        Methods currently supported: I{pearson (default), spearman}.

        If a single RDD of Vectors is passed in, a correlation matrix
        comparing the columns in the input RDD is returned. Use C{method=}
        to specify the method to be used for single RDD inout.
        If two RDDs of floats are passed in, a single float is returned.

        >>> x = sc.parallelize([1.0, 0.0, -2.0], 2)
        >>> y = sc.parallelize([4.0, 5.0, 3.0], 2)
        >>> zeros = sc.parallelize([0.0, 0.0, 0.0], 2)
        >>> abs(Statistics.corr(x, y) - 0.6546537) < 1e-7
        True
        >>> Statistics.corr(x, y) == Statistics.corr(x, y, "pearson")
        True
        >>> Statistics.corr(x, y, "spearman")
        0.5
        >>> from math import isnan
        >>> isnan(Statistics.corr(x, zeros))
        True
        >>> from pyspark.mllib.linalg import Vectors
        >>> rdd = sc.parallelize([Vectors.dense([1, 0, 0, -2]), Vectors.dense([4, 5, 0, 3]),
        ...                       Vectors.dense([6, 7, 0,  8]), Vectors.dense([9, 0, 0, 1])])
        >>> pearsonCorr = Statistics.corr(rdd)
        >>> print str(pearsonCorr).replace('nan', 'NaN')
        [[ 1.          0.05564149         NaN  0.40047142]
         [ 0.05564149  1.                 NaN  0.91359586]
         [        NaN         NaN  1.                 NaN]
         [ 0.40047142  0.91359586         NaN  1.        ]]
        >>> spearmanCorr = Statistics.corr(rdd, method="spearman")
        >>> print str(spearmanCorr).replace('nan', 'NaN')
        [[ 1.          0.10540926         NaN  0.4       ]
         [ 0.10540926  1.                 NaN  0.9486833 ]
         [        NaN         NaN  1.                 NaN]
         [ 0.4         0.9486833          NaN  1.        ]]
        >>> try:
        ...     Statistics.corr(rdd, "spearman")
        ...     print "Method name as second argument without 'method=' shouldn't be allowed."
        ... except TypeError:
        ...     pass
        """
        sc = x.ctx
        # Check inputs to determine whether a single value or a matrix is needed for output.
        # Since it's legal for users to use the method name as the second argument, we need to
        # check if y is used to specify the method name instead.
        if type(y) == str:
            raise TypeError("Use 'method=' to specify method name.")

        if not y:
            jx = _to_java_object_rdd(x.map(_convert_to_vector))
            resultMat = sc._jvm.PythonMLLibAPI().corr(jx, method)
            bytes = sc._jvm.SerDe.dumps(resultMat)
            ser = PickleSerializer()
            return ser.loads(str(bytes)).toArray()
        else:
            jx = _to_java_object_rdd(x.map(float))
            jy = _to_java_object_rdd(y.map(float))
            return sc._jvm.PythonMLLibAPI().corr(jx, jy, method)


def _test():
    import doctest
    from pyspark import SparkContext
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
