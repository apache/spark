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

from pyspark.mllib._common import \
    _get_unmangled_double_vector_rdd, _get_unmangled_rdd, \
    _serialize_double, _deserialize_double, _deserialize_double_matrix

class Statistics(object):

    @staticmethod
    def corr2(X, method=None):
        """
        # TODO Figure out what to do with naming for this method:
        If we stick with corr, all doctests are bypassed (i.e. Python confuses
        it with the other corr.

        Compute the correlation matrix for the input RDD of Vectors using the
        specified method.
        Methods currently supported: I{pearson (default), spearman}

        >>> from linalg import Vectors
        >>> from numpy import array
        >>> rdd = sc.parallelize([Vectors.dense([1, 0, 0, -2]), Vectors.dense([4, 5, 0, 3]), \
        ...                       Vectors.dense([6, 7, 0,  8]), Vectors.dense([9, 0, 0, 1])])
        >>> pearsonMat = array([[]])
        >>> Statistics.corr2(rdd) == Statistics.corr2(rdd, "pearson")
        True
        >>> Statistics.corr2(rdd, "spearman")
        """
        sc = X.ctx
        serialized = _get_unmangled_double_vector_rdd(X)
        resultMat = sc._jvm.PythonMLLibAPI().corr(serialized._jrdd, method)
        return _deserialize_double_matrix(resultMat)

    @staticmethod
    def corr(x, y, method=None):
        """
        Compute the correlation for the input RDDs using the specified method.
        Methods currently supported: I{pearson (default), spearman}

        # TODO fix the ordering bug:
        #  If the numPartitions = 2 is removed from parallelize, we end up getting
        #  [0.0, 1.0, -2.0] for x when the RDD is processed in Scala, which produce a
        #  Spearman correlation value of 0.0 instead of 0.5.
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

        """
        sc = x.ctx
        xSer = _get_unmangled_rdd(x, _serialize_double)
        ySer = _get_unmangled_rdd(y, _serialize_double)
        result = sc._jvm.PythonMLLibAPI().corr(xSer._jrdd, ySer._jrdd, method)
        return _deserialize_double(result)


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