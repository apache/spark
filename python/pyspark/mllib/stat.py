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

from pyspark.mllib._common import \
    _get_unmangled_double_vector_rdd, _get_unmangled_rdd, \
    _serialize_double, _serialize_double_vector, \
    _deserialize_double, _deserialize_double_matrix


class Statistics(object):

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
        >>> from linalg import Vectors
        >>> rdd = sc.parallelize([Vectors.dense([1, 0, 0, -2]), Vectors.dense([4, 5, 0, 3]),
        ...                       Vectors.dense([6, 7, 0,  8]), Vectors.dense([9, 0, 0, 1])])
        >>> Statistics.corr(rdd)
        array([[ 1.        ,  0.05564149,         nan,  0.40047142],
               [ 0.05564149,  1.        ,         nan,  0.91359586],
               [        nan,         nan,  1.        ,         nan],
               [ 0.40047142,  0.91359586,         nan,  1.        ]])
        >>> Statistics.corr(rdd, method="spearman")
        array([[ 1.        ,  0.10540926,         nan,  0.4       ],
               [ 0.10540926,  1.        ,         nan,  0.9486833 ],
               [        nan,         nan,  1.        ,         nan],
               [ 0.4       ,  0.9486833 ,         nan,  1.        ]])
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
            try:
                Xser = _get_unmangled_double_vector_rdd(x)
            except TypeError:
                raise TypeError("corr called on a single RDD not consisted of Vectors.")
            resultMat = sc._jvm.PythonMLLibAPI().corr(Xser._jrdd, method)
            return _deserialize_double_matrix(resultMat)
        else:
            xSer = _get_unmangled_rdd(x, _serialize_double)
            ySer = _get_unmangled_rdd(y, _serialize_double)
            result = sc._jvm.PythonMLLibAPI().corr(xSer._jrdd, ySer._jrdd, method)
            return result


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
