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
if sys.version >= '3':
    basestring = str

from pyspark.rdd import RDD, ignore_unicode_prefix
from pyspark.mllib.common import callMLlibFunc, JavaModelWrapper
from pyspark.mllib.linalg import Matrix, _convert_to_vector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat.test import ChiSqTestResult, KolmogorovSmirnovTestResult


__all__ = ['MultivariateStatisticalSummary', 'Statistics']


class MultivariateStatisticalSummary(JavaModelWrapper):

    """
    Trait for multivariate statistical summary of a data matrix.
    """

    def mean(self):
        return self.call("mean").toArray()

    def variance(self):
        return self.call("variance").toArray()

    def count(self):
        return int(self.call("count"))

    def numNonzeros(self):
        return self.call("numNonzeros").toArray()

    def max(self):
        return self.call("max").toArray()

    def min(self):
        return self.call("min").toArray()

    def normL1(self):
        return self.call("normL1").toArray()

    def normL2(self):
        return self.call("normL2").toArray()


class Statistics(object):

    @staticmethod
    def colStats(rdd):
        """
        Computes column-wise summary statistics for the input RDD[Vector].

        :param rdd: an RDD[Vector] for which column-wise summary statistics
                    are to be computed.
        :return: :class:`MultivariateStatisticalSummary` object containing
                 column-wise summary statistics.

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
        3
        >>> cStats.numNonzeros()
        array([ 3.,  2.,  0.,  3.])
        >>> cStats.max()
        array([ 6.,  7.,  0.,  8.])
        >>> cStats.min()
        array([ 2.,  0.,  0., -2.])
        """
        cStats = callMLlibFunc("colStats", rdd.map(_convert_to_vector))
        return MultivariateStatisticalSummary(cStats)

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

        :param x: an RDD of vector for which the correlation matrix is to be computed,
                  or an RDD of float of the same cardinality as y when y is specified.
        :param y: an RDD of float of the same cardinality as x.
        :param method: String specifying the method to use for computing correlation.
                       Supported: `pearson` (default), `spearman`
        :return: Correlation matrix comparing columns in x.

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
        >>> print(str(pearsonCorr).replace('nan', 'NaN'))
        [[ 1.          0.05564149         NaN  0.40047142]
         [ 0.05564149  1.                 NaN  0.91359586]
         [        NaN         NaN  1.                 NaN]
         [ 0.40047142  0.91359586         NaN  1.        ]]
        >>> spearmanCorr = Statistics.corr(rdd, method="spearman")
        >>> print(str(spearmanCorr).replace('nan', 'NaN'))
        [[ 1.          0.10540926         NaN  0.4       ]
         [ 0.10540926  1.                 NaN  0.9486833 ]
         [        NaN         NaN  1.                 NaN]
         [ 0.4         0.9486833          NaN  1.        ]]
        >>> try:
        ...     Statistics.corr(rdd, "spearman")
        ...     print("Method name as second argument without 'method=' shouldn't be allowed.")
        ... except TypeError:
        ...     pass
        """
        # Check inputs to determine whether a single value or a matrix is needed for output.
        # Since it's legal for users to use the method name as the second argument, we need to
        # check if y is used to specify the method name instead.
        if type(y) == str:
            raise TypeError("Use 'method=' to specify method name.")

        if not y:
            return callMLlibFunc("corr", x.map(_convert_to_vector), method).toArray()
        else:
            return callMLlibFunc("corr", x.map(float), y.map(float), method)

    @staticmethod
    @ignore_unicode_prefix
    def chiSqTest(observed, expected=None):
        """
        If `observed` is Vector, conduct Pearson's chi-squared goodness
        of fit test of the observed data against the expected distribution,
        or againt the uniform distribution (by default), with each category
        having an expected frequency of `1 / len(observed)`.

        If `observed` is matrix, conduct Pearson's independence test on the
        input contingency matrix, which cannot contain negative entries or
        columns or rows that sum up to 0.

        If `observed` is an RDD of LabeledPoint, conduct Pearson's independence
        test for every feature against the label across the input RDD.
        For each feature, the (feature, label) pairs are converted into a
        contingency matrix for which the chi-squared statistic is computed.
        All label and feature values must be categorical.

        .. note:: `observed` cannot contain negative values

        :param observed: it could be a vector containing the observed categorical
                         counts/relative frequencies, or the contingency matrix
                         (containing either counts or relative frequencies),
                         or an RDD of LabeledPoint containing the labeled dataset
                         with categorical features. Real-valued features will be
                         treated as categorical for each distinct value.
        :param expected: Vector containing the expected categorical counts/relative
                         frequencies. `expected` is rescaled if the `expected` sum
                         differs from the `observed` sum.
        :return: ChiSquaredTest object containing the test statistic, degrees
                 of freedom, p-value, the method used, and the null hypothesis.

        >>> from pyspark.mllib.linalg import Vectors, Matrices
        >>> observed = Vectors.dense([4, 6, 5])
        >>> pearson = Statistics.chiSqTest(observed)
        >>> print(pearson.statistic)
        0.4
        >>> pearson.degreesOfFreedom
        2
        >>> print(round(pearson.pValue, 4))
        0.8187
        >>> pearson.method
        u'pearson'
        >>> pearson.nullHypothesis
        u'observed follows the same distribution as expected.'

        >>> observed = Vectors.dense([21, 38, 43, 80])
        >>> expected = Vectors.dense([3, 5, 7, 20])
        >>> pearson = Statistics.chiSqTest(observed, expected)
        >>> print(round(pearson.pValue, 4))
        0.0027

        >>> data = [40.0, 24.0, 29.0, 56.0, 32.0, 42.0, 31.0, 10.0, 0.0, 30.0, 15.0, 12.0]
        >>> chi = Statistics.chiSqTest(Matrices.dense(3, 4, data))
        >>> print(round(chi.statistic, 4))
        21.9958

        >>> data = [LabeledPoint(0.0, Vectors.dense([0.5, 10.0])),
        ...         LabeledPoint(0.0, Vectors.dense([1.5, 20.0])),
        ...         LabeledPoint(1.0, Vectors.dense([1.5, 30.0])),
        ...         LabeledPoint(0.0, Vectors.dense([3.5, 30.0])),
        ...         LabeledPoint(0.0, Vectors.dense([3.5, 40.0])),
        ...         LabeledPoint(1.0, Vectors.dense([3.5, 40.0])),]
        >>> rdd = sc.parallelize(data, 4)
        >>> chi = Statistics.chiSqTest(rdd)
        >>> print(chi[0].statistic)
        0.75
        >>> print(chi[1].statistic)
        1.5
        """
        if isinstance(observed, RDD):
            if not isinstance(observed.first(), LabeledPoint):
                raise ValueError("observed should be an RDD of LabeledPoint")
            jmodels = callMLlibFunc("chiSqTest", observed)
            return [ChiSqTestResult(m) for m in jmodels]

        if isinstance(observed, Matrix):
            jmodel = callMLlibFunc("chiSqTest", observed)
        else:
            if expected and len(expected) != len(observed):
                raise ValueError("`expected` should have same length with `observed`")
            jmodel = callMLlibFunc("chiSqTest", _convert_to_vector(observed), expected)
        return ChiSqTestResult(jmodel)

    @staticmethod
    @ignore_unicode_prefix
    def kolmogorovSmirnovTest(data, distName="norm", *params):
        """
        Performs the Kolmogorov-Smirnov (KS) test for data sampled from
        a continuous distribution. It tests the null hypothesis that
        the data is generated from a particular distribution.

        The given data is sorted and the Empirical Cumulative
        Distribution Function (ECDF) is calculated
        which for a given point is the number of points having a CDF
        value lesser than it divided by the total number of points.

        Since the data is sorted, this is a step function
        that rises by (1 / length of data) for every ordered point.

        The KS statistic gives us the maximum distance between the
        ECDF and the CDF. Intuitively if this statistic is large, the
        probabilty that the null hypothesis is true becomes small.
        For specific details of the implementation, please have a look
        at the Scala documentation.

        :param data: RDD, samples from the data
        :param distName: string, currently only "norm" is supported.
                         (Normal distribution) to calculate the
                         theoretical distribution of the data.
        :param params: additional values which need to be provided for
                       a certain distribution.
                       If not provided, the default values are used.
        :return: KolmogorovSmirnovTestResult object containing the test
                 statistic, degrees of freedom, p-value,
                 the method used, and the null hypothesis.

        >>> kstest = Statistics.kolmogorovSmirnovTest
        >>> data = sc.parallelize([-1.0, 0.0, 1.0])
        >>> ksmodel = kstest(data, "norm")
        >>> print(round(ksmodel.pValue, 3))
        1.0
        >>> print(round(ksmodel.statistic, 3))
        0.175
        >>> ksmodel.nullHypothesis
        u'Sample follows theoretical distribution'

        >>> data = sc.parallelize([2.0, 3.0, 4.0])
        >>> ksmodel = kstest(data, "norm", 3.0, 1.0)
        >>> print(round(ksmodel.pValue, 3))
        1.0
        >>> print(round(ksmodel.statistic, 3))
        0.175
        """
        if not isinstance(data, RDD):
            raise TypeError("data should be an RDD, got %s." % type(data))
        if not isinstance(distName, basestring):
            raise TypeError("distName should be a string, got %s." % type(distName))

        params = [float(param) for param in params]
        return KolmogorovSmirnovTestResult(
            callMLlibFunc("kolmogorovSmirnovTest", data, distName, params))


def _test():
    import doctest
    from pyspark.sql import SparkSession
    globs = globals().copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("mllib.stat.statistics tests")\
        .getOrCreate()
    globs['sc'] = spark.sparkContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
