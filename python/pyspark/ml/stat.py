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

from typing import Optional, Tuple, TYPE_CHECKING


from pyspark import since, SparkContext
from pyspark.ml.common import _java2py, _py2java
from pyspark.ml.linalg import Matrix, Vector
from pyspark.ml.wrapper import JavaWrapper, _jvm
from pyspark.sql.column import Column, _to_seq
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject


class ChiSquareTest:
    """
    Conduct Pearson's independence test for every feature against the label. For each feature,
    the (feature, label) pairs are converted into a contingency matrix for which the Chi-squared
    statistic is computed. All label and feature values must be categorical.

    The null hypothesis is that the occurrence of the outcomes is statistically independent.

    .. versionadded:: 2.2.0

    """

    @staticmethod
    def test(
        dataset: DataFrame, featuresCol: str, labelCol: str, flatten: bool = False
    ) -> DataFrame:
        """
        Perform a Pearson's independence test using dataset.

        .. versionadded:: 2.2.0
        .. versionchanged:: 3.1.0
           Added optional ``flatten`` argument.

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            DataFrame of categorical labels and categorical features.
            Real-valued features will be treated as categorical for each distinct value.
        featuresCol : str
            Name of features column in dataset, of type `Vector` (`VectorUDT`).
        labelCol : str
            Name of label column in dataset, of any numerical type.
        flatten : bool, optional
            if True, flattens the returned dataframe.

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            DataFrame containing the test result for every feature against the label.
            If flatten is True, this DataFrame will contain one row per feature with the following
            fields:

            - `featureIndex: int`
            - `pValue: float`
            - `degreesOfFreedom: int`
            - `statistic: float`

            If flatten is False, this DataFrame will contain a single Row with the following fields:

            - `pValues: Vector`
            - `degreesOfFreedom: Array[int]`
            - `statistics: Vector`

            Each of these fields has one value per feature.

        Examples
        --------
        >>> from pyspark.ml.linalg import Vectors
        >>> from pyspark.ml.stat import ChiSquareTest
        >>> dataset = [[0, Vectors.dense([0, 0, 1])],
        ...            [0, Vectors.dense([1, 0, 1])],
        ...            [1, Vectors.dense([2, 1, 1])],
        ...            [1, Vectors.dense([3, 1, 1])]]
        >>> dataset = spark.createDataFrame(dataset, ["label", "features"])
        >>> chiSqResult = ChiSquareTest.test(dataset, 'features', 'label')
        >>> chiSqResult.select("degreesOfFreedom").collect()[0]
        Row(degreesOfFreedom=[3, 1, 0])
        >>> chiSqResult = ChiSquareTest.test(dataset, 'features', 'label', True)
        >>> row = chiSqResult.orderBy("featureIndex").collect()
        >>> row[0].statistic
        4.0
        """
        sc = SparkContext._active_spark_context
        assert sc is not None

        javaTestObj = _jvm().org.apache.spark.ml.stat.ChiSquareTest
        args = [_py2java(sc, arg) for arg in (dataset, featuresCol, labelCol, flatten)]
        return _java2py(sc, javaTestObj.test(*args))


class Correlation:
    """
    Compute the correlation matrix for the input dataset of Vectors using the specified method.
    Methods currently supported: `pearson` (default), `spearman`.

    .. versionadded:: 2.2.0

    Notes
    -----
    For Spearman, a rank correlation, we need to create an RDD[Double] for each column
    and sort it in order to retrieve the ranks and then join the columns back into an RDD[Vector],
    which is fairly costly. Cache the input Dataset before calling corr with `method = 'spearman'`
    to avoid recomputing the common lineage.
    """

    @staticmethod
    def corr(dataset: DataFrame, column: str, method: str = "pearson") -> DataFrame:
        """
        Compute the correlation matrix with specified method using dataset.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            A DataFrame.
        column : str
            The name of the column of vectors for which the correlation coefficient needs
            to be computed. This must be a column of the dataset, and it must contain
            Vector objects.
        method : str, optional
            String specifying the method to use for computing correlation.
            Supported: `pearson` (default), `spearman`.

        Returns
        -------
        A DataFrame that contains the correlation matrix of the column of vectors. This
        DataFrame contains a single row and a single column of name `METHODNAME(COLUMN)`.

        Examples
        --------
        >>> from pyspark.ml.linalg import DenseMatrix, Vectors
        >>> from pyspark.ml.stat import Correlation
        >>> dataset = [[Vectors.dense([1, 0, 0, -2])],
        ...            [Vectors.dense([4, 5, 0, 3])],
        ...            [Vectors.dense([6, 7, 0, 8])],
        ...            [Vectors.dense([9, 0, 0, 1])]]
        >>> dataset = spark.createDataFrame(dataset, ['features'])
        >>> pearsonCorr = Correlation.corr(dataset, 'features', 'pearson').collect()[0][0]
        >>> print(str(pearsonCorr).replace('nan', 'NaN'))
        DenseMatrix([[ 1.        ,  0.0556...,         NaN,  0.4004...],
                     [ 0.0556...,  1.        ,         NaN,  0.9135...],
                     [        NaN,         NaN,  1.        ,         NaN],
                     [ 0.4004...,  0.9135...,         NaN,  1.        ]])
        >>> spearmanCorr = Correlation.corr(dataset, 'features', method='spearman').collect()[0][0]
        >>> print(str(spearmanCorr).replace('nan', 'NaN'))
        DenseMatrix([[ 1.        ,  0.1054...,         NaN,  0.4       ],
                     [ 0.1054...,  1.        ,         NaN,  0.9486... ],
                     [        NaN,         NaN,  1.        ,         NaN],
                     [ 0.4       ,  0.9486... ,         NaN,  1.        ]])
        """
        sc = SparkContext._active_spark_context
        assert sc is not None

        javaCorrObj = _jvm().org.apache.spark.ml.stat.Correlation
        args = [_py2java(sc, arg) for arg in (dataset, column, method)]
        return _java2py(sc, javaCorrObj.corr(*args))


class KolmogorovSmirnovTest:
    """
    Conduct the two-sided Kolmogorov Smirnov (KS) test for data sampled from a continuous
    distribution.

    By comparing the largest difference between the empirical cumulative
    distribution of the sample data and the theoretical distribution we can provide a test for the
    the null hypothesis that the sample data comes from that theoretical distribution.

    .. versionadded:: 2.4.0

    """

    @staticmethod
    def test(dataset: DataFrame, sampleCol: str, distName: str, *params: float) -> DataFrame:
        """
        Conduct a one-sample, two-sided Kolmogorov-Smirnov test for probability distribution
        equality. Currently supports the normal distribution, taking as parameters the mean and
        standard deviation.

        .. versionadded:: 2.4.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            a Dataset or a DataFrame containing the sample of data to test.
        sampleCol : str
            Name of sample column in dataset, of any numerical type.
        distName : str
            a `string` name for a theoretical distribution, currently only support "norm".
        params : float
            a list of `float` values specifying the parameters to be used for the theoretical
            distribution. For "norm" distribution, the parameters includes mean and variance.

        Returns
        -------
        A DataFrame that contains the Kolmogorov-Smirnov test result for the input sampled data.
        This DataFrame will contain a single Row with the following fields:

        - `pValue: Double`
        - `statistic: Double`

        Examples
        --------
        >>> from pyspark.ml.stat import KolmogorovSmirnovTest
        >>> dataset = [[-1.0], [0.0], [1.0]]
        >>> dataset = spark.createDataFrame(dataset, ['sample'])
        >>> ksResult = KolmogorovSmirnovTest.test(dataset, 'sample', 'norm', 0.0, 1.0).first()
        >>> round(ksResult.pValue, 3)
        1.0
        >>> round(ksResult.statistic, 3)
        0.175
        >>> dataset = [[2.0], [3.0], [4.0]]
        >>> dataset = spark.createDataFrame(dataset, ['sample'])
        >>> ksResult = KolmogorovSmirnovTest.test(dataset, 'sample', 'norm', 3.0, 1.0).first()
        >>> round(ksResult.pValue, 3)
        1.0
        >>> round(ksResult.statistic, 3)
        0.175
        """
        sc = SparkContext._active_spark_context
        assert sc is not None

        javaTestObj = _jvm().org.apache.spark.ml.stat.KolmogorovSmirnovTest
        dataset = _py2java(sc, dataset)
        params = [float(param) for param in params]  # type: ignore[assignment]
        return _java2py(
            sc, javaTestObj.test(dataset, sampleCol, distName, _jvm().PythonUtils.toSeq(params))
        )


class Summarizer:
    """
    Tools for vectorized statistics on MLlib Vectors.
    The methods in this package provide various statistics for Vectors contained inside DataFrames.
    This class lets users pick the statistics they would like to extract for a given column.

    .. versionadded:: 2.4.0

    Examples
    --------
    >>> from pyspark.ml.stat import Summarizer
    >>> from pyspark.sql import Row
    >>> from pyspark.ml.linalg import Vectors
    >>> summarizer = Summarizer.metrics("mean", "count")
    >>> df = sc.parallelize([Row(weight=1.0, features=Vectors.dense(1.0, 1.0, 1.0)),
    ...                      Row(weight=0.0, features=Vectors.dense(1.0, 2.0, 3.0))]).toDF()
    >>> df.select(summarizer.summary(df.features, df.weight)).show(truncate=False)
    +-----------------------------------+
    |aggregate_metrics(features, weight)|
    +-----------------------------------+
    |{[1.0,1.0,1.0], 1}                 |
    +-----------------------------------+
    >>> df.select(summarizer.summary(df.features)).show(truncate=False)
    +--------------------------------+
    |aggregate_metrics(features, 1.0)|
    +--------------------------------+
    |{[1.0,1.5,2.0], 2}              |
    +--------------------------------+
    >>> df.select(Summarizer.mean(df.features, df.weight)).show(truncate=False)
    +--------------+
    |mean(features)|
    +--------------+
    |[1.0,1.0,1.0] |
    +--------------+
    >>> df.select(Summarizer.mean(df.features)).show(truncate=False)
    +--------------+
    |mean(features)|
    +--------------+
    |[1.0,1.5,2.0] |
    +--------------+
    """

    @staticmethod
    @since("2.4.0")
    def mean(col: Column, weightCol: Optional[Column] = None) -> Column:
        """
        return a column of mean summary
        """
        return Summarizer._get_single_metric(col, weightCol, "mean")

    @staticmethod
    @since("3.0.0")
    def sum(col: Column, weightCol: Optional[Column] = None) -> Column:
        """
        return a column of sum summary
        """
        return Summarizer._get_single_metric(col, weightCol, "sum")

    @staticmethod
    @since("2.4.0")
    def variance(col: Column, weightCol: Optional[Column] = None) -> Column:
        """
        return a column of variance summary
        """
        return Summarizer._get_single_metric(col, weightCol, "variance")

    @staticmethod
    @since("3.0.0")
    def std(col: Column, weightCol: Optional[Column] = None) -> Column:
        """
        return a column of std summary
        """
        return Summarizer._get_single_metric(col, weightCol, "std")

    @staticmethod
    @since("2.4.0")
    def count(col: Column, weightCol: Optional[Column] = None) -> Column:
        """
        return a column of count summary
        """
        return Summarizer._get_single_metric(col, weightCol, "count")

    @staticmethod
    @since("2.4.0")
    def numNonZeros(col: Column, weightCol: Optional[Column] = None) -> Column:
        """
        return a column of numNonZero summary
        """
        return Summarizer._get_single_metric(col, weightCol, "numNonZeros")

    @staticmethod
    @since("2.4.0")
    def max(col: Column, weightCol: Optional[Column] = None) -> Column:
        """
        return a column of max summary
        """
        return Summarizer._get_single_metric(col, weightCol, "max")

    @staticmethod
    @since("2.4.0")
    def min(col: Column, weightCol: Optional[Column] = None) -> Column:
        """
        return a column of min summary
        """
        return Summarizer._get_single_metric(col, weightCol, "min")

    @staticmethod
    @since("2.4.0")
    def normL1(col: Column, weightCol: Optional[Column] = None) -> Column:
        """
        return a column of normL1 summary
        """
        return Summarizer._get_single_metric(col, weightCol, "normL1")

    @staticmethod
    @since("2.4.0")
    def normL2(col: Column, weightCol: Optional[Column] = None) -> Column:
        """
        return a column of normL2 summary
        """
        return Summarizer._get_single_metric(col, weightCol, "normL2")

    @staticmethod
    def _check_param(featuresCol: Column, weightCol: Optional[Column]) -> Tuple[Column, Column]:
        if weightCol is None:
            weightCol = lit(1.0)
        if not isinstance(featuresCol, Column) or not isinstance(weightCol, Column):
            raise TypeError("featureCol and weightCol should be a Column")
        return featuresCol, weightCol

    @staticmethod
    def _get_single_metric(col: Column, weightCol: Optional[Column], metric: str) -> Column:
        col, weightCol = Summarizer._check_param(col, weightCol)
        return Column(
            JavaWrapper._new_java_obj(
                "org.apache.spark.ml.stat.Summarizer." + metric, col._jc, weightCol._jc
            )
        )

    @staticmethod
    def metrics(*metrics: str) -> "SummaryBuilder":
        """
        Given a list of metrics, provides a builder that it turns computes metrics from a column.

        See the documentation of :py:class:`Summarizer` for an example.

        The following metrics are accepted (case sensitive):
         - mean: a vector that contains the coefficient-wise mean.
         - sum: a vector that contains the coefficient-wise sum.
         - variance: a vector tha contains the coefficient-wise variance.
         - std: a vector tha contains the coefficient-wise standard deviation.
         - count: the count of all vectors seen.
         - numNonzeros: a vector with the number of non-zeros for each coefficients
         - max: the maximum for each coefficient.
         - min: the minimum for each coefficient.
         - normL2: the Euclidean norm for each coefficient.
         - normL1: the L1 norm of each coefficient (sum of the absolute values).

        .. versionadded:: 2.4.0

        Notes
        -----
        Currently, the performance of this interface is about 2x~3x slower than using the RDD
        interface.

        Examples
        --------
        metrics : str
            metrics that can be provided.

        Returns
        -------
        :py:class:`pyspark.ml.stat.SummaryBuilder`
        """
        sc = SparkContext._active_spark_context
        assert sc is not None

        js = JavaWrapper._new_java_obj(
            "org.apache.spark.ml.stat.Summarizer.metrics", _to_seq(sc, metrics)
        )
        return SummaryBuilder(js)


class SummaryBuilder(JavaWrapper):
    """
    A builder object that provides summary statistics about a given column.

    Users should not directly create such builders, but instead use one of the methods in
    :py:class:`pyspark.ml.stat.Summarizer`

    .. versionadded:: 2.4.0

    """

    def __init__(self, jSummaryBuilder: "JavaObject"):
        super(SummaryBuilder, self).__init__(jSummaryBuilder)

    def summary(self, featuresCol: Column, weightCol: Optional[Column] = None) -> Column:
        """
        Returns an aggregate object that contains the summary of the column with the requested
        metrics.

        .. versionadded:: 2.4.0

        Parameters
        ----------
        featuresCol : str
            a column that contains features Vector object.
        weightCol : str, optional
            a column that contains weight value. Default weight is 1.0.

        Returns
        -------
        :py:class:`pyspark.sql.Column`
            an aggregate column that contains the statistics. The exact content of this
            structure is determined during the creation of the builder.
        """
        featuresCol, weightCol = Summarizer._check_param(featuresCol, weightCol)
        assert self._java_obj is not None

        return Column(self._java_obj.summary(featuresCol._jc, weightCol._jc))


class MultivariateGaussian:
    """Represents a (mean, cov) tuple

    .. versionadded:: 3.0.0

    Examples
    --------
    >>> from pyspark.ml.linalg import DenseMatrix, Vectors
    >>> from pyspark.ml.stat import MultivariateGaussian
    >>> m = MultivariateGaussian(Vectors.dense([11,12]), DenseMatrix(2, 2, (1.0, 3.0, 5.0, 2.0)))
    >>> (m.mean, m.cov.toArray())
    (DenseVector([11.0, 12.0]), array([[ 1.,  5.],
           [ 3.,  2.]]))
    """

    def __init__(self, mean: Vector, cov: Matrix):
        self.mean = mean
        self.cov = cov


if __name__ == "__main__":
    import doctest
    import numpy
    import pyspark.ml.stat
    from pyspark.sql import SparkSession

    try:
        # Numpy 1.14+ changed it's string format.
        numpy.set_printoptions(legacy="1.13")
    except TypeError:
        pass

    globs = pyspark.ml.stat.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder.master("local[2]").appName("ml.stat tests").getOrCreate()
    sc = spark.sparkContext
    globs["sc"] = sc
    globs["spark"] = spark

    failure_count, test_count = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)
