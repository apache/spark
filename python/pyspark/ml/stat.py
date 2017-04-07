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

from pyspark import since, SparkContext
from pyspark.ml.common import _java2py, _py2java
from pyspark.ml.wrapper import _jvm


class ChiSquareTest(object):
    """
    .. note:: Experimental

    Conduct Pearson's independence test for every feature against the label. For each feature,
    the (feature, label) pairs are converted into a contingency matrix for which the Chi-squared
    statistic is computed. All label and feature values must be categorical.

    The null hypothesis is that the occurrence of the outcomes is statistically independent.

    :param dataset:
      DataFrame of categorical labels and categorical features.
      Real-valued features will be treated as categorical for each distinct value.
    :param featuresCol:
      Name of features column in dataset, of type `Vector` (`VectorUDT`).
    :param labelCol:
      Name of label column in dataset, of any numerical type.
    :return:
      DataFrame containing the test result for every feature against the label.
      This DataFrame will contain a single Row with the following fields:
      - `pValues: Vector`
      - `degreesOfFreedom: Array[Int]`
      - `statistics: Vector`
      Each of these fields has one value per feature.

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

    .. versionadded:: 2.2.0

    """
    @staticmethod
    @since("2.2.0")
    def test(dataset, featuresCol, labelCol):
        """
        Perform a Pearson's independence test using dataset.
        """
        sc = SparkContext._active_spark_context
        javaTestObj = _jvm().org.apache.spark.ml.stat.ChiSquareTest
        args = [_py2java(sc, arg) for arg in (dataset, featuresCol, labelCol)]
        return _java2py(sc, javaTestObj.test(*args))


class Correlation(object):
    """
    .. note:: Experimental

    Compute the correlation matrix for the input dataset of Vectors using the specified method.
    Methods currently supported: `pearson` (default), `spearman`.

    .. note:: For Spearman, a rank correlation, we need to create an RDD[Double] for each column
      and sort it in order to retrieve the ranks and then join the columns back into an RDD[Vector],
      which is fairly costly. Cache the input Dataset before calling corr with `method = 'spearman'`
      to avoid recomputing the common lineage.

    :param dataset:
      A dataset or a dataframe.
    :param column:
      The name of the column of vectors for which the correlation coefficient needs
      to be computed. This must be a column of the dataset, and it must contain
      Vector objects.
    :param method:
      String specifying the method to use for computing correlation.
      Supported: `pearson` (default), `spearman`.
    :return:
      A dataframe that contains the correlation matrix of the column of vectors. This
      dataframe contains a single row and a single column of name
      '$METHODNAME($COLUMN)'.

    >>> from pyspark.ml.linalg import Vectors
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

    .. versionadded:: 2.2.0

    """
    @staticmethod
    @since("2.2.0")
    def corr(dataset, column, method="pearson"):
        """
        Compute the correlation matrix with specified method using dataset.
        """
        sc = SparkContext._active_spark_context
        javaCorrObj = _jvm().org.apache.spark.ml.stat.Correlation
        args = [_py2java(sc, arg) for arg in (dataset, column, method)]
        return _java2py(sc, javaCorrObj.corr(*args))


if __name__ == "__main__":
    import doctest
    import pyspark.ml.stat
    from pyspark.sql import SparkSession

    globs = pyspark.ml.stat.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("ml.stat tests") \
        .getOrCreate()
    sc = spark.sparkContext
    globs['sc'] = sc
    globs['spark'] = spark

    failure_count, test_count = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        exit(-1)
