from pyspark import since, SparkContext
from pyspark.ml.common import _java2py, _py2java
from pyspark.ml.wrapper import _jvm


class ChiSquareTest(object):
    """ Conduct Pearson's independence test for every feature against the label. For each feature,
    the (feature, label) pairs are converted into a contingency matrix for which the Chi-squared
    statistic is computed. All label and feature values must be categorical.

    The null hypothesis is that the occurrence of the outcomes is statistically independent.

    :param dataset:
      Dataset for running Chi square test. Instance of `pyspark.sql.DataFrame` with categorical
      feature and label columns.
    :param featuresCol:
      Name of features column in dataset, type `str`. Column should be of type `Vector`
      (`VectorUDT`).
    :param labelCol:
      Name of label column in dataset, type str. Column should have any numerical type.
    :return:
      DataFrame, with a single row, containing the test result for each feature against the
      label. The DataFrame will contain the following fields:
        `pValues: Vector`
        `degreesOfFreedom: Array[Int]`
        `statistics: Vector`
      Each of these fields has one value per feature.

    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.stat import ChiSquareTest
    >>> v0 = Vectors.dense([0, 0, 1])
    >>> v1 = Vectors.dense([1, 0, 1])
    >>> v2 = Vectors.dense([2, 1, 1])
    >>> v3 = Vectors.dense([3, 1, 1])
    >>> labels = [0, 0, 1, 1]
    >>> dataset = spark.createDataFrame(zip(labels, [v0, v1, v2, v3]),
    ...                                 ["label", "features"])
    >>> chiSqResult = ChiSquareTest.test(dataset, 'features', 'label')
    >>> chiSqResult.select("degreesOfFreedom").collect()[0]
    Row(degreesOfFreedom=[3, 1, 0])

    .. versionadded:: 2.2.0

    """

    @staticmethod
    @since("2.2.0")
    def test(dataset, featuresCol, labelCol):
        """ Perform a Pearson's independence test using dataset.
        """
        sc = SparkContext._active_spark_context
        javaTestObj = _jvm().org.apache.spark.ml.stat.ChiSquareTest
        args = [_py2java(sc, arg) for arg in (dataset, featuresCol, labelCol)]
        return _java2py(sc, javaTestObj.test(*args))


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
        exit(-1)
