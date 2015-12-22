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

from pyspark import since
from pyspark.mllib.common import JavaModelWrapper, callMLlibFunc
from pyspark.sql import SQLContext
from pyspark.sql.types import StructField, StructType, DoubleType, IntegerType, ArrayType

__all__ = ['BinaryClassificationMetrics', 'RegressionMetrics',
           'MulticlassMetrics', 'RankingMetrics']


class BinaryClassificationMetrics(JavaModelWrapper):
    """
    Evaluator for binary classification.

    :param scoreAndLabels: an RDD of (score, label) pairs

    >>> scoreAndLabels = sc.parallelize([
    ...     (0.1, 0.0), (0.1, 1.0), (0.4, 0.0), (0.6, 0.0), (0.6, 1.0), (0.6, 1.0), (0.8, 1.0)], 2)
    >>> metrics = BinaryClassificationMetrics(scoreAndLabels)
    >>> metrics.areaUnderROC
    0.70...
    >>> metrics.areaUnderPR
    0.83...
    >>> metrics.unpersist()

    .. versionadded:: 1.4.0
    """

    def __init__(self, scoreAndLabels):
        sc = scoreAndLabels.ctx
        sql_ctx = SQLContext.getOrCreate(sc)
        df = sql_ctx.createDataFrame(scoreAndLabels, schema=StructType([
            StructField("score", DoubleType(), nullable=False),
            StructField("label", DoubleType(), nullable=False)]))
        java_class = sc._jvm.org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
        java_model = java_class(df._jdf)
        super(BinaryClassificationMetrics, self).__init__(java_model)

    @property
    @since('1.4.0')
    def areaUnderROC(self):
        """
        Computes the area under the receiver operating characteristic
        (ROC) curve.
        """
        return self.call("areaUnderROC")

    @property
    @since('1.4.0')
    def areaUnderPR(self):
        """
        Computes the area under the precision-recall curve.
        """
        return self.call("areaUnderPR")

    @since('1.4.0')
    def unpersist(self):
        """
        Unpersists intermediate RDDs used in the computation.
        """
        self.call("unpersist")


class RegressionMetrics(JavaModelWrapper):
    """
    Evaluator for regression.

    :param predictionAndObservations: an RDD of (prediction,
                                      observation) pairs.

    >>> predictionAndObservations = sc.parallelize([
    ...     (2.5, 3.0), (0.0, -0.5), (2.0, 2.0), (8.0, 7.0)])
    >>> metrics = RegressionMetrics(predictionAndObservations)
    >>> metrics.explainedVariance
    8.859...
    >>> metrics.meanAbsoluteError
    0.5...
    >>> metrics.meanSquaredError
    0.37...
    >>> metrics.rootMeanSquaredError
    0.61...
    >>> metrics.r2
    0.94...

    .. versionadded:: 1.4.0
    """

    def __init__(self, predictionAndObservations):
        sc = predictionAndObservations.ctx
        sql_ctx = SQLContext.getOrCreate(sc)
        df = sql_ctx.createDataFrame(predictionAndObservations, schema=StructType([
            StructField("prediction", DoubleType(), nullable=False),
            StructField("observation", DoubleType(), nullable=False)]))
        java_class = sc._jvm.org.apache.spark.mllib.evaluation.RegressionMetrics
        java_model = java_class(df._jdf)
        super(RegressionMetrics, self).__init__(java_model)

    @property
    @since('1.4.0')
    def explainedVariance(self):
        """
        Returns the explained variance regression score.
        explainedVariance = 1 - variance(y - \hat{y}) / variance(y)
        """
        return self.call("explainedVariance")

    @property
    @since('1.4.0')
    def meanAbsoluteError(self):
        """
        Returns the mean absolute error, which is a risk function corresponding to the
        expected value of the absolute error loss or l1-norm loss.
        """
        return self.call("meanAbsoluteError")

    @property
    @since('1.4.0')
    def meanSquaredError(self):
        """
        Returns the mean squared error, which is a risk function corresponding to the
        expected value of the squared error loss or quadratic loss.
        """
        return self.call("meanSquaredError")

    @property
    @since('1.4.0')
    def rootMeanSquaredError(self):
        """
        Returns the root mean squared error, which is defined as the square root of
        the mean squared error.
        """
        return self.call("rootMeanSquaredError")

    @property
    @since('1.4.0')
    def r2(self):
        """
        Returns R^2^, the coefficient of determination.
        """
        return self.call("r2")


class MulticlassMetrics(JavaModelWrapper):
    """
    Evaluator for multiclass classification.

    :param predictionAndLabels: an RDD of (prediction, label) pairs.

    >>> predictionAndLabels = sc.parallelize([(0.0, 0.0), (0.0, 1.0), (0.0, 0.0),
    ...     (1.0, 0.0), (1.0, 1.0), (1.0, 1.0), (1.0, 1.0), (2.0, 2.0), (2.0, 0.0)])
    >>> metrics = MulticlassMetrics(predictionAndLabels)
    >>> metrics.confusionMatrix().toArray()
    array([[ 2.,  1.,  1.],
           [ 1.,  3.,  0.],
           [ 0.,  0.,  1.]])
    >>> metrics.falsePositiveRate(0.0)
    0.2...
    >>> metrics.precision(1.0)
    0.75...
    >>> metrics.recall(2.0)
    1.0...
    >>> metrics.fMeasure(0.0, 2.0)
    0.52...
    >>> metrics.precision()
    0.66...
    >>> metrics.recall()
    0.66...
    >>> metrics.weightedFalsePositiveRate
    0.19...
    >>> metrics.weightedPrecision
    0.68...
    >>> metrics.weightedRecall
    0.66...
    >>> metrics.weightedFMeasure()
    0.66...
    >>> metrics.weightedFMeasure(2.0)
    0.65...

    .. versionadded:: 1.4.0
    """

    def __init__(self, predictionAndLabels):
        sc = predictionAndLabels.ctx
        sql_ctx = SQLContext.getOrCreate(sc)
        df = sql_ctx.createDataFrame(predictionAndLabels, schema=StructType([
            StructField("prediction", DoubleType(), nullable=False),
            StructField("label", DoubleType(), nullable=False)]))
        java_class = sc._jvm.org.apache.spark.mllib.evaluation.MulticlassMetrics
        java_model = java_class(df._jdf)
        super(MulticlassMetrics, self).__init__(java_model)

    @since('1.4.0')
    def confusionMatrix(self):
        """
        Returns confusion matrix: predicted classes are in columns,
        they are ordered by class label ascending, as in "labels".
        """
        return self.call("confusionMatrix")

    @since('1.4.0')
    def truePositiveRate(self, label):
        """
        Returns true positive rate for a given label (category).
        """
        return self.call("truePositiveRate", label)

    @since('1.4.0')
    def falsePositiveRate(self, label):
        """
        Returns false positive rate for a given label (category).
        """
        return self.call("falsePositiveRate", label)

    @since('1.4.0')
    def precision(self, label=None):
        """
        Returns precision or precision for a given label (category) if specified.
        """
        if label is None:
            return self.call("precision")
        else:
            return self.call("precision", float(label))

    @since('1.4.0')
    def recall(self, label=None):
        """
        Returns recall or recall for a given label (category) if specified.
        """
        if label is None:
            return self.call("recall")
        else:
            return self.call("recall", float(label))

    @since('1.4.0')
    def fMeasure(self, label=None, beta=None):
        """
        Returns f-measure or f-measure for a given label (category) if specified.
        """
        if beta is None:
            if label is None:
                return self.call("fMeasure")
            else:
                return self.call("fMeasure", label)
        else:
            if label is None:
                raise Exception("If the beta parameter is specified, label can not be none")
            else:
                return self.call("fMeasure", label, beta)

    @property
    @since('1.4.0')
    def weightedTruePositiveRate(self):
        """
        Returns weighted true positive rate.
        (equals to precision, recall and f-measure)
        """
        return self.call("weightedTruePositiveRate")

    @property
    @since('1.4.0')
    def weightedFalsePositiveRate(self):
        """
        Returns weighted false positive rate.
        """
        return self.call("weightedFalsePositiveRate")

    @property
    @since('1.4.0')
    def weightedRecall(self):
        """
        Returns weighted averaged recall.
        (equals to precision, recall and f-measure)
        """
        return self.call("weightedRecall")

    @property
    @since('1.4.0')
    def weightedPrecision(self):
        """
        Returns weighted averaged precision.
        """
        return self.call("weightedPrecision")

    @since('1.4.0')
    def weightedFMeasure(self, beta=None):
        """
        Returns weighted averaged f-measure.
        """
        if beta is None:
            return self.call("weightedFMeasure")
        else:
            return self.call("weightedFMeasure", beta)


class RankingMetrics(JavaModelWrapper):
    """
    Evaluator for ranking algorithms.

    :param predictionAndLabels: an RDD of (predicted ranking,
                                ground truth set) pairs.

    >>> predictionAndLabels = sc.parallelize([
    ...     ([1, 6, 2, 7, 8, 3, 9, 10, 4, 5], [1, 2, 3, 4, 5]),
    ...     ([4, 1, 5, 6, 2, 7, 3, 8, 9, 10], [1, 2, 3]),
    ...     ([1, 2, 3, 4, 5], [])])
    >>> metrics = RankingMetrics(predictionAndLabels)
    >>> metrics.precisionAt(1)
    0.33...
    >>> metrics.precisionAt(5)
    0.26...
    >>> metrics.precisionAt(15)
    0.17...
    >>> metrics.meanAveragePrecision
    0.35...
    >>> metrics.ndcgAt(3)
    0.33...
    >>> metrics.ndcgAt(10)
    0.48...

    .. versionadded:: 1.4.0
    """

    def __init__(self, predictionAndLabels):
        sc = predictionAndLabels.ctx
        sql_ctx = SQLContext.getOrCreate(sc)
        df = sql_ctx.createDataFrame(predictionAndLabels,
                                     schema=sql_ctx._inferSchema(predictionAndLabels))
        java_model = callMLlibFunc("newRankingMetrics", df._jdf)
        super(RankingMetrics, self).__init__(java_model)

    @since('1.4.0')
    def precisionAt(self, k):
        """
        Compute the average precision of all the queries, truncated at ranking position k.

        If for a query, the ranking algorithm returns n (n < k) results, the precision value
        will be computed as #(relevant items retrieved) / k. This formula also applies when
        the size of the ground truth set is less than k.

        If a query has an empty ground truth set, zero will be used as precision together
        with a log warning.
        """
        return self.call("precisionAt", int(k))

    @property
    @since('1.4.0')
    def meanAveragePrecision(self):
        """
        Returns the mean average precision (MAP) of all the queries.
        If a query has an empty ground truth set, the average precision will be zero and
        a log warining is generated.
        """
        return self.call("meanAveragePrecision")

    @since('1.4.0')
    def ndcgAt(self, k):
        """
        Compute the average NDCG value of all the queries, truncated at ranking position k.
        The discounted cumulative gain at position k is computed as:
        sum,,i=1,,^k^ (2^{relevance of ''i''th item}^ - 1) / log(i + 1),
        and the NDCG is obtained by dividing the DCG value on the ground truth set.
        In the current implementation, the relevance value is binary.
        If a query has an empty ground truth set, zero will be used as NDCG together with
        a log warning.
        """
        return self.call("ndcgAt", int(k))


class MultilabelMetrics(JavaModelWrapper):
    """
    Evaluator for multilabel classification.

    :param predictionAndLabels: an RDD of (predictions, labels) pairs,
                                both are non-null Arrays, each with
                                unique elements.

    >>> predictionAndLabels = sc.parallelize([([0.0, 1.0], [0.0, 2.0]), ([0.0, 2.0], [0.0, 1.0]),
    ...     ([], [0.0]), ([2.0], [2.0]), ([2.0, 0.0], [2.0, 0.0]),
    ...     ([0.0, 1.0, 2.0], [0.0, 1.0]), ([1.0], [1.0, 2.0])])
    >>> metrics = MultilabelMetrics(predictionAndLabels)
    >>> metrics.precision(0.0)
    1.0
    >>> metrics.recall(1.0)
    0.66...
    >>> metrics.f1Measure(2.0)
    0.5
    >>> metrics.precision()
    0.66...
    >>> metrics.recall()
    0.64...
    >>> metrics.f1Measure()
    0.63...
    >>> metrics.microPrecision
    0.72...
    >>> metrics.microRecall
    0.66...
    >>> metrics.microF1Measure
    0.69...
    >>> metrics.hammingLoss
    0.33...
    >>> metrics.subsetAccuracy
    0.28...
    >>> metrics.accuracy
    0.54...

    .. versionadded:: 1.4.0
    """

    def __init__(self, predictionAndLabels):
        sc = predictionAndLabels.ctx
        sql_ctx = SQLContext.getOrCreate(sc)
        df = sql_ctx.createDataFrame(predictionAndLabels,
                                     schema=sql_ctx._inferSchema(predictionAndLabels))
        java_class = sc._jvm.org.apache.spark.mllib.evaluation.MultilabelMetrics
        java_model = java_class(df._jdf)
        super(MultilabelMetrics, self).__init__(java_model)

    @since('1.4.0')
    def precision(self, label=None):
        """
        Returns precision or precision for a given label (category) if specified.
        """
        if label is None:
            return self.call("precision")
        else:
            return self.call("precision", float(label))

    @since('1.4.0')
    def recall(self, label=None):
        """
        Returns recall or recall for a given label (category) if specified.
        """
        if label is None:
            return self.call("recall")
        else:
            return self.call("recall", float(label))

    @since('1.4.0')
    def f1Measure(self, label=None):
        """
        Returns f1Measure or f1Measure for a given label (category) if specified.
        """
        if label is None:
            return self.call("f1Measure")
        else:
            return self.call("f1Measure", float(label))

    @property
    @since('1.4.0')
    def microPrecision(self):
        """
        Returns micro-averaged label-based precision.
        (equals to micro-averaged document-based precision)
        """
        return self.call("microPrecision")

    @property
    @since('1.4.0')
    def microRecall(self):
        """
        Returns micro-averaged label-based recall.
        (equals to micro-averaged document-based recall)
        """
        return self.call("microRecall")

    @property
    @since('1.4.0')
    def microF1Measure(self):
        """
        Returns micro-averaged label-based f1-measure.
        (equals to micro-averaged document-based f1-measure)
        """
        return self.call("microF1Measure")

    @property
    @since('1.4.0')
    def hammingLoss(self):
        """
        Returns Hamming-loss.
        """
        return self.call("hammingLoss")

    @property
    @since('1.4.0')
    def subsetAccuracy(self):
        """
        Returns subset accuracy.
        (for equal sets of labels)
        """
        return self.call("subsetAccuracy")

    @property
    @since('1.4.0')
    def accuracy(self):
        """
        Returns accuracy.
        """
        return self.call("accuracy")


def _test():
    import doctest
    from pyspark import SparkContext
    import pyspark.mllib.evaluation
    globs = pyspark.mllib.evaluation.__dict__.copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest')
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
