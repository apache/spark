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
from abc import abstractmethod, ABCMeta

from pyspark import since, keyword_only
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasLabelCol, HasPredictionCol, HasRawPredictionCol, \
    HasFeaturesCol
from pyspark.ml.common import inherit_doc
from pyspark.ml.util import JavaMLReadable, JavaMLWritable

__all__ = ['Evaluator', 'BinaryClassificationEvaluator', 'RegressionEvaluator',
           'MulticlassClassificationEvaluator', 'ClusteringEvaluator']


@inherit_doc
class Evaluator(Params):
    """
    Base class for evaluators that compute metrics from predictions.

    .. versionadded:: 1.4.0
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def _evaluate(self, dataset):
        """
        Evaluates the output.

        :param dataset: a dataset that contains labels/observations and
               predictions
        :return: metric
        """
        raise NotImplementedError()

    @since("1.4.0")
    def evaluate(self, dataset, params=None):
        """
        Evaluates the output with optional parameters.

        :param dataset: a dataset that contains labels/observations and
                        predictions
        :param params: an optional param map that overrides embedded
                       params
        :return: metric
        """
        if params is None:
            params = dict()
        if isinstance(params, dict):
            if params:
                return self.copy(params)._evaluate(dataset)
            else:
                return self._evaluate(dataset)
        else:
            raise ValueError("Params must be a param map but got %s." % type(params))

    @since("1.5.0")
    def isLargerBetter(self):
        """
        Indicates whether the metric returned by :py:meth:`evaluate` should be maximized
        (True, default) or minimized (False).
        A given evaluator may support multiple metrics which may be maximized or minimized.
        """
        return True


@inherit_doc
class JavaEvaluator(JavaParams, Evaluator):
    """
    Base class for :py:class:`Evaluator`s that wrap Java/Scala
    implementations.
    """

    __metaclass__ = ABCMeta

    def _evaluate(self, dataset):
        """
        Evaluates the output.
        :param dataset: a dataset that contains labels/observations and predictions.
        :return: evaluation metric
        """
        self._transfer_params_to_java()
        return self._java_obj.evaluate(dataset._jdf)

    def isLargerBetter(self):
        self._transfer_params_to_java()
        return self._java_obj.isLargerBetter()


@inherit_doc
class BinaryClassificationEvaluator(JavaEvaluator, HasLabelCol, HasRawPredictionCol,
                                    JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Evaluator for binary classification, which expects two input columns: rawPrediction and label.
    The rawPrediction column can be of type double (binary 0/1 prediction, or probability of label
    1) or of type vector (length-2 vector of raw predictions, scores, or label probabilities).

    >>> from pyspark.ml.linalg import Vectors
    >>> scoreAndLabels = map(lambda x: (Vectors.dense([1.0 - x[0], x[0]]), x[1]),
    ...    [(0.1, 0.0), (0.1, 1.0), (0.4, 0.0), (0.6, 0.0), (0.6, 1.0), (0.6, 1.0), (0.8, 1.0)])
    >>> dataset = spark.createDataFrame(scoreAndLabels, ["raw", "label"])
    ...
    >>> evaluator = BinaryClassificationEvaluator(rawPredictionCol="raw")
    >>> evaluator.evaluate(dataset)
    0.70...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "areaUnderPR"})
    0.83...
    >>> bce_path = temp_path + "/bce"
    >>> evaluator.save(bce_path)
    >>> evaluator2 = BinaryClassificationEvaluator.load(bce_path)
    >>> str(evaluator2.getRawPredictionCol())
    'raw'

    .. versionadded:: 1.4.0
    """

    metricName = Param(Params._dummy(), "metricName",
                       "metric name in evaluation (areaUnderROC|areaUnderPR)",
                       typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, rawPredictionCol="rawPrediction", labelCol="label",
                 metricName="areaUnderROC"):
        """
        __init__(self, rawPredictionCol="rawPrediction", labelCol="label", \
                 metricName="areaUnderROC")
        """
        super(BinaryClassificationEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator", self.uid)
        self._setDefault(metricName="areaUnderROC")
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @since("1.4.0")
    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    @since("1.4.0")
    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    @keyword_only
    @since("1.4.0")
    def setParams(self, rawPredictionCol="rawPrediction", labelCol="label",
                  metricName="areaUnderROC"):
        """
        setParams(self, rawPredictionCol="rawPrediction", labelCol="label", \
                  metricName="areaUnderROC")
        Sets params for binary classification evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class RegressionEvaluator(JavaEvaluator, HasLabelCol, HasPredictionCol,
                          JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Evaluator for Regression, which expects two input
    columns: prediction and label.

    >>> scoreAndLabels = [(-28.98343821, -27.0), (20.21491975, 21.5),
    ...   (-25.98418959, -22.0), (30.69731842, 33.0), (74.69283752, 71.0)]
    >>> dataset = spark.createDataFrame(scoreAndLabels, ["raw", "label"])
    ...
    >>> evaluator = RegressionEvaluator(predictionCol="raw")
    >>> evaluator.evaluate(dataset)
    2.842...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "r2"})
    0.993...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "mae"})
    2.649...
    >>> re_path = temp_path + "/re"
    >>> evaluator.save(re_path)
    >>> evaluator2 = RegressionEvaluator.load(re_path)
    >>> str(evaluator2.getPredictionCol())
    'raw'

    .. versionadded:: 1.4.0
    """
    metricName = Param(Params._dummy(), "metricName",
                       """metric name in evaluation - one of:
                       rmse - root mean squared error (default)
                       mse - mean squared error
                       r2 - r^2 metric
                       mae - mean absolute error.""",
                       typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, predictionCol="prediction", labelCol="label",
                 metricName="rmse"):
        """
        __init__(self, predictionCol="prediction", labelCol="label", \
                 metricName="rmse")
        """
        super(RegressionEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.evaluation.RegressionEvaluator", self.uid)
        self._setDefault(metricName="rmse")
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @since("1.4.0")
    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    @since("1.4.0")
    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    @keyword_only
    @since("1.4.0")
    def setParams(self, predictionCol="prediction", labelCol="label",
                  metricName="rmse"):
        """
        setParams(self, predictionCol="prediction", labelCol="label", \
                  metricName="rmse")
        Sets params for regression evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class MulticlassClassificationEvaluator(JavaEvaluator, HasLabelCol, HasPredictionCol,
                                        JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Evaluator for Multiclass Classification, which expects two input
    columns: prediction and label.

    >>> scoreAndLabels = [(0.0, 0.0), (0.0, 1.0), (0.0, 0.0),
    ...     (1.0, 0.0), (1.0, 1.0), (1.0, 1.0), (1.0, 1.0), (2.0, 2.0), (2.0, 0.0)]
    >>> dataset = spark.createDataFrame(scoreAndLabels, ["prediction", "label"])
    ...
    >>> evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    >>> evaluator.evaluate(dataset)
    0.66...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "accuracy"})
    0.66...
    >>> mce_path = temp_path + "/mce"
    >>> evaluator.save(mce_path)
    >>> evaluator2 = MulticlassClassificationEvaluator.load(mce_path)
    >>> str(evaluator2.getPredictionCol())
    'prediction'

    .. versionadded:: 1.5.0
    """
    metricName = Param(Params._dummy(), "metricName",
                       "metric name in evaluation "
                       "(f1|weightedPrecision|weightedRecall|accuracy)",
                       typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, predictionCol="prediction", labelCol="label",
                 metricName="f1"):
        """
        __init__(self, predictionCol="prediction", labelCol="label", \
                 metricName="f1")
        """
        super(MulticlassClassificationEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator", self.uid)
        self._setDefault(metricName="f1")
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @since("1.5.0")
    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    @since("1.5.0")
    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    @keyword_only
    @since("1.5.0")
    def setParams(self, predictionCol="prediction", labelCol="label",
                  metricName="f1"):
        """
        setParams(self, predictionCol="prediction", labelCol="label", \
                  metricName="f1")
        Sets params for multiclass classification evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class ClusteringEvaluator(JavaEvaluator, HasPredictionCol, HasFeaturesCol,
                          JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Evaluator for Clustering results, which expects two input
    columns: prediction and features. The metric computes the Silhouette
    measure using the squared Euclidean distance.

    The Silhouette is a measure for the validation of the consistency
    within clusters. It ranges between 1 and -1, where a value close to
    1 means that the points in a cluster are close to the other points
    in the same cluster and far from the points of the other clusters.

    >>> from pyspark.ml.linalg import Vectors
    >>> featureAndPredictions = map(lambda x: (Vectors.dense(x[0]), x[1]),
    ...     [([0.0, 0.5], 0.0), ([0.5, 0.0], 0.0), ([10.0, 11.0], 1.0),
    ...     ([10.5, 11.5], 1.0), ([1.0, 1.0], 0.0), ([8.0, 6.0], 1.0)])
    >>> dataset = spark.createDataFrame(featureAndPredictions, ["features", "prediction"])
    ...
    >>> evaluator = ClusteringEvaluator(predictionCol="prediction")
    >>> evaluator.evaluate(dataset)
    0.9079...
    >>> ce_path = temp_path + "/ce"
    >>> evaluator.save(ce_path)
    >>> evaluator2 = ClusteringEvaluator.load(ce_path)
    >>> str(evaluator2.getPredictionCol())
    'prediction'

    .. versionadded:: 2.3.0
    """
    metricName = Param(Params._dummy(), "metricName",
                       "metric name in evaluation (silhouette)",
                       typeConverter=TypeConverters.toString)
    distanceMeasure = Param(Params._dummy(), "distanceMeasure", "The distance measure. " +
                            "Supported options: 'squaredEuclidean' and 'cosine'.",
                            typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, predictionCol="prediction", featuresCol="features",
                 metricName="silhouette", distanceMeasure="squaredEuclidean"):
        """
        __init__(self, predictionCol="prediction", featuresCol="features", \
                 metricName="silhouette", distanceMeasure="squaredEuclidean")
        """
        super(ClusteringEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.evaluation.ClusteringEvaluator", self.uid)
        self._setDefault(metricName="silhouette", distanceMeasure="squaredEuclidean")
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @since("2.3.0")
    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    @since("2.3.0")
    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    @keyword_only
    @since("2.3.0")
    def setParams(self, predictionCol="prediction", featuresCol="features",
                  metricName="silhouette", distanceMeasure="squaredEuclidean"):
        """
        setParams(self, predictionCol="prediction", featuresCol="features", \
                  metricName="silhouette", distanceMeasure="squaredEuclidean")
        Sets params for clustering evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.4.0")
    def setDistanceMeasure(self, value):
        """
        Sets the value of :py:attr:`distanceMeasure`.
        """
        return self._set(distanceMeasure=value)

    @since("2.4.0")
    def getDistanceMeasure(self):
        """
        Gets the value of `distanceMeasure`
        """
        return self.getOrDefault(self.distanceMeasure)


if __name__ == "__main__":
    import doctest
    import tempfile
    import pyspark.ml.evaluation
    from pyspark.sql import SparkSession
    globs = pyspark.ml.evaluation.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder\
        .master("local[2]")\
        .appName("ml.evaluation tests")\
        .getOrCreate()
    globs['spark'] = spark
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
        sys.exit(-1)
