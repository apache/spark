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

from pyspark.ml.wrapper import JavaEvaluator
from pyspark.ml.param import Param, Params
from pyspark.ml.param.shared import HasLabelCol, HasRawPredictionCol
from pyspark.ml.util import keyword_only
from pyspark.mllib.common import inherit_doc

__all__ = ['BinaryClassificationEvaluator']


@inherit_doc
class BinaryClassificationEvaluator(JavaEvaluator, HasLabelCol, HasRawPredictionCol):
    """
    Evaluator for binary classification, which expects two input
    columns: rawPrediction and label.

    >>> from pyspark.mllib.linalg import Vectors
    >>> scoreAndLabels = map(lambda x: (Vectors.dense([1.0 - x[0], x[0]]), x[1]),
    ...    [(0.1, 0.0), (0.1, 1.0), (0.4, 0.0), (0.6, 0.0), (0.6, 1.0), (0.6, 1.0), (0.8, 1.0)])
    >>> dataset = sqlContext.createDataFrame(scoreAndLabels, ["raw", "label"])
    ...
    >>> evaluator = BinaryClassificationEvaluator(rawPredictionCol="raw")
    >>> evaluator.evaluate(dataset)
    0.70...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "areaUnderPR"})
    0.83...
    """

    _java_class = "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator"

    # a placeholder to make it appear in the generated doc
    metricName = Param(Params._dummy(), "metricName",
                       "metric name in evaluation (areaUnderROC|areaUnderPR)")

    @keyword_only
    def __init__(self, rawPredictionCol="rawPrediction", labelCol="label",
                 metricName="areaUnderROC"):
        """
        __init__(self, rawPredictionCol="rawPrediction", labelCol="label", \
                 metricName="areaUnderROC")
        """
        super(BinaryClassificationEvaluator, self).__init__()
        #: param for metric name in evaluation (areaUnderROC|areaUnderPR)
        self.metricName = Param(self, "metricName",
                                "metric name in evaluation (areaUnderROC|areaUnderPR)")
        self._setDefault(rawPredictionCol="rawPrediction", labelCol="label",
                         metricName="areaUnderROC")
        kwargs = self.__init__._input_kwargs
        self._set(**kwargs)

    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        self._paramMap[self.metricName] = value
        return self

    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    @keyword_only
    def setParams(self, rawPredictionCol="rawPrediction", labelCol="label",
                  metricName="areaUnderROC"):
        """
        setParams(self, rawPredictionCol="rawPrediction", labelCol="label", \
                  metricName="areaUnderROC")
        Sets params for binary classification evaluator.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


if __name__ == "__main__":
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.evaluation tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)
