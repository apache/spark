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

from pyspark.mllib.common import JavaModelWrapper
from pyspark.sql import SQLContext
from pyspark.sql.types import StructField, StructType, DoubleType


class BinaryClassificationMetrics(JavaModelWrapper):
    """
    Evaluator for binary classification.

    >>> scoreAndLabels = sc.parallelize([
    ...     (0.1, 0.0), (0.1, 1.0), (0.4, 0.0), (0.6, 0.0), (0.6, 1.0), (0.6, 1.0), (0.8, 1.0)], 2)
    >>> metrics = BinaryClassificationMetrics(scoreAndLabels)
    >>> metrics.areaUnderROC()
    0.70...
    >>> metrics.areaUnderPR()
    0.83...
    >>> metrics.unpersist()
    """

    def __init__(self, scoreAndLabels):
        """
        :param scoreAndLabels: an RDD of (score, label) pairs
        """
        sc = scoreAndLabels.ctx
        sql_ctx = SQLContext(sc)
        df = sql_ctx.createDataFrame(scoreAndLabels, schema=StructType([
            StructField("score", DoubleType(), nullable=False),
            StructField("label", DoubleType(), nullable=False)]))
        java_class = sc._jvm.org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
        java_model = java_class(df._jdf)
        super(BinaryClassificationMetrics, self).__init__(java_model)

    def areaUnderROC(self):
        """
        Computes the area under the receiver operating characteristic
        (ROC) curve.
        """
        return self.call("areaUnderROC")

    def areaUnderPR(self):
        """
        Computes the area under the precision-recall curve.
        """
        return self.call("areaUnderPR")

    def unpersist(self):
        """
        Unpersists intermediate RDDs used in the computation.
        """
        self.call("unpersist")


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
