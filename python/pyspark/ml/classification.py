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

from pyspark.ml.util import inherit_doc
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.param.shared import HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,\
    HasRegParam


__all__ = ['LogisticRegression', 'LogisticRegressionModel']


@inherit_doc
class LogisticRegression(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                         HasRegParam):
    """
    Logistic regression.

    >>> from pyspark.sql import Row
    >>> from pyspark.mllib.linalg import Vectors
    >>> dataset = sqlCtx.inferSchema(sc.parallelize([ \
            Row(label=1.0, features=Vectors.dense(1.0)), \
            Row(label=0.0, features=Vectors.sparse(1, [], []))]))
    >>> lr = LogisticRegression() \
            .setMaxIter(5) \
            .setRegParam(0.01)
    >>> model = lr.fit(dataset)
    >>> test0 = sqlCtx.inferSchema(sc.parallelize([Row(features=Vectors.dense(-1.0))]))
    >>> print model.transform(test0).head().prediction
    0.0
    >>> test1 = sqlCtx.inferSchema(sc.parallelize([Row(features=Vectors.sparse(1, [0], [1.0]))]))
    >>> print model.transform(test1).head().prediction
    1.0
    """
    _java_class = "org.apache.spark.ml.classification.LogisticRegression"

    def _create_model(self, java_model):
        return LogisticRegressionModel(java_model)


class LogisticRegressionModel(JavaModel):
    """
    Model fitted by LogisticRegression.
    """


if __name__ == "__main__":
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.feature tests")
    sqlCtx = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlCtx'] = sqlCtx
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)
