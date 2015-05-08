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

from pyspark.ml.util import keyword_only
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.param.shared import *
from pyspark.mllib.common import inherit_doc


__all__ = ['LinearRegression', 'LinearRegressionModel']


@inherit_doc
class LinearRegression(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                       HasRegParam, HasTol):
    """
    Linear regression.

    The learning objective is to minimize the squared error, with regularization.
    The specific squared error loss function used is:
      L = 1/2n ||A weights - y||^2^

    This support multiple types of regularization:
     - none (a.k.a. ordinary least squares)
     - L2 (ridge regression)
     - L1 (Lasso)
     - L2 + L1 (elastic net)

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> lr = LinearRegression(maxIter=5, regParam=0.0)
    >>> model = lr.fit(df)
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    -1.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    >>> lr.setParams("vector")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    """
    _java_class = "org.apache.spark.ml.regression.LinearRegression"
    # a placeholder to make it appear in the generated doc
    elasticNetParam = Param(Params._dummy(), "elasticNetParam",
                            "the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, " +
                            "the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.")

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6)
        """
        super(LinearRegression, self).__init__()
        self.elasticNetParam = Param(self, "elasticNetParam",
                                     "the ElasticNet mixing parameter, in range [0, 1]. For " +
                                     "alpha = 0, the penalty is an L2 penalty. For alpha = 1, " +
                                     "it is an L1 penalty.")
        self._setDefault(maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6)
        Sets params for linear regression.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return LinearRegressionModel(java_model)

    def setElasticNetParam(self, value):
        """
        Sets the value of :py:attr:`elasticNetParam`.
        """
        self.paramMap[self.elasticNetParam] = value
        return self

    def getElasticNetParam(self):
        """
        Gets the value of elasticNetParam or its default value.
        """
        return self.getOrDefault(self.elasticNetParam)


class LinearRegressionModel(JavaModel):
    """
    Model fitted by LinearRegression.
    """


if __name__ == "__main__":
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.regression tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)
