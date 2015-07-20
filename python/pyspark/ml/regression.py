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


__all__ = ['DecisionTreeRegressor', 'DecisionTreeRegressionModel', 'GBTRegressor',
           'GBTRegressionModel', 'LinearRegression', 'LinearRegressionModel',
           'RandomForestRegressor', 'RandomForestRegressionModel']


@inherit_doc
class LinearRegression(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                       HasRegParam, HasTol):
    """
    Linear regression.

    The learning objective is to minimize the squared error, with regularization.
    The specific squared error loss function used is: L = 1/2n ||A weights - y||^2^

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
    >>> model.weights
    DenseVector([1.0])
    >>> model.intercept
    0.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    >>> lr.setParams("vector")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    """

    # a placeholder to make it appear in the generated doc
    elasticNetParam = \
        Param(Params._dummy(), "elasticNetParam",
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
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.LinearRegression", self.uid)
        #: param for the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty
        #  is an L2 penalty. For alpha = 1, it is an L1 penalty.
        self.elasticNetParam = \
            Param(self, "elasticNetParam",
                  "the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty " +
                  "is an L2 penalty. For alpha = 1, it is an L1 penalty.")
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
        self._paramMap[self.elasticNetParam] = value
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

    @property
    def weights(self):
        """
        Model weights.
        """
        return self._call_java("weights")

    @property
    def intercept(self):
        """
        Model intercept.
        """
        return self._call_java("intercept")


class TreeRegressorParams(object):
    """
    Private class to track supported impurity measures.
    """
    supportedImpurities = ["variance"]


class RandomForestParams(object):
    """
    Private class to track supported random forest parameters.
    """
    supportedFeatureSubsetStrategies = ["auto", "all", "onethird", "sqrt", "log2"]


class GBTParams(object):
    """
    Private class to track supported GBT params.
    """
    supportedLossTypes = ["squared", "absolute"]


@inherit_doc
class DecisionTreeRegressor(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol,
                            DecisionTreeParams, HasCheckpointInterval):
    """
    `http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree`
    learning algorithm for regression.
    It supports both continuous and categorical features.

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> dt = DecisionTreeRegressor(maxDepth=2)
    >>> model = dt.fit(df)
    >>> model.depth
    1
    >>> model.numNodes
    3
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    """

    # a placeholder to make it appear in the generated doc
    impurity = Param(Params._dummy(), "impurity",
                     "Criterion used for information gain calculation (case-insensitive). " +
                     "Supported options: " + ", ".join(TreeRegressorParams.supportedImpurities))

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="variance"):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="variance")
        """
        super(DecisionTreeRegressor, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.DecisionTreeRegressor", self.uid)
        #: param for Criterion used for information gain calculation (case-insensitive).
        self.impurity = \
            Param(self, "impurity",
                  "Criterion used for information gain calculation (case-insensitive). " +
                  "Supported options: " + ", ".join(TreeRegressorParams.supportedImpurities))
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         impurity="variance")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  impurity="variance"):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="variance")
        Sets params for the DecisionTreeRegressor.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return DecisionTreeRegressionModel(java_model)

    def setImpurity(self, value):
        """
        Sets the value of :py:attr:`impurity`.
        """
        self._paramMap[self.impurity] = value
        return self

    def getImpurity(self):
        """
        Gets the value of impurity or its default value.
        """
        return self.getOrDefault(self.impurity)


@inherit_doc
class DecisionTreeModel(JavaModel):

    @property
    def numNodes(self):
        """Return number of nodes of the decision tree."""
        return self._call_java("numNodes")

    @property
    def depth(self):
        """Return depth of the decision tree."""
        return self._call_java("depth")

    def __repr__(self):
        return self._call_java("toString")


@inherit_doc
class TreeEnsembleModels(JavaModel):

    @property
    def treeWeights(self):
        """Return the weights for each tree"""
        return list(self._call_java("javaTreeWeights"))

    def __repr__(self):
        return self._call_java("toString")


@inherit_doc
class DecisionTreeRegressionModel(DecisionTreeModel):
    """
    Model fitted by DecisionTreeRegressor.
    """


@inherit_doc
class RandomForestRegressor(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasSeed,
                            DecisionTreeParams, HasCheckpointInterval):
    """
    `http://en.wikipedia.org/wiki/Random_forest  Random Forest`
    learning algorithm for regression.
    It supports both continuous and categorical features.

    >>> from numpy import allclose
    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> rf = RandomForestRegressor(numTrees=2, maxDepth=2, seed=42)
    >>> model = rf.fit(df)
    >>> allclose(model.treeWeights, [1.0, 1.0])
    True
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    0.5
    """

    # a placeholder to make it appear in the generated doc
    impurity = Param(Params._dummy(), "impurity",
                     "Criterion used for information gain calculation (case-insensitive). " +
                     "Supported options: " + ", ".join(TreeRegressorParams.supportedImpurities))
    subsamplingRate = Param(Params._dummy(), "subsamplingRate",
                            "Fraction of the training data used for learning each decision tree, " +
                            "in range (0, 1].")
    numTrees = Param(Params._dummy(), "numTrees", "Number of trees to train (>= 1)")
    featureSubsetStrategy = \
        Param(Params._dummy(), "featureSubsetStrategy",
              "The number of features to consider for splits at each tree node. Supported " +
              "options: " + ", ".join(RandomForestParams.supportedFeatureSubsetStrategies))

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="variance",
                 numTrees=20, featureSubsetStrategy="auto", seed=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                 impurity="variance", numTrees=20, \
                 featureSubsetStrategy="auto", seed=None)
        """
        super(RandomForestRegressor, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.RandomForestRegressor", self.uid)
        #: param for Criterion used for information gain calculation (case-insensitive).
        self.impurity = \
            Param(self, "impurity",
                  "Criterion used for information gain calculation (case-insensitive). " +
                  "Supported options: " + ", ".join(TreeRegressorParams.supportedImpurities))
        #: param for Fraction of the training data used for learning each decision tree,
        #  in range (0, 1]
        self.subsamplingRate = Param(self, "subsamplingRate",
                                     "Fraction of the training data used for learning each " +
                                     "decision tree, in range (0, 1].")
        #: param for Number of trees to train (>= 1)
        self.numTrees = Param(self, "numTrees", "Number of trees to train (>= 1)")
        #: param for The number of features to consider for splits at each tree node
        self.featureSubsetStrategy = \
            Param(self, "featureSubsetStrategy",
                  "The number of features to consider for splits at each tree node. Supported " +
                  "options: " + ", ".join(RandomForestParams.supportedFeatureSubsetStrategies))
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=None,
                         impurity="variance", numTrees=20, featureSubsetStrategy="auto")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=None,
                  impurity="variance", numTrees=20, featureSubsetStrategy="auto"):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=None, \
                  impurity="variance", numTrees=20, featureSubsetStrategy="auto")
        Sets params for linear regression.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return RandomForestRegressionModel(java_model)

    def setImpurity(self, value):
        """
        Sets the value of :py:attr:`impurity`.
        """
        self._paramMap[self.impurity] = value
        return self

    def getImpurity(self):
        """
        Gets the value of impurity or its default value.
        """
        return self.getOrDefault(self.impurity)

    def setSubsamplingRate(self, value):
        """
        Sets the value of :py:attr:`subsamplingRate`.
        """
        self._paramMap[self.subsamplingRate] = value
        return self

    def getSubsamplingRate(self):
        """
        Gets the value of subsamplingRate or its default value.
        """
        return self.getOrDefault(self.subsamplingRate)

    def setNumTrees(self, value):
        """
        Sets the value of :py:attr:`numTrees`.
        """
        self._paramMap[self.numTrees] = value
        return self

    def getNumTrees(self):
        """
        Gets the value of numTrees or its default value.
        """
        return self.getOrDefault(self.numTrees)

    def setFeatureSubsetStrategy(self, value):
        """
        Sets the value of :py:attr:`featureSubsetStrategy`.
        """
        self._paramMap[self.featureSubsetStrategy] = value
        return self

    def getFeatureSubsetStrategy(self):
        """
        Gets the value of featureSubsetStrategy or its default value.
        """
        return self.getOrDefault(self.featureSubsetStrategy)


class RandomForestRegressionModel(TreeEnsembleModels):
    """
    Model fitted by RandomForestRegressor.
    """


@inherit_doc
class GBTRegressor(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                   DecisionTreeParams, HasCheckpointInterval):
    """
    `http://en.wikipedia.org/wiki/Gradient_boosting Gradient-Boosted Trees (GBTs)`
    learning algorithm for regression.
    It supports both continuous and categorical features.

    >>> from numpy import allclose
    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> gbt = GBTRegressor(maxIter=5, maxDepth=2)
    >>> model = gbt.fit(df)
    >>> allclose(model.treeWeights, [1.0, 0.1, 0.1, 0.1, 0.1])
    True
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    """

    # a placeholder to make it appear in the generated doc
    lossType = Param(Params._dummy(), "lossType",
                     "Loss function which GBT tries to minimize (case-insensitive). " +
                     "Supported options: " + ", ".join(GBTParams.supportedLossTypes))
    subsamplingRate = Param(Params._dummy(), "subsamplingRate",
                            "Fraction of the training data used for learning each decision tree, " +
                            "in range (0, 1].")
    stepSize = Param(Params._dummy(), "stepSize",
                     "Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the " +
                     "contribution of each estimator")

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, lossType="squared",
                 maxIter=20, stepSize=0.1):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                 lossType="squared", maxIter=20, stepSize=0.1)
        """
        super(GBTRegressor, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.regression.GBTRegressor", self.uid)
        #: param for Loss function which GBT tries to minimize (case-insensitive).
        self.lossType = Param(self, "lossType",
                              "Loss function which GBT tries to minimize (case-insensitive). " +
                              "Supported options: " + ", ".join(GBTParams.supportedLossTypes))
        #: Fraction of the training data used for learning each decision tree, in range (0, 1].
        self.subsamplingRate = Param(self, "subsamplingRate",
                                     "Fraction of the training data used for learning each " +
                                     "decision tree, in range (0, 1].")
        #: Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of
        #  each estimator
        self.stepSize = Param(self, "stepSize",
                              "Step size (a.k.a. learning rate) in interval (0, 1] for shrinking " +
                              "the contribution of each estimator")
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         lossType="squared", maxIter=20, stepSize=0.1)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  lossType="squared", maxIter=20, stepSize=0.1):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                  lossType="squared", maxIter=20, stepSize=0.1)
        Sets params for Gradient Boosted Tree Regression.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return GBTRegressionModel(java_model)

    def setLossType(self, value):
        """
        Sets the value of :py:attr:`lossType`.
        """
        self._paramMap[self.lossType] = value
        return self

    def getLossType(self):
        """
        Gets the value of lossType or its default value.
        """
        return self.getOrDefault(self.lossType)

    def setSubsamplingRate(self, value):
        """
        Sets the value of :py:attr:`subsamplingRate`.
        """
        self._paramMap[self.subsamplingRate] = value
        return self

    def getSubsamplingRate(self):
        """
        Gets the value of subsamplingRate or its default value.
        """
        return self.getOrDefault(self.subsamplingRate)

    def setStepSize(self, value):
        """
        Sets the value of :py:attr:`stepSize`.
        """
        self._paramMap[self.stepSize] = value
        return self

    def getStepSize(self):
        """
        Gets the value of stepSize or its default value.
        """
        return self.getOrDefault(self.stepSize)


class GBTRegressionModel(TreeEnsembleModels):
    """
    Model fitted by GBTRegressor.
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
