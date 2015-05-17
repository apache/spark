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
from pyspark.ml.regression import RandomForestParams
from pyspark.mllib.common import inherit_doc


__all__ = ['LogisticRegression', 'LogisticRegressionModel', 'DecisionTreeClassifier',
           'DecisionTreeClassificationModel', 'GBTClassifier', 'GBTClassificationModel',
           'RandomForestClassifier', 'RandomForestClassificationModel']


@inherit_doc
class LogisticRegression(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                         HasRegParam, HasTol, HasProbabilityCol):
    """
    Logistic regression.

    >>> from pyspark.sql import Row
    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sc.parallelize([
    ...     Row(label=1.0, features=Vectors.dense(1.0)),
    ...     Row(label=0.0, features=Vectors.sparse(1, [], []))]).toDF()
    >>> lr = LogisticRegression(maxIter=5, regParam=0.01)
    >>> model = lr.fit(df)
    >>> test0 = sc.parallelize([Row(features=Vectors.dense(-1.0))]).toDF()
    >>> model.transform(test0).head().prediction
    0.0
    >>> model.weights
    DenseVector([5.5...])
    >>> model.intercept
    -2.68...
    >>> test1 = sc.parallelize([Row(features=Vectors.sparse(1, [0], [1.0]))]).toDF()
    >>> model.transform(test1).head().prediction
    1.0
    >>> lr.setParams("vector")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    """
    _java_class = "org.apache.spark.ml.classification.LogisticRegression"
    # a placeholder to make it appear in the generated doc
    elasticNetParam = \
        Param(Params._dummy(), "elasticNetParam",
              "the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, " +
              "the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.")
    fitIntercept = Param(Params._dummy(), "fitIntercept", "whether to fit an intercept term.")
    threshold = Param(Params._dummy(), "threshold",
                      "threshold in binary classification prediction, in range [0, 1].")

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxIter=100, regParam=0.1, elasticNetParam=0.0, tol=1e-6, fitIntercept=True,
                 threshold=0.5, probabilityCol="probability"):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, regParam=0.1, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                 threshold=0.5, probabilityCol="probability")
        """
        super(LogisticRegression, self).__init__()
        #: param for the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty
        #  is an L2 penalty. For alpha = 1, it is an L1 penalty.
        self.elasticNetParam = \
            Param(self, "elasticNetParam",
                  "the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty " +
                  "is an L2 penalty. For alpha = 1, it is an L1 penalty.")
        #: param for whether to fit an intercept term.
        self.fitIntercept = Param(self, "fitIntercept", "whether to fit an intercept term.")
        #: param for threshold in binary classification prediction, in range [0, 1].
        self.threshold = Param(self, "threshold",
                               "threshold in binary classification prediction, in range [0, 1].")
        self._setDefault(maxIter=100, regParam=0.1, elasticNetParam=0.0, tol=1E-6,
                         fitIntercept=True, threshold=0.5)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxIter=100, regParam=0.1, elasticNetParam=0.0, tol=1e-6, fitIntercept=True,
                  threshold=0.5, probabilityCol="probability"):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, regParam=0.1, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                 threshold=0.5, probabilityCol="probability")
        Sets params for logistic regression.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return LogisticRegressionModel(java_model)

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

    def setFitIntercept(self, value):
        """
        Sets the value of :py:attr:`fitIntercept`.
        """
        self.paramMap[self.fitIntercept] = value
        return self

    def getFitIntercept(self):
        """
        Gets the value of fitIntercept or its default value.
        """
        return self.getOrDefault(self.fitIntercept)

    def setThreshold(self, value):
        """
        Sets the value of :py:attr:`threshold`.
        """
        self.paramMap[self.threshold] = value
        return self

    def getThreshold(self):
        """
        Gets the value of threshold or its default value.
        """
        return self.getOrDefault(self.threshold)


class LogisticRegressionModel(JavaModel):
    """
    Model fitted by LogisticRegression.
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


class TreeClassifierParams(object):
    """
    Private class to track supported impurity measures.
    """
    supportedImpurities = ["entropy", "gini"]


class GBTParams(object):
    """
    Private class to track supported GBT params.
    """
    supportedLossTypes = ["logistic"]


@inherit_doc
class DecisionTreeClassifier(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol,
                             DecisionTreeParams, HasCheckpointInterval):
    """
    `http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree`
    learning algorithm for classification.
    It supports both binary and multiclass labels, as well as both continuous and categorical
    features.

    >>> from pyspark.mllib.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> dt = DecisionTreeClassifier(maxDepth=2, labelCol="indexed")
    >>> model = dt.fit(td)
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    """

    _java_class = "org.apache.spark.ml.classification.DecisionTreeClassifier"
    # a placeholder to make it appear in the generated doc
    impurity = Param(Params._dummy(), "impurity",
                     "Criterion used for information gain calculation (case-insensitive). " +
                     "Supported options: " + ", ".join(TreeClassifierParams.supportedImpurities))

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini"):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini")
        """
        super(DecisionTreeClassifier, self).__init__()
        #: param for Criterion used for information gain calculation (case-insensitive).
        self.impurity = \
            Param(self, "impurity",
                  "Criterion used for information gain calculation (case-insensitive). " +
                  "Supported options: " + ", ".join(TreeClassifierParams.supportedImpurities))
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         impurity="gini")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  impurity="gini"):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini")
        Sets params for the DecisionTreeClassifier.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return DecisionTreeClassificationModel(java_model)

    def setImpurity(self, value):
        """
        Sets the value of :py:attr:`impurity`.
        """
        self.paramMap[self.impurity] = value
        return self

    def getImpurity(self):
        """
        Gets the value of impurity or its default value.
        """
        return self.getOrDefault(self.impurity)


class DecisionTreeClassificationModel(JavaModel):
    """
    Model fitted by DecisionTreeClassifier.
    """


@inherit_doc
class RandomForestClassifier(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasSeed,
                             DecisionTreeParams, HasCheckpointInterval):
    """
    `http://en.wikipedia.org/wiki/Random_forest  Random Forest`
    learning algorithm for classification.
    It supports both binary and multiclass labels, as well as both continuous and categorical
    features.

    >>> from pyspark.mllib.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> rf = RandomForestClassifier(numTrees=2, maxDepth=2, labelCol="indexed")
    >>> model = rf.fit(td)
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    """

    _java_class = "org.apache.spark.ml.classification.RandomForestClassifier"
    # a placeholder to make it appear in the generated doc
    impurity = Param(Params._dummy(), "impurity",
                     "Criterion used for information gain calculation (case-insensitive). " +
                     "Supported options: " + ", ".join(TreeClassifierParams.supportedImpurities))
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
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini",
                 numTrees=20, featureSubsetStrategy="auto", seed=42):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini", \
                 numTrees=20, featureSubsetStrategy="auto", seed=42)
        """
        super(RandomForestClassifier, self).__init__()
        #: param for Criterion used for information gain calculation (case-insensitive).
        self.impurity = \
            Param(self, "impurity",
                  "Criterion used for information gain calculation (case-insensitive). " +
                  "Supported options: " + ", ".join(TreeClassifierParams.supportedImpurities))
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
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=42,
                         impurity="gini", numTrees=20, featureSubsetStrategy="auto")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=42,
                  impurity="gini", numTrees=20, featureSubsetStrategy="auto"):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=42, \
                  impurity="gini", numTrees=20, featureSubsetStrategy="auto")
        Sets params for linear classification.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return RandomForestClassificationModel(java_model)

    def setImpurity(self, value):
        """
        Sets the value of :py:attr:`impurity`.
        """
        self.paramMap[self.impurity] = value
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
        self.paramMap[self.subsamplingRate] = value
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
        self.paramMap[self.numTrees] = value
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
        self.paramMap[self.featureSubsetStrategy] = value
        return self

    def getFeatureSubsetStrategy(self):
        """
        Gets the value of featureSubsetStrategy or its default value.
        """
        return self.getOrDefault(self.featureSubsetStrategy)


class RandomForestClassificationModel(JavaModel):
    """
    Model fitted by RandomForestClassifier.
    """


@inherit_doc
class GBTClassifier(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                    DecisionTreeParams, HasCheckpointInterval):
    """
    `http://en.wikipedia.org/wiki/Gradient_boosting Gradient-Boosted Trees (GBTs)`
    learning algorithm for classification.
    It supports binary labels, as well as both continuous and categorical features.
    Note: Multiclass labels are not currently supported.

    >>> from pyspark.mllib.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> gbt = GBTClassifier(maxIter=5, maxDepth=2, labelCol="indexed")
    >>> model = gbt.fit(td)
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    """

    _java_class = "org.apache.spark.ml.classification.GBTClassifier"
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
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, lossType="logistic",
                 maxIter=20, stepSize=0.1):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                 lossType="logistic", maxIter=20, stepSize=0.1)
        """
        super(GBTClassifier, self).__init__()
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
                         lossType="logistic", maxIter=20, stepSize=0.1)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  lossType="logistic", maxIter=20, stepSize=0.1):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                  lossType="logistic", maxIter=20, stepSize=0.1)
        Sets params for Gradient Boosted Tree Classification.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return GBTClassificationModel(java_model)

    def setLossType(self, value):
        """
        Sets the value of :py:attr:`lossType`.
        """
        self.paramMap[self.lossType] = value
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
        self.paramMap[self.subsamplingRate] = value
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
        self.paramMap[self.stepSize] = value
        return self

    def getStepSize(self):
        """
        Gets the value of stepSize or its default value.
        """
        return self.getOrDefault(self.stepSize)


class GBTClassificationModel(JavaModel):
    """
    Model fitted by GBTClassifier.
    """


if __name__ == "__main__":
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.classification tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)
