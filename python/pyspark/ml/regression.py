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

import warnings

from pyspark import since, keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaWrapper
from pyspark.mllib.common import inherit_doc
from pyspark.sql import DataFrame


__all__ = ['AFTSurvivalRegression', 'AFTSurvivalRegressionModel',
           'DecisionTreeRegressor', 'DecisionTreeRegressionModel',
           'GBTRegressor', 'GBTRegressionModel',
           'GeneralizedLinearRegression', 'GeneralizedLinearRegressionModel',
           'IsotonicRegression', 'IsotonicRegressionModel',
           'LinearRegression', 'LinearRegressionModel',
           'LinearRegressionSummary', 'LinearRegressionTrainingSummary',
           'RandomForestRegressor', 'RandomForestRegressionModel']


@inherit_doc
class LinearRegression(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                       HasRegParam, HasTol, HasElasticNetParam, HasFitIntercept,
                       HasStandardization, HasSolver, HasWeightCol, JavaMLWritable, JavaMLReadable):
    """
    Linear regression.

    The learning objective is to minimize the squared error, with regularization.
    The specific squared error loss function used is: L = 1/2n ||A coefficients - y||^2^

    This support multiple types of regularization:
     - none (a.k.a. ordinary least squares)
     - L2 (ridge regression)
     - L1 (Lasso)
     - L2 + L1 (elastic net)

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, 2.0, Vectors.dense(1.0)),
    ...     (0.0, 2.0, Vectors.sparse(1, [], []))], ["label", "weight", "features"])
    >>> lr = LinearRegression(maxIter=5, regParam=0.0, solver="normal", weightCol="weight")
    >>> model = lr.fit(df)
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> abs(model.transform(test0).head().prediction - (-1.0)) < 0.001
    True
    >>> abs(model.coefficients[0] - 1.0) < 0.001
    True
    >>> abs(model.intercept - 0.0) < 0.001
    True
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> abs(model.transform(test1).head().prediction - 1.0) < 0.001
    True
    >>> lr.setParams("vector")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    >>> lr_path = temp_path + "/lr"
    >>> lr.save(lr_path)
    >>> lr2 = LinearRegression.load(lr_path)
    >>> lr2.getMaxIter()
    5
    >>> model_path = temp_path + "/lr_model"
    >>> model.save(model_path)
    >>> model2 = LinearRegressionModel.load(model_path)
    >>> model.coefficients[0] == model2.coefficients[0]
    True
    >>> model.intercept == model2.intercept
    True

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True,
                 standardization=True, solver="auto", weightCol=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                 standardization=True, solver="auto", weightCol=None)
        """
        super(LinearRegression, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.LinearRegression", self.uid)
        self._setDefault(maxIter=100, regParam=0.0, tol=1e-6)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True,
                  standardization=True, solver="auto", weightCol=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                  standardization=True, solver="auto", weightCol=None)
        Sets params for linear regression.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return LinearRegressionModel(java_model)


class LinearRegressionModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by LinearRegression.

    .. versionadded:: 1.4.0
    """

    @property
    @since("1.6.0")
    def coefficients(self):
        """
        Model coefficients.
        """
        return self._call_java("coefficients")

    @property
    @since("1.4.0")
    def intercept(self):
        """
        Model intercept.
        """
        return self._call_java("intercept")

    @property
    @since("2.0.0")
    def summary(self):
        """
        Gets summary (e.g. residuals, mse, r-squared ) of model on
        training set. An exception is thrown if
        `trainingSummary is None`.
        """
        java_lrt_summary = self._call_java("summary")
        return LinearRegressionTrainingSummary(java_lrt_summary)

    @property
    @since("2.0.0")
    def hasSummary(self):
        """
        Indicates whether a training summary exists for this model
        instance.
        """
        return self._call_java("hasSummary")

    @since("2.0.0")
    def evaluate(self, dataset):
        """
        Evaluates the model on a test dataset.

        :param dataset:
          Test dataset to evaluate model on, where dataset is an
          instance of :py:class:`pyspark.sql.DataFrame`
        """
        if not isinstance(dataset, DataFrame):
            raise ValueError("dataset must be a DataFrame but got %s." % type(dataset))
        java_lr_summary = self._call_java("evaluate", dataset)
        return LinearRegressionSummary(java_lr_summary)


class LinearRegressionSummary(JavaWrapper):
    """
    .. note:: Experimental

    Linear regression results evaluated on a dataset.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def predictions(self):
        """
        Dataframe outputted by the model's `transform` method.
        """
        return self._call_java("predictions")

    @property
    @since("2.0.0")
    def predictionCol(self):
        """
        Field in "predictions" which gives the predicted value of
        the label at each instance.
        """
        return self._call_java("predictionCol")

    @property
    @since("2.0.0")
    def labelCol(self):
        """
        Field in "predictions" which gives the true label of each
        instance.
        """
        return self._call_java("labelCol")

    @property
    @since("2.0.0")
    def featuresCol(self):
        """
        Field in "predictions" which gives the features of each instance
        as a vector.
        """
        return self._call_java("featuresCol")

    @property
    @since("2.0.0")
    def explainedVariance(self):
        """
        Returns the explained variance regression score.
        explainedVariance = 1 - variance(y - \hat{y}) / variance(y)

        .. seealso:: `Wikipedia explain variation \
        <http://en.wikipedia.org/wiki/Explained_variation>`_

        Note: This ignores instance weights (setting all to 1.0) from
        `LinearRegression.weightCol`. This will change in later Spark
        versions.
        """
        return self._call_java("explainedVariance")

    @property
    @since("2.0.0")
    def meanAbsoluteError(self):
        """
        Returns the mean absolute error, which is a risk function
        corresponding to the expected value of the absolute error
        loss or l1-norm loss.

        Note: This ignores instance weights (setting all to 1.0) from
        `LinearRegression.weightCol`. This will change in later Spark
        versions.
        """
        return self._call_java("meanAbsoluteError")

    @property
    @since("2.0.0")
    def meanSquaredError(self):
        """
        Returns the mean squared error, which is a risk function
        corresponding to the expected value of the squared error
        loss or quadratic loss.

        Note: This ignores instance weights (setting all to 1.0) from
        `LinearRegression.weightCol`. This will change in later Spark
        versions.
        """
        return self._call_java("meanSquaredError")

    @property
    @since("2.0.0")
    def rootMeanSquaredError(self):
        """
        Returns the root mean squared error, which is defined as the
        square root of the mean squared error.

        Note: This ignores instance weights (setting all to 1.0) from
        `LinearRegression.weightCol`. This will change in later Spark
        versions.
        """
        return self._call_java("rootMeanSquaredError")

    @property
    @since("2.0.0")
    def r2(self):
        """
        Returns R^2^, the coefficient of determination.

        .. seealso:: `Wikipedia coefficient of determination \
        <http://en.wikipedia.org/wiki/Coefficient_of_determination>`

        Note: This ignores instance weights (setting all to 1.0) from
        `LinearRegression.weightCol`. This will change in later Spark
        versions.
        """
        return self._call_java("r2")

    @property
    @since("2.0.0")
    def residuals(self):
        """
        Residuals (label - predicted value)
        """
        return self._call_java("residuals")

    @property
    @since("2.0.0")
    def numInstances(self):
        """
        Number of instances in DataFrame predictions
        """
        return self._call_java("numInstances")

    @property
    @since("2.0.0")
    def devianceResiduals(self):
        """
        The weighted residuals, the usual residuals rescaled by the
        square root of the instance weights.
        """
        return self._call_java("devianceResiduals")

    @property
    @since("2.0.0")
    def coefficientStandardErrors(self):
        """
        Standard error of estimated coefficients and intercept.
        This value is only available when using the "normal" solver.

        If :py:attr:`LinearRegression.fitIntercept` is set to True,
        then the last element returned corresponds to the intercept.

        .. seealso:: :py:attr:`LinearRegression.solver`
        """
        return self._call_java("coefficientStandardErrors")

    @property
    @since("2.0.0")
    def tValues(self):
        """
        T-statistic of estimated coefficients and intercept.
        This value is only available when using the "normal" solver.

        If :py:attr:`LinearRegression.fitIntercept` is set to True,
        then the last element returned corresponds to the intercept.

        .. seealso:: :py:attr:`LinearRegression.solver`
        """
        return self._call_java("tValues")

    @property
    @since("2.0.0")
    def pValues(self):
        """
        Two-sided p-value of estimated coefficients and intercept.
        This value is only available when using the "normal" solver.

        If :py:attr:`LinearRegression.fitIntercept` is set to True,
        then the last element returned corresponds to the intercept.

        .. seealso:: :py:attr:`LinearRegression.solver`
        """
        return self._call_java("pValues")


@inherit_doc
class LinearRegressionTrainingSummary(LinearRegressionSummary):
    """
    .. note:: Experimental

    Linear regression training results. Currently, the training summary ignores the
    training weights except for the objective trace.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def objectiveHistory(self):
        """
        Objective function (scaled loss + regularization) at each
        iteration.
        This value is only available when using the "l-bfgs" solver.

        .. seealso:: :py:attr:`LinearRegression.solver`
        """
        return self._call_java("objectiveHistory")

    @property
    @since("2.0.0")
    def totalIterations(self):
        """
        Number of training iterations until termination.
        This value is only available when using the "l-bfgs" solver.

        .. seealso:: :py:attr:`LinearRegression.solver`
        """
        return self._call_java("totalIterations")


@inherit_doc
class IsotonicRegression(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol,
                         HasWeightCol, JavaMLWritable, JavaMLReadable):
    """
    .. note:: Experimental

    Currently implemented using parallelized pool adjacent violators algorithm.
    Only univariate (single feature) algorithm supported.

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> ir = IsotonicRegression()
    >>> model = ir.fit(df)
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> model.boundaries
    DenseVector([0.0, 1.0])
    >>> ir_path = temp_path + "/ir"
    >>> ir.save(ir_path)
    >>> ir2 = IsotonicRegression.load(ir_path)
    >>> ir2.getIsotonic()
    True
    >>> model_path = temp_path + "/ir_model"
    >>> model.save(model_path)
    >>> model2 = IsotonicRegressionModel.load(model_path)
    >>> model.boundaries == model2.boundaries
    True
    >>> model.predictions == model2.predictions
    True
    """

    isotonic = \
        Param(Params._dummy(), "isotonic",
              "whether the output sequence should be isotonic/increasing (true) or" +
              "antitonic/decreasing (false).", typeConverter=TypeConverters.toBoolean)
    featureIndex = \
        Param(Params._dummy(), "featureIndex",
              "The index of the feature if featuresCol is a vector column, no effect otherwise.",
              typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 weightCol=None, isotonic=True, featureIndex=0):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 weightCol=None, isotonic=True, featureIndex=0):
        """
        super(IsotonicRegression, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.IsotonicRegression", self.uid)
        self._setDefault(isotonic=True, featureIndex=0)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  weightCol=None, isotonic=True, featureIndex=0):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 weightCol=None, isotonic=True, featureIndex=0):
        Set the params for IsotonicRegression.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return IsotonicRegressionModel(java_model)

    def setIsotonic(self, value):
        """
        Sets the value of :py:attr:`isotonic`.
        """
        return self._set(isotonic=value)

    def getIsotonic(self):
        """
        Gets the value of isotonic or its default value.
        """
        return self.getOrDefault(self.isotonic)

    def setFeatureIndex(self, value):
        """
        Sets the value of :py:attr:`featureIndex`.
        """
        return self._set(featureIndex=value)

    def getFeatureIndex(self):
        """
        Gets the value of featureIndex or its default value.
        """
        return self.getOrDefault(self.featureIndex)


class IsotonicRegressionModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    .. note:: Experimental

    Model fitted by IsotonicRegression.
    """

    @property
    def boundaries(self):
        """
        Model boundaries.
        """
        return self._call_java("boundaries")

    @property
    def predictions(self):
        """
        Predictions associated with the boundaries at the same index, monotone because of isotonic
        regression.
        """
        return self._call_java("predictions")


class TreeEnsembleParams(DecisionTreeParams):
    """
    Mixin for Decision Tree-based ensemble algorithms parameters.
    """

    subsamplingRate = Param(Params._dummy(), "subsamplingRate", "Fraction of the training data " +
                            "used for learning each decision tree, in range (0, 1].",
                            typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super(TreeEnsembleParams, self).__init__()

    @since("1.4.0")
    def setSubsamplingRate(self, value):
        """
        Sets the value of :py:attr:`subsamplingRate`.
        """
        return self._set(subsamplingRate=value)

    @since("1.4.0")
    def getSubsamplingRate(self):
        """
        Gets the value of subsamplingRate or its default value.
        """
        return self.getOrDefault(self.subsamplingRate)


class TreeRegressorParams(Params):
    """
    Private class to track supported impurity measures.
    """

    supportedImpurities = ["variance"]
    impurity = Param(Params._dummy(), "impurity",
                     "Criterion used for information gain calculation (case-insensitive). " +
                     "Supported options: " +
                     ", ".join(supportedImpurities), typeConverter=TypeConverters.toString)

    def __init__(self):
        super(TreeRegressorParams, self).__init__()

    @since("1.4.0")
    def setImpurity(self, value):
        """
        Sets the value of :py:attr:`impurity`.
        """
        return self._set(impurity=value)

    @since("1.4.0")
    def getImpurity(self):
        """
        Gets the value of impurity or its default value.
        """
        return self.getOrDefault(self.impurity)


class RandomForestParams(TreeEnsembleParams):
    """
    Private class to track supported random forest parameters.
    """

    supportedFeatureSubsetStrategies = ["auto", "all", "onethird", "sqrt", "log2"]
    numTrees = Param(Params._dummy(), "numTrees", "Number of trees to train (>= 1).",
                     typeConverter=TypeConverters.toInt)
    featureSubsetStrategy = \
        Param(Params._dummy(), "featureSubsetStrategy",
              "The number of features to consider for splits at each tree node. Supported " +
              "options: " + ", ".join(supportedFeatureSubsetStrategies),
              typeConverter=TypeConverters.toString)

    def __init__(self):
        super(RandomForestParams, self).__init__()

    @since("1.4.0")
    def setNumTrees(self, value):
        """
        Sets the value of :py:attr:`numTrees`.
        """
        return self._set(numTrees=value)

    @since("1.4.0")
    def getNumTrees(self):
        """
        Gets the value of numTrees or its default value.
        """
        return self.getOrDefault(self.numTrees)

    @since("1.4.0")
    def setFeatureSubsetStrategy(self, value):
        """
        Sets the value of :py:attr:`featureSubsetStrategy`.
        """
        return self._set(featureSubsetStrategy=value)

    @since("1.4.0")
    def getFeatureSubsetStrategy(self):
        """
        Gets the value of featureSubsetStrategy or its default value.
        """
        return self.getOrDefault(self.featureSubsetStrategy)


class GBTParams(TreeEnsembleParams):
    """
    Private class to track supported GBT params.
    """
    supportedLossTypes = ["squared", "absolute"]


@inherit_doc
class DecisionTreeRegressor(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol,
                            DecisionTreeParams, TreeRegressorParams, HasCheckpointInterval,
                            HasSeed, JavaMLWritable, JavaMLReadable, HasVarianceCol):
    """
    `Decision tree <http://en.wikipedia.org/wiki/Decision_tree_learning>`_
    learning algorithm for regression.
    It supports both continuous and categorical features.

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> dt = DecisionTreeRegressor(maxDepth=2, varianceCol="variance")
    >>> model = dt.fit(df)
    >>> model.depth
    1
    >>> model.numNodes
    3
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    >>> dtr_path = temp_path + "/dtr"
    >>> dt.save(dtr_path)
    >>> dt2 = DecisionTreeRegressor.load(dtr_path)
    >>> dt2.getMaxDepth()
    2
    >>> model_path = temp_path + "/dtr_model"
    >>> model.save(model_path)
    >>> model2 = DecisionTreeRegressionModel.load(model_path)
    >>> model.numNodes == model2.numNodes
    True
    >>> model.depth == model2.depth
    True
    >>> model.transform(test1).head().variance
    0.0

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="variance",
                 seed=None, varianceCol=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                 impurity="variance", seed=None, varianceCol=None)
        """
        super(DecisionTreeRegressor, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.DecisionTreeRegressor", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         impurity="variance")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  impurity="variance", seed=None, varianceCol=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                  impurity="variance", seed=None, varianceCol=None)
        Sets params for the DecisionTreeRegressor.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return DecisionTreeRegressionModel(java_model)


@inherit_doc
class DecisionTreeModel(JavaModel):
    """Abstraction for Decision Tree models.

    .. versionadded:: 1.5.0
    """

    @property
    @since("1.5.0")
    def numNodes(self):
        """Return number of nodes of the decision tree."""
        return self._call_java("numNodes")

    @property
    @since("1.5.0")
    def depth(self):
        """Return depth of the decision tree."""
        return self._call_java("depth")

    def __repr__(self):
        return self._call_java("toString")


@inherit_doc
class TreeEnsembleModels(JavaModel):
    """Represents a tree ensemble model.

    .. versionadded:: 1.5.0
    """

    @property
    @since("1.5.0")
    def treeWeights(self):
        """Return the weights for each tree"""
        return list(self._call_java("javaTreeWeights"))

    def __repr__(self):
        return self._call_java("toString")


@inherit_doc
class DecisionTreeRegressionModel(DecisionTreeModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by DecisionTreeRegressor.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def featureImportances(self):
        """
        Estimate of the importance of each feature.

        This generalizes the idea of "Gini" importance to other losses,
        following the explanation of Gini importance from "Random Forests" documentation
        by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.

        This feature importance is calculated as follows:
          - importance(feature j) = sum (over nodes which split on feature j) of the gain,
            where gain is scaled by the number of instances passing through node
          - Normalize importances for tree to sum to 1.

        Note: Feature importance for single decision trees can have high variance due to
              correlated predictor variables. Consider using a :py:class:`RandomForestRegressor`
              to determine feature importance instead.
        """
        return self._call_java("featureImportances")


@inherit_doc
class RandomForestRegressor(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasSeed,
                            RandomForestParams, TreeRegressorParams, HasCheckpointInterval,
                            JavaMLWritable, JavaMLReadable):
    """
    `Random Forest <http://en.wikipedia.org/wiki/Random_forest>`_
    learning algorithm for regression.
    It supports both continuous and categorical features.

    >>> from numpy import allclose
    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> rf = RandomForestRegressor(numTrees=2, maxDepth=2, seed=42)
    >>> model = rf.fit(df)
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> allclose(model.treeWeights, [1.0, 1.0])
    True
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    0.5
    >>> rfr_path = temp_path + "/rfr"
    >>> rf.save(rfr_path)
    >>> rf2 = RandomForestRegressor.load(rfr_path)
    >>> rf2.getNumTrees()
    2
    >>> model_path = temp_path + "/rfr_model"
    >>> model.save(model_path)
    >>> model2 = RandomForestRegressionModel.load(model_path)
    >>> model.featureImportances == model2.featureImportances
    True

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                 impurity="variance", subsamplingRate=1.0, seed=None, numTrees=20,
                 featureSubsetStrategy="auto"):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                 impurity="variance", subsamplingRate=1.0, seed=None, numTrees=20, \
                 featureSubsetStrategy="auto")
        """
        super(RandomForestRegressor, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.RandomForestRegressor", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         impurity="variance", subsamplingRate=1.0, seed=None, numTrees=20,
                         featureSubsetStrategy="auto")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  impurity="variance", subsamplingRate=1.0, seed=None, numTrees=20,
                  featureSubsetStrategy="auto"):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                  impurity="variance", subsamplingRate=1.0, seed=None, numTrees=20, \
                  featureSubsetStrategy="auto")
        Sets params for linear regression.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return RandomForestRegressionModel(java_model)


class RandomForestRegressionModel(TreeEnsembleModels, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by RandomForestRegressor.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def featureImportances(self):
        """
        Estimate of the importance of each feature.

        Each feature's importance is the average of its importance across all trees in the ensemble
        The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
        (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
        and follows the implementation from scikit-learn.

        .. seealso:: :py:attr:`DecisionTreeRegressionModel.featureImportances`
        """
        return self._call_java("featureImportances")


@inherit_doc
class GBTRegressor(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                   GBTParams, HasCheckpointInterval, HasStepSize, HasSeed, JavaMLWritable,
                   JavaMLReadable):
    """
    `Gradient-Boosted Trees (GBTs) <http://en.wikipedia.org/wiki/Gradient_boosting>`_
    learning algorithm for regression.
    It supports both continuous and categorical features.

    >>> from numpy import allclose
    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> gbt = GBTRegressor(maxIter=5, maxDepth=2, seed=42)
    >>> model = gbt.fit(df)
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> allclose(model.treeWeights, [1.0, 0.1, 0.1, 0.1, 0.1])
    True
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    >>> gbtr_path = temp_path + "gbtr"
    >>> gbt.save(gbtr_path)
    >>> gbt2 = GBTRegressor.load(gbtr_path)
    >>> gbt2.getMaxDepth()
    2
    >>> model_path = temp_path + "gbtr_model"
    >>> model.save(model_path)
    >>> model2 = GBTRegressionModel.load(model_path)
    >>> model.featureImportances == model2.featureImportances
    True
    >>> model.treeWeights == model2.treeWeights
    True

    .. versionadded:: 1.4.0
    """

    lossType = Param(Params._dummy(), "lossType",
                     "Loss function which GBT tries to minimize (case-insensitive). " +
                     "Supported options: " + ", ".join(GBTParams.supportedLossTypes),
                     typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, subsamplingRate=1.0,
                 checkpointInterval=10, lossType="squared", maxIter=20, stepSize=0.1, seed=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, subsamplingRate=1.0, \
                 checkpointInterval=10, lossType="squared", maxIter=20, stepSize=0.1, seed=None)
        """
        super(GBTRegressor, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.regression.GBTRegressor", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, subsamplingRate=1.0,
                         checkpointInterval=10, lossType="squared", maxIter=20, stepSize=0.1,
                         seed=None)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, subsamplingRate=1.0,
                  checkpointInterval=10, lossType="squared", maxIter=20, stepSize=0.1, seed=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, subsamplingRate=1.0, \
                  checkpointInterval=10, lossType="squared", maxIter=20, stepSize=0.1, seed=None)
        Sets params for Gradient Boosted Tree Regression.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return GBTRegressionModel(java_model)

    @since("1.4.0")
    def setLossType(self, value):
        """
        Sets the value of :py:attr:`lossType`.
        """
        return self._set(lossType=value)

    @since("1.4.0")
    def getLossType(self):
        """
        Gets the value of lossType or its default value.
        """
        return self.getOrDefault(self.lossType)


class GBTRegressionModel(TreeEnsembleModels, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by GBTRegressor.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def featureImportances(self):
        """
        Estimate of the importance of each feature.

        Each feature's importance is the average of its importance across all trees in the ensemble
        The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
        (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
        and follows the implementation from scikit-learn.

        .. seealso:: :py:attr:`DecisionTreeRegressionModel.featureImportances`
        """
        return self._call_java("featureImportances")


@inherit_doc
class AFTSurvivalRegression(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol,
                            HasFitIntercept, HasMaxIter, HasTol, JavaMLWritable, JavaMLReadable):
    """
    Accelerated Failure Time (AFT) Model Survival Regression

    Fit a parametric AFT survival regression model based on the Weibull distribution
    of the survival time.

    .. seealso:: `AFT Model <https://en.wikipedia.org/wiki/Accelerated_failure_time_model>`_

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0), 1.0),
    ...     (0.0, Vectors.sparse(1, [], []), 0.0)], ["label", "features", "censor"])
    >>> aftsr = AFTSurvivalRegression()
    >>> model = aftsr.fit(df)
    >>> model.predict(Vectors.dense(6.3))
    1.0
    >>> model.predictQuantiles(Vectors.dense(6.3))
    DenseVector([0.0101, 0.0513, 0.1054, 0.2877, 0.6931, 1.3863, 2.3026, 2.9957, 4.6052])
    >>> model.transform(df).show()
    +-----+---------+------+----------+
    |label| features|censor|prediction|
    +-----+---------+------+----------+
    |  1.0|    [1.0]|   1.0|       1.0|
    |  0.0|(1,[],[])|   0.0|       1.0|
    +-----+---------+------+----------+
    ...
    >>> aftsr_path = temp_path + "/aftsr"
    >>> aftsr.save(aftsr_path)
    >>> aftsr2 = AFTSurvivalRegression.load(aftsr_path)
    >>> aftsr2.getMaxIter()
    100
    >>> model_path = temp_path + "/aftsr_model"
    >>> model.save(model_path)
    >>> model2 = AFTSurvivalRegressionModel.load(model_path)
    >>> model.coefficients == model2.coefficients
    True
    >>> model.intercept == model2.intercept
    True
    >>> model.scale == model2.scale
    True

    .. versionadded:: 1.6.0
    """

    censorCol = Param(Params._dummy(), "censorCol",
                      "censor column name. The value of this column could be 0 or 1. " +
                      "If the value is 1, it means the event has occurred i.e. " +
                      "uncensored; otherwise censored.", typeConverter=TypeConverters.toString)
    quantileProbabilities = \
        Param(Params._dummy(), "quantileProbabilities",
              "quantile probabilities array. Values of the quantile probabilities array " +
              "should be in the range (0, 1) and the array should be non-empty.",
              typeConverter=TypeConverters.toListFloat)
    quantilesCol = Param(Params._dummy(), "quantilesCol",
                         "quantiles column name. This column will output quantiles of " +
                         "corresponding quantileProbabilities if it is set.",
                         typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 fitIntercept=True, maxIter=100, tol=1E-6, censorCol="censor",
                 quantileProbabilities=list([0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]),
                 quantilesCol=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 fitIntercept=True, maxIter=100, tol=1E-6, censorCol="censor", \
                 quantileProbabilities=[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99], \
                 quantilesCol=None)
        """
        super(AFTSurvivalRegression, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.AFTSurvivalRegression", self.uid)
        self._setDefault(censorCol="censor",
                         quantileProbabilities=[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99],
                         maxIter=100, tol=1E-6)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  fitIntercept=True, maxIter=100, tol=1E-6, censorCol="censor",
                  quantileProbabilities=list([0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]),
                  quantilesCol=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  fitIntercept=True, maxIter=100, tol=1E-6, censorCol="censor", \
                  quantileProbabilities=[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99], \
                  quantilesCol=None):
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return AFTSurvivalRegressionModel(java_model)

    @since("1.6.0")
    def setCensorCol(self, value):
        """
        Sets the value of :py:attr:`censorCol`.
        """
        return self._set(censorCol=value)

    @since("1.6.0")
    def getCensorCol(self):
        """
        Gets the value of censorCol or its default value.
        """
        return self.getOrDefault(self.censorCol)

    @since("1.6.0")
    def setQuantileProbabilities(self, value):
        """
        Sets the value of :py:attr:`quantileProbabilities`.
        """
        return self._set(quantileProbabilities=value)

    @since("1.6.0")
    def getQuantileProbabilities(self):
        """
        Gets the value of quantileProbabilities or its default value.
        """
        return self.getOrDefault(self.quantileProbabilities)

    @since("1.6.0")
    def setQuantilesCol(self, value):
        """
        Sets the value of :py:attr:`quantilesCol`.
        """
        return self._set(quantilesCol=value)

    @since("1.6.0")
    def getQuantilesCol(self):
        """
        Gets the value of quantilesCol or its default value.
        """
        return self.getOrDefault(self.quantilesCol)


class AFTSurvivalRegressionModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by AFTSurvivalRegression.

    .. versionadded:: 1.6.0
    """

    @property
    @since("1.6.0")
    def coefficients(self):
        """
        Model coefficients.
        """
        return self._call_java("coefficients")

    @property
    @since("1.6.0")
    def intercept(self):
        """
        Model intercept.
        """
        return self._call_java("intercept")

    @property
    @since("1.6.0")
    def scale(self):
        """
        Model scale paramter.
        """
        return self._call_java("scale")

    def predictQuantiles(self, features):
        """
        Predicted Quantiles
        """
        return self._call_java("predictQuantiles", features)

    def predict(self, features):
        """
        Predicted value
        """
        return self._call_java("predict", features)


@inherit_doc
class GeneralizedLinearRegression(JavaEstimator, HasLabelCol, HasFeaturesCol, HasPredictionCol,
                                  HasFitIntercept, HasMaxIter, HasTol, HasRegParam, HasWeightCol,
                                  HasSolver, JavaMLWritable, JavaMLReadable):
    """
    Generalized Linear Regression.

    Fit a Generalized Linear Model specified by giving a symbolic description of the linear
    predictor (link function) and a description of the error distribution (family). It supports
    "gaussian", "binomial", "poisson" and "gamma" as family. Valid link functions for each family
    is listed below. The first link function of each family is the default one.
    - "gaussian" -> "identity", "log", "inverse"
    - "binomial" -> "logit", "probit", "cloglog"
    - "poisson"  -> "log", "identity", "sqrt"
    - "gamma"    -> "inverse", "identity", "log"

    .. seealso:: `GLM <https://en.wikipedia.org/wiki/Generalized_linear_model>`_

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(0.0, 0.0)),
    ...     (1.0, Vectors.dense(1.0, 2.0)),
    ...     (2.0, Vectors.dense(0.0, 0.0)),
    ...     (2.0, Vectors.dense(1.0, 1.0)),], ["label", "features"])
    >>> glr = GeneralizedLinearRegression(family="gaussian", link="identity")
    >>> model = glr.fit(df)
    >>> abs(model.transform(df).head().prediction - 1.5) < 0.001
    True
    >>> model.coefficients
    DenseVector([1.5..., -1.0...])
    >>> abs(model.intercept - 1.5) < 0.001
    True
    >>> glr_path = temp_path + "/glr"
    >>> glr.save(glr_path)
    >>> glr2 = GeneralizedLinearRegression.load(glr_path)
    >>> glr.getFamily() == glr2.getFamily()
    True
    >>> model_path = temp_path + "/glr_model"
    >>> model.save(model_path)
    >>> model2 = GeneralizedLinearRegressionModel.load(model_path)
    >>> model.intercept == model2.intercept
    True
    >>> model.coefficients[0] == model2.coefficients[0]
    True

    .. versionadded:: 2.0.0
    """

    family = Param(Params._dummy(), "family", "The name of family which is a description of " +
                   "the error distribution to be used in the model. Supported options: " +
                   "gaussian(default), binomial, poisson and gamma.",
                   typeConverter=TypeConverters.toString)
    link = Param(Params._dummy(), "link", "The name of link function which provides the " +
                 "relationship between the linear predictor and the mean of the distribution " +
                 "function. Supported options: identity, log, inverse, logit, probit, cloglog " +
                 "and sqrt.", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, labelCol="label", featuresCol="features", predictionCol="prediction",
                 family="gaussian", link=None, fitIntercept=True, maxIter=25, tol=1e-6,
                 regParam=0.0, weightCol=None, solver="irls"):
        """
        __init__(self, labelCol="label", featuresCol="features", predictionCol="prediction", \
                 family="gaussian", link=None, fitIntercept=True, maxIter=25, tol=1e-6, \
                 regParam=0.0, weightCol=None, solver="irls")
        """
        super(GeneralizedLinearRegression, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.GeneralizedLinearRegression", self.uid)
        self._setDefault(family="gaussian", maxIter=25, tol=1e-6, regParam=0.0, solver="irls")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.0.0")
    def setParams(self, labelCol="label", featuresCol="features", predictionCol="prediction",
                  family="gaussian", link=None, fitIntercept=True, maxIter=25, tol=1e-6,
                  regParam=0.0, weightCol=None, solver="irls"):
        """
        setParams(self, labelCol="label", featuresCol="features", predictionCol="prediction", \
                  family="gaussian", link=None, fitIntercept=True, maxIter=25, tol=1e-6, \
                  regParam=0.0, weightCol=None, solver="irls")
        Sets params for generalized linear regression.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return GeneralizedLinearRegressionModel(java_model)

    @since("2.0.0")
    def setFamily(self, value):
        """
        Sets the value of :py:attr:`family`.
        """
        return self._set(family=value)

    @since("2.0.0")
    def getFamily(self):
        """
        Gets the value of family or its default value.
        """
        return self.getOrDefault(self.family)

    @since("2.0.0")
    def setLink(self, value):
        """
        Sets the value of :py:attr:`link`.
        """
        return self._set(link=value)

    @since("2.0.0")
    def getLink(self):
        """
        Gets the value of link or its default value.
        """
        return self.getOrDefault(self.link)


class GeneralizedLinearRegressionModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by GeneralizedLinearRegression.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def coefficients(self):
        """
        Model coefficients.
        """
        return self._call_java("coefficients")

    @property
    @since("2.0.0")
    def intercept(self):
        """
        Model intercept.
        """
        return self._call_java("intercept")


if __name__ == "__main__":
    import doctest
    import pyspark.ml.regression
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = pyspark.ml.regression.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.regression tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    import tempfile
    temp_path = tempfile.mkdtemp()
    globs['temp_path'] = temp_path
    try:
        (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
        sc.stop()
    finally:
        from shutil import rmtree
        try:
            rmtree(temp_path)
        except OSError:
            pass
    if failure_count:
        exit(-1)
