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

from pyspark import since, keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.tree import _DecisionTreeModel, _DecisionTreeParams, \
    _TreeEnsembleModel, _TreeEnsembleParams, _RandomForestParams, _GBTParams, \
    _HasVarianceImpurity, _TreeRegressorParams
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaParams, \
    JavaPredictor, JavaPredictionModel, _JavaPredictorParams, JavaWrapper
from pyspark.ml.common import inherit_doc
from pyspark.sql import DataFrame


__all__ = ['AFTSurvivalRegression', 'AFTSurvivalRegressionModel',
           'DecisionTreeRegressor', 'DecisionTreeRegressionModel',
           'GBTRegressor', 'GBTRegressionModel',
           'GeneralizedLinearRegression', 'GeneralizedLinearRegressionModel',
           'GeneralizedLinearRegressionSummary', 'GeneralizedLinearRegressionTrainingSummary',
           'IsotonicRegression', 'IsotonicRegressionModel',
           'LinearRegression', 'LinearRegressionModel',
           'LinearRegressionSummary', 'LinearRegressionTrainingSummary',
           'RandomForestRegressor', 'RandomForestRegressionModel',
           'FMRegressor', 'FMRegressionModel']


class JavaRegressor(JavaPredictor, _JavaPredictorParams):
    """
    Java Regressor for regression tasks.

    .. versionadded:: 3.0.0
    """
    pass


class JavaRegressionModel(JavaPredictionModel, _JavaPredictorParams):
    """
    Java Model produced by a ``_JavaRegressor``.
    To be mixed in with :class:`pyspark.ml.JavaModel`

    .. versionadded:: 3.0.0
    """
    pass


class _LinearRegressionParams(_JavaPredictorParams, HasRegParam, HasElasticNetParam, HasMaxIter,
                              HasTol, HasFitIntercept, HasStandardization, HasWeightCol, HasSolver,
                              HasAggregationDepth, HasLoss):
    """
    Params for :py:class:`LinearRegression` and :py:class:`LinearRegressionModel`.

    .. versionadded:: 3.0.0
    """

    solver = Param(Params._dummy(), "solver", "The solver algorithm for optimization. Supported " +
                   "options: auto, normal, l-bfgs.", typeConverter=TypeConverters.toString)

    loss = Param(Params._dummy(), "loss", "The loss function to be optimized. Supported " +
                 "options: squaredError, huber.", typeConverter=TypeConverters.toString)

    epsilon = Param(Params._dummy(), "epsilon", "The shape parameter to control the amount of " +
                    "robustness. Must be > 1.0. Only valid when loss is huber",
                    typeConverter=TypeConverters.toFloat)

    @since("2.3.0")
    def getEpsilon(self):
        """
        Gets the value of epsilon or its default value.
        """
        return self.getOrDefault(self.epsilon)


@inherit_doc
class LinearRegression(JavaRegressor, _LinearRegressionParams, JavaMLWritable, JavaMLReadable):
    """
    Linear regression.

    The learning objective is to minimize the specified loss function, with regularization.
    This supports two kinds of loss:

    * squaredError (a.k.a squared loss)
    * huber (a hybrid of squared error for relatively small errors and absolute error for \
    relatively large ones, and we estimate the scale parameter from training data)

    This supports multiple types of regularization:

    * none (a.k.a. ordinary least squares)
    * L2 (ridge regression)
    * L1 (Lasso)
    * L2 + L1 (elastic net)

    Note: Fitting with huber loss only supports none and L2 regularization.

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     (1.0, 2.0, Vectors.dense(1.0)),
    ...     (0.0, 2.0, Vectors.sparse(1, [], []))], ["label", "weight", "features"])
    >>> lr = LinearRegression(regParam=0.0, solver="normal", weightCol="weight")
    >>> lr.setMaxIter(5)
    LinearRegression...
    >>> lr.getMaxIter()
    5
    >>> lr.setRegParam(0.1)
    LinearRegression...
    >>> lr.getRegParam()
    0.1
    >>> lr.setRegParam(0.0)
    LinearRegression...
    >>> model = lr.fit(df)
    >>> model.setFeaturesCol("features")
    LinearRegressionModel...
    >>> model.setPredictionCol("newPrediction")
    LinearRegressionModel...
    >>> model.getMaxIter()
    5
    >>> test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> abs(model.predict(test0.head().features) - (-1.0)) < 0.001
    True
    >>> abs(model.transform(test0).head().newPrediction - (-1.0)) < 0.001
    True
    >>> abs(model.coefficients[0] - 1.0) < 0.001
    True
    >>> abs(model.intercept - 0.0) < 0.001
    True
    >>> test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> abs(model.transform(test1).head().newPrediction - 1.0) < 0.001
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
    >>> model.numFeatures
    1
    >>> model.write().format("pmml").save(model_path + "_2")

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True,
                 standardization=True, solver="auto", weightCol=None, aggregationDepth=2,
                 loss="squaredError", epsilon=1.35):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                 standardization=True, solver="auto", weightCol=None, aggregationDepth=2, \
                 loss="squaredError", epsilon=1.35)
        """
        super(LinearRegression, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.LinearRegression", self.uid)
        self._setDefault(maxIter=100, regParam=0.0, tol=1e-6, loss="squaredError", epsilon=1.35)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True,
                  standardization=True, solver="auto", weightCol=None, aggregationDepth=2,
                  loss="squaredError", epsilon=1.35):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                  standardization=True, solver="auto", weightCol=None, aggregationDepth=2, \
                  loss="squaredError", epsilon=1.35)
        Sets params for linear regression.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return LinearRegressionModel(java_model)

    @since("2.3.0")
    def setEpsilon(self, value):
        """
        Sets the value of :py:attr:`epsilon`.
        """
        return self._set(epsilon=value)

    def setMaxIter(self, value):
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    def setRegParam(self, value):
        """
        Sets the value of :py:attr:`regParam`.
        """
        return self._set(regParam=value)

    def setTol(self, value):
        """
        Sets the value of :py:attr:`tol`.
        """
        return self._set(tol=value)

    def setElasticNetParam(self, value):
        """
        Sets the value of :py:attr:`elasticNetParam`.
        """
        return self._set(elasticNetParam=value)

    def setFitIntercept(self, value):
        """
        Sets the value of :py:attr:`fitIntercept`.
        """
        return self._set(fitIntercept=value)

    def setStandardization(self, value):
        """
        Sets the value of :py:attr:`standardization`.
        """
        return self._set(standardization=value)

    def setWeightCol(self, value):
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    def setSolver(self, value):
        """
        Sets the value of :py:attr:`solver`.
        """
        return self._set(solver=value)

    def setAggregationDepth(self, value):
        """
        Sets the value of :py:attr:`aggregationDepth`.
        """
        return self._set(aggregationDepth=value)

    def setLoss(self, value):
        """
        Sets the value of :py:attr:`loss`.
        """
        return self._set(lossType=value)


class LinearRegressionModel(JavaRegressionModel, _LinearRegressionParams, GeneralJavaMLWritable,
                            JavaMLReadable, HasTrainingSummary):
    """
    Model fitted by :class:`LinearRegression`.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
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
    @since("2.3.0")
    def scale(self):
        r"""
        The value by which :math:`\|y - X'w\|` is scaled down when loss is "huber", otherwise 1.0.
        """
        return self._call_java("scale")

    @property
    @since("2.0.0")
    def summary(self):
        """
        Gets summary (e.g. residuals, mse, r-squared ) of model on
        training set. An exception is thrown if
        `trainingSummary is None`.
        """
        if self.hasSummary:
            return LinearRegressionTrainingSummary(super(LinearRegressionModel, self).summary)
        else:
            raise RuntimeError("No training summary available for this %s" %
                               self.__class__.__name__)

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
        r"""
        Returns the explained variance regression score.
        explainedVariance = :math:`1 - \frac{variance(y - \hat{y})}{variance(y)}`

        .. seealso:: `Wikipedia explain variation
            <http://en.wikipedia.org/wiki/Explained_variation>`_

        .. note:: This ignores instance weights (setting all to 1.0) from
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

        .. note:: This ignores instance weights (setting all to 1.0) from
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

        .. note:: This ignores instance weights (setting all to 1.0) from
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

        .. note:: This ignores instance weights (setting all to 1.0) from
            `LinearRegression.weightCol`. This will change in later Spark
            versions.
        """
        return self._call_java("rootMeanSquaredError")

    @property
    @since("2.0.0")
    def r2(self):
        """
        Returns R^2, the coefficient of determination.

        .. seealso:: `Wikipedia coefficient of determination
            <http://en.wikipedia.org/wiki/Coefficient_of_determination>`_

        .. note:: This ignores instance weights (setting all to 1.0) from
            `LinearRegression.weightCol`. This will change in later Spark
            versions.
        """
        return self._call_java("r2")

    @property
    @since("2.4.0")
    def r2adj(self):
        """
        Returns Adjusted R^2, the adjusted coefficient of determination.

        .. seealso:: `Wikipedia coefficient of determination, Adjusted R^2
            <https://en.wikipedia.org/wiki/Coefficient_of_determination#Adjusted_R2>`_

        .. note:: This ignores instance weights (setting all to 1.0) from
            `LinearRegression.weightCol`. This will change in later Spark versions.
        """
        return self._call_java("r2adj")

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
    @since("2.2.0")
    def degreesOfFreedom(self):
        """
        Degrees of freedom.
        """
        return self._call_java("degreesOfFreedom")

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


class _IsotonicRegressionParams(HasFeaturesCol, HasLabelCol, HasPredictionCol, HasWeightCol):
    """
    Params for :py:class:`IsotonicRegression` and :py:class:`IsotonicRegressionModel`.

    .. versionadded:: 3.0.0
    """

    isotonic = Param(
        Params._dummy(), "isotonic",
        "whether the output sequence should be isotonic/increasing (true) or" +
        "antitonic/decreasing (false).", typeConverter=TypeConverters.toBoolean)
    featureIndex = Param(
        Params._dummy(), "featureIndex",
        "The index of the feature if featuresCol is a vector column, no effect otherwise.",
        typeConverter=TypeConverters.toInt)

    def getIsotonic(self):
        """
        Gets the value of isotonic or its default value.
        """
        return self.getOrDefault(self.isotonic)

    def getFeatureIndex(self):
        """
        Gets the value of featureIndex or its default value.
        """
        return self.getOrDefault(self.featureIndex)


@inherit_doc
class IsotonicRegression(JavaEstimator, _IsotonicRegressionParams, HasWeightCol,
                         JavaMLWritable, JavaMLReadable):
    """
    Currently implemented using parallelized pool adjacent violators algorithm.
    Only univariate (single feature) algorithm supported.

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> ir = IsotonicRegression()
    >>> model = ir.fit(df)
    >>> model.setFeaturesCol("features")
    IsotonicRegressionModel...
    >>> model.numFeatures
    1
    >>> test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> model.predict(test0.head().features[model.getFeatureIndex()])
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

    .. versionadded:: 1.6.0
    """
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
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  weightCol=None, isotonic=True, featureIndex=0):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 weightCol=None, isotonic=True, featureIndex=0):
        Set the params for IsotonicRegression.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return IsotonicRegressionModel(java_model)

    def setIsotonic(self, value):
        """
        Sets the value of :py:attr:`isotonic`.
        """
        return self._set(isotonic=value)

    def setFeatureIndex(self, value):
        """
        Sets the value of :py:attr:`featureIndex`.
        """
        return self._set(featureIndex=value)

    @since("1.6.0")
    def setFeaturesCol(self, value):
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("1.6.0")
    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @since("1.6.0")
    def setLabelCol(self, value):
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)

    @since("1.6.0")
    def setWeightCol(self, value):
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)


class IsotonicRegressionModel(JavaModel, _IsotonicRegressionParams, JavaMLWritable,
                              JavaMLReadable):
    """
    Model fitted by :class:`IsotonicRegression`.

    .. versionadded:: 1.6.0
    """

    @since("3.0.0")
    def setFeaturesCol(self, value):
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("3.0.0")
    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    def setFeatureIndex(self, value):
        """
        Sets the value of :py:attr:`featureIndex`.
        """
        return self._set(featureIndex=value)

    @property
    @since("1.6.0")
    def boundaries(self):
        """
        Boundaries in increasing order for which predictions are known.
        """
        return self._call_java("boundaries")

    @property
    @since("1.6.0")
    def predictions(self):
        """
        Predictions associated with the boundaries at the same index, monotone because of isotonic
        regression.
        """
        return self._call_java("predictions")

    @property
    @since("3.0.0")
    def numFeatures(self):
        """
        Returns the number of features the model was trained on. If unknown, returns -1
        """
        return self._call_java("numFeatures")

    @since("3.0.0")
    def predict(self, value):
        """
        Predict label for the given features.
        """
        return self._call_java("predict", value)


class _DecisionTreeRegressorParams(_DecisionTreeParams, _TreeRegressorParams, HasVarianceCol):
    """
    Params for :py:class:`DecisionTreeRegressor` and :py:class:`DecisionTreeRegressionModel`.

    .. versionadded:: 3.0.0
    """

    pass


@inherit_doc
class DecisionTreeRegressor(JavaRegressor, _DecisionTreeRegressorParams, JavaMLWritable,
                            JavaMLReadable):
    """
    `Decision tree <http://en.wikipedia.org/wiki/Decision_tree_learning>`_
    learning algorithm for regression.
    It supports both continuous and categorical features.

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> dt = DecisionTreeRegressor(maxDepth=2)
    >>> dt.setVarianceCol("variance")
    DecisionTreeRegressor...
    >>> model = dt.fit(df)
    >>> model.getVarianceCol()
    'variance'
    >>> model.setLeafCol("leafId")
    DecisionTreeRegressionModel...
    >>> model.depth
    1
    >>> model.numNodes
    3
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> model.numFeatures
    1
    >>> test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.predict(test0.head().features)
    0.0
    >>> result = model.transform(test0).head()
    >>> result.prediction
    0.0
    >>> model.predictLeaf(test0.head().features)
    0.0
    >>> result.leafId
    0.0
    >>> test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
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

    >>> df3 = spark.createDataFrame([
    ...     (1.0, 0.2, Vectors.dense(1.0)),
    ...     (1.0, 0.8, Vectors.dense(1.0)),
    ...     (0.0, 1.0, Vectors.sparse(1, [], []))], ["label", "weight", "features"])
    >>> dt3 = DecisionTreeRegressor(maxDepth=2, weightCol="weight", varianceCol="variance")
    >>> model3 = dt3.fit(df3)
    >>> print(model3.toDebugString)
    DecisionTreeRegressionModel...depth=1, numNodes=3...

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="variance",
                 seed=None, varianceCol=None, weightCol=None, leafCol="",
                 minWeightFractionPerNode=0.0):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                 impurity="variance", seed=None, varianceCol=None, weightCol=None, \
                 leafCol="", minWeightFractionPerNode=0.0)
        """
        super(DecisionTreeRegressor, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.DecisionTreeRegressor", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         impurity="variance", leafCol="", minWeightFractionPerNode=0.0)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  impurity="variance", seed=None, varianceCol=None, weightCol=None,
                  leafCol="", minWeightFractionPerNode=0.0):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                  impurity="variance", seed=None, varianceCol=None, weightCol=None, \
                  leafCol="", minWeightFractionPerNode=0.0)
        Sets params for the DecisionTreeRegressor.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return DecisionTreeRegressionModel(java_model)

    @since("1.4.0")
    def setMaxDepth(self, value):
        """
        Sets the value of :py:attr:`maxDepth`.
        """
        return self._set(maxDepth=value)

    @since("1.4.0")
    def setMaxBins(self, value):
        """
        Sets the value of :py:attr:`maxBins`.
        """
        return self._set(maxBins=value)

    @since("1.4.0")
    def setMinInstancesPerNode(self, value):
        """
        Sets the value of :py:attr:`minInstancesPerNode`.
        """
        return self._set(minInstancesPerNode=value)

    @since("3.0.0")
    def setMinWeightFractionPerNode(self, value):
        """
        Sets the value of :py:attr:`minWeightFractionPerNode`.
        """
        return self._set(minWeightFractionPerNode=value)

    @since("1.4.0")
    def setMinInfoGain(self, value):
        """
        Sets the value of :py:attr:`minInfoGain`.
        """
        return self._set(minInfoGain=value)

    @since("1.4.0")
    def setMaxMemoryInMB(self, value):
        """
        Sets the value of :py:attr:`maxMemoryInMB`.
        """
        return self._set(maxMemoryInMB=value)

    @since("1.4.0")
    def setCacheNodeIds(self, value):
        """
        Sets the value of :py:attr:`cacheNodeIds`.
        """
        return self._set(cacheNodeIds=value)

    @since("1.4.0")
    def setImpurity(self, value):
        """
        Sets the value of :py:attr:`impurity`.
        """
        return self._set(impurity=value)

    @since("1.4.0")
    def setCheckpointInterval(self, value):
        """
        Sets the value of :py:attr:`checkpointInterval`.
        """
        return self._set(checkpointInterval=value)

    def setSeed(self, value):
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("3.0.0")
    def setWeightCol(self, value):
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @since("2.0.0")
    def setVarianceCol(self, value):
        """
        Sets the value of :py:attr:`varianceCol`.
        """
        return self._set(varianceCol=value)


@inherit_doc
class DecisionTreeRegressionModel(
    JavaRegressionModel, _DecisionTreeModel, _DecisionTreeRegressorParams,
    JavaMLWritable, JavaMLReadable
):
    """
    Model fitted by :class:`DecisionTreeRegressor`.

    .. versionadded:: 1.4.0
    """

    @since("3.0.0")
    def setVarianceCol(self, value):
        """
        Sets the value of :py:attr:`varianceCol`.
        """
        return self._set(varianceCol=value)

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

        .. note:: Feature importance for single decision trees can have high variance due to
              correlated predictor variables. Consider using a :py:class:`RandomForestRegressor`
              to determine feature importance instead.
        """
        return self._call_java("featureImportances")


class _RandomForestRegressorParams(_RandomForestParams, _TreeRegressorParams):
    """
    Params for :py:class:`RandomForestRegressor` and :py:class:`RandomForestRegressionModel`.

    .. versionadded:: 3.0.0
    """
    pass


@inherit_doc
class RandomForestRegressor(JavaRegressor, _RandomForestRegressorParams, JavaMLWritable,
                            JavaMLReadable):
    """
    `Random Forest <http://en.wikipedia.org/wiki/Random_forest>`_
    learning algorithm for regression.
    It supports both continuous and categorical features.

    >>> from numpy import allclose
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> rf = RandomForestRegressor(numTrees=2, maxDepth=2)
    >>> rf.getMinWeightFractionPerNode()
    0.0
    >>> rf.setSeed(42)
    RandomForestRegressor...
    >>> model = rf.fit(df)
    >>> model.getBootstrap()
    True
    >>> model.getSeed()
    42
    >>> model.setLeafCol("leafId")
    RandomForestRegressionModel...
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> allclose(model.treeWeights, [1.0, 1.0])
    True
    >>> test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.predict(test0.head().features)
    0.0
    >>> model.predictLeaf(test0.head().features)
    DenseVector([0.0, 0.0])
    >>> result = model.transform(test0).head()
    >>> result.prediction
    0.0
    >>> result.leafId
    DenseVector([0.0, 0.0])
    >>> model.numFeatures
    1
    >>> model.trees
    [DecisionTreeRegressionModel...depth=..., DecisionTreeRegressionModel...]
    >>> model.getNumTrees
    2
    >>> test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
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
                 featureSubsetStrategy="auto", leafCol="", minWeightFractionPerNode=0.0,
                 weightCol=None, bootstrap=True):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                 impurity="variance", subsamplingRate=1.0, seed=None, numTrees=20, \
                 featureSubsetStrategy="auto", leafCol=", minWeightFractionPerNode=0.0", \
                 weightCol=None, bootstrap=True)
        """
        super(RandomForestRegressor, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.RandomForestRegressor", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         impurity="variance", subsamplingRate=1.0, numTrees=20,
                         featureSubsetStrategy="auto", leafCol="", minWeightFractionPerNode=0.0,
                         bootstrap=True)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  impurity="variance", subsamplingRate=1.0, seed=None, numTrees=20,
                  featureSubsetStrategy="auto", leafCol="", minWeightFractionPerNode=0.0,
                  weightCol=None, bootstrap=True):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                  impurity="variance", subsamplingRate=1.0, seed=None, numTrees=20, \
                  featureSubsetStrategy="auto", leafCol="", minWeightFractionPerNode=0.0, \
                  weightCol=None, bootstrap=True)
        Sets params for linear regression.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return RandomForestRegressionModel(java_model)

    def setMaxDepth(self, value):
        """
        Sets the value of :py:attr:`maxDepth`.
        """
        return self._set(maxDepth=value)

    def setMaxBins(self, value):
        """
        Sets the value of :py:attr:`maxBins`.
        """
        return self._set(maxBins=value)

    def setMinInstancesPerNode(self, value):
        """
        Sets the value of :py:attr:`minInstancesPerNode`.
        """
        return self._set(minInstancesPerNode=value)

    def setMinInfoGain(self, value):
        """
        Sets the value of :py:attr:`minInfoGain`.
        """
        return self._set(minInfoGain=value)

    def setMaxMemoryInMB(self, value):
        """
        Sets the value of :py:attr:`maxMemoryInMB`.
        """
        return self._set(maxMemoryInMB=value)

    def setCacheNodeIds(self, value):
        """
        Sets the value of :py:attr:`cacheNodeIds`.
        """
        return self._set(cacheNodeIds=value)

    @since("1.4.0")
    def setImpurity(self, value):
        """
        Sets the value of :py:attr:`impurity`.
        """
        return self._set(impurity=value)

    @since("1.4.0")
    def setNumTrees(self, value):
        """
        Sets the value of :py:attr:`numTrees`.
        """
        return self._set(numTrees=value)

    @since("3.0.0")
    def setBootstrap(self, value):
        """
        Sets the value of :py:attr:`bootstrap`.
        """
        return self._set(bootstrap=value)

    @since("1.4.0")
    def setSubsamplingRate(self, value):
        """
        Sets the value of :py:attr:`subsamplingRate`.
        """
        return self._set(subsamplingRate=value)

    @since("2.4.0")
    def setFeatureSubsetStrategy(self, value):
        """
        Sets the value of :py:attr:`featureSubsetStrategy`.
        """
        return self._set(featureSubsetStrategy=value)

    def setCheckpointInterval(self, value):
        """
        Sets the value of :py:attr:`checkpointInterval`.
        """
        return self._set(checkpointInterval=value)

    def setSeed(self, value):
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("3.0.0")
    def setWeightCol(self, value):
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @since("3.0.0")
    def setMinWeightFractionPerNode(self, value):
        """
        Sets the value of :py:attr:`minWeightFractionPerNode`.
        """
        return self._set(minWeightFractionPerNode=value)


class RandomForestRegressionModel(
    JavaRegressionModel, _TreeEnsembleModel, _RandomForestRegressorParams,
    JavaMLWritable, JavaMLReadable
):
    """
    Model fitted by :class:`RandomForestRegressor`.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def trees(self):
        """Trees in this ensemble. Warning: These have null parent Estimators."""
        return [DecisionTreeRegressionModel(m) for m in list(self._call_java("trees"))]

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


class _GBTRegressorParams(_GBTParams, _TreeRegressorParams):
    """
    Params for :py:class:`GBTRegressor` and :py:class:`GBTRegressorModel`.

    .. versionadded:: 3.0.0
    """

    supportedLossTypes = ["squared", "absolute"]

    lossType = Param(Params._dummy(), "lossType",
                     "Loss function which GBT tries to minimize (case-insensitive). " +
                     "Supported options: " + ", ".join(supportedLossTypes),
                     typeConverter=TypeConverters.toString)

    @since("1.4.0")
    def getLossType(self):
        """
        Gets the value of lossType or its default value.
        """
        return self.getOrDefault(self.lossType)


@inherit_doc
class GBTRegressor(JavaRegressor, _GBTRegressorParams, JavaMLWritable, JavaMLReadable):
    """
    `Gradient-Boosted Trees (GBTs) <http://en.wikipedia.org/wiki/Gradient_boosting>`_
    learning algorithm for regression.
    It supports both continuous and categorical features.

    >>> from numpy import allclose
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> gbt = GBTRegressor(maxDepth=2, seed=42, leafCol="leafId")
    >>> gbt.setMaxIter(5)
    GBTRegressor...
    >>> gbt.setMinWeightFractionPerNode(0.049)
    GBTRegressor...
    >>> gbt.getMaxIter()
    5
    >>> print(gbt.getImpurity())
    variance
    >>> print(gbt.getFeatureSubsetStrategy())
    all
    >>> model = gbt.fit(df)
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> model.numFeatures
    1
    >>> allclose(model.treeWeights, [1.0, 0.1, 0.1, 0.1, 0.1])
    True
    >>> test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.predict(test0.head().features)
    0.0
    >>> model.predictLeaf(test0.head().features)
    DenseVector([0.0, 0.0, 0.0, 0.0, 0.0])
    >>> result = model.transform(test0).head()
    >>> result.prediction
    0.0
    >>> result.leafId
    DenseVector([0.0, 0.0, 0.0, 0.0, 0.0])
    >>> test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
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
    >>> model.trees
    [DecisionTreeRegressionModel...depth=..., DecisionTreeRegressionModel...]
    >>> validation = spark.createDataFrame([(0.0, Vectors.dense(-1.0))],
    ...              ["label", "features"])
    >>> model.evaluateEachIteration(validation, "squared")
    [0.0, 0.0, 0.0, 0.0, 0.0]
    >>> gbt = gbt.setValidationIndicatorCol("validationIndicator")
    >>> gbt.getValidationIndicatorCol()
    'validationIndicator'
    >>> gbt.getValidationTol()
    0.01

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, subsamplingRate=1.0,
                 checkpointInterval=10, lossType="squared", maxIter=20, stepSize=0.1, seed=None,
                 impurity="variance", featureSubsetStrategy="all", validationTol=0.01,
                 validationIndicatorCol=None, leafCol="", minWeightFractionPerNode=0.0,
                 weightCol=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, subsamplingRate=1.0, \
                 checkpointInterval=10, lossType="squared", maxIter=20, stepSize=0.1, seed=None, \
                 impurity="variance", featureSubsetStrategy="all", validationTol=0.01, \
                 validationIndicatorCol=None, leafCol="", minWeightFractionPerNode=0.0,
                 weightCol=None)
        """
        super(GBTRegressor, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.regression.GBTRegressor", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, subsamplingRate=1.0,
                         checkpointInterval=10, lossType="squared", maxIter=20, stepSize=0.1,
                         impurity="variance", featureSubsetStrategy="all", validationTol=0.01,
                         leafCol="", minWeightFractionPerNode=0.0)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, subsamplingRate=1.0,
                  checkpointInterval=10, lossType="squared", maxIter=20, stepSize=0.1, seed=None,
                  impuriy="variance", featureSubsetStrategy="all", validationTol=0.01,
                  validationIndicatorCol=None, leafCol="", minWeightFractionPerNode=0.0,
                  weightCol=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, subsamplingRate=1.0, \
                  checkpointInterval=10, lossType="squared", maxIter=20, stepSize=0.1, seed=None, \
                  impurity="variance", featureSubsetStrategy="all", validationTol=0.01, \
                  validationIndicatorCol=None, leafCol="", minWeightFractionPerNode=0.0, \
                  weightCol=None)
        Sets params for Gradient Boosted Tree Regression.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return GBTRegressionModel(java_model)

    @since("1.4.0")
    def setMaxDepth(self, value):
        """
        Sets the value of :py:attr:`maxDepth`.
        """
        return self._set(maxDepth=value)

    @since("1.4.0")
    def setMaxBins(self, value):
        """
        Sets the value of :py:attr:`maxBins`.
        """
        return self._set(maxBins=value)

    @since("1.4.0")
    def setMinInstancesPerNode(self, value):
        """
        Sets the value of :py:attr:`minInstancesPerNode`.
        """
        return self._set(minInstancesPerNode=value)

    @since("1.4.0")
    def setMinInfoGain(self, value):
        """
        Sets the value of :py:attr:`minInfoGain`.
        """
        return self._set(minInfoGain=value)

    @since("1.4.0")
    def setMaxMemoryInMB(self, value):
        """
        Sets the value of :py:attr:`maxMemoryInMB`.
        """
        return self._set(maxMemoryInMB=value)

    @since("1.4.0")
    def setCacheNodeIds(self, value):
        """
        Sets the value of :py:attr:`cacheNodeIds`.
        """
        return self._set(cacheNodeIds=value)

    @since("1.4.0")
    def setImpurity(self, value):
        """
        Sets the value of :py:attr:`impurity`.
        """
        return self._set(impurity=value)

    @since("1.4.0")
    def setLossType(self, value):
        """
        Sets the value of :py:attr:`lossType`.
        """
        return self._set(lossType=value)

    @since("1.4.0")
    def setSubsamplingRate(self, value):
        """
        Sets the value of :py:attr:`subsamplingRate`.
        """
        return self._set(subsamplingRate=value)

    @since("2.4.0")
    def setFeatureSubsetStrategy(self, value):
        """
        Sets the value of :py:attr:`featureSubsetStrategy`.
        """
        return self._set(featureSubsetStrategy=value)

    @since("3.0.0")
    def setValidationIndicatorCol(self, value):
        """
        Sets the value of :py:attr:`validationIndicatorCol`.
        """
        return self._set(validationIndicatorCol=value)

    @since("1.4.0")
    def setMaxIter(self, value):
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("1.4.0")
    def setCheckpointInterval(self, value):
        """
        Sets the value of :py:attr:`checkpointInterval`.
        """
        return self._set(checkpointInterval=value)

    @since("1.4.0")
    def setSeed(self, value):
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("1.4.0")
    def setStepSize(self, value):
        """
        Sets the value of :py:attr:`stepSize`.
        """
        return self._set(stepSize=value)

    @since("3.0.0")
    def setWeightCol(self, value):
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @since("3.0.0")
    def setMinWeightFractionPerNode(self, value):
        """
        Sets the value of :py:attr:`minWeightFractionPerNode`.
        """
        return self._set(minWeightFractionPerNode=value)


class GBTRegressionModel(
    JavaRegressionModel, _TreeEnsembleModel, _GBTRegressorParams,
    JavaMLWritable, JavaMLReadable
):
    """
    Model fitted by :class:`GBTRegressor`.

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

    @property
    @since("2.0.0")
    def trees(self):
        """Trees in this ensemble. Warning: These have null parent Estimators."""
        return [DecisionTreeRegressionModel(m) for m in list(self._call_java("trees"))]

    @since("2.4.0")
    def evaluateEachIteration(self, dataset, loss):
        """
        Method to compute error or loss for every iteration of gradient boosting.

        :param dataset:
            Test dataset to evaluate model on, where dataset is an
            instance of :py:class:`pyspark.sql.DataFrame`
        :param loss:
            The loss function used to compute error.
            Supported options: squared, absolute
        """
        return self._call_java("evaluateEachIteration", dataset, loss)


class _AFTSurvivalRegressionParams(_JavaPredictorParams, HasMaxIter, HasTol, HasFitIntercept,
                                   HasAggregationDepth):
    """
    Params for :py:class:`AFTSurvivalRegression` and :py:class:`AFTSurvivalRegressionModel`.

    .. versionadded:: 3.0.0
    """

    censorCol = Param(
        Params._dummy(), "censorCol",
        "censor column name. The value of this column could be 0 or 1. " +
        "If the value is 1, it means the event has occurred i.e. " +
        "uncensored; otherwise censored.", typeConverter=TypeConverters.toString)
    quantileProbabilities = Param(
        Params._dummy(), "quantileProbabilities",
        "quantile probabilities array. Values of the quantile probabilities array " +
        "should be in the range (0, 1) and the array should be non-empty.",
        typeConverter=TypeConverters.toListFloat)
    quantilesCol = Param(
        Params._dummy(), "quantilesCol",
        "quantiles column name. This column will output quantiles of " +
        "corresponding quantileProbabilities if it is set.",
        typeConverter=TypeConverters.toString)

    @since("1.6.0")
    def getCensorCol(self):
        """
        Gets the value of censorCol or its default value.
        """
        return self.getOrDefault(self.censorCol)

    @since("1.6.0")
    def getQuantileProbabilities(self):
        """
        Gets the value of quantileProbabilities or its default value.
        """
        return self.getOrDefault(self.quantileProbabilities)

    @since("1.6.0")
    def getQuantilesCol(self):
        """
        Gets the value of quantilesCol or its default value.
        """
        return self.getOrDefault(self.quantilesCol)


@inherit_doc
class AFTSurvivalRegression(JavaRegressor, _AFTSurvivalRegressionParams,
                            JavaMLWritable, JavaMLReadable):
    """
    Accelerated Failure Time (AFT) Model Survival Regression

    Fit a parametric AFT survival regression model based on the Weibull distribution
    of the survival time.

    .. seealso:: `AFT Model <https://en.wikipedia.org/wiki/Accelerated_failure_time_model>`_

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0), 1.0),
    ...     (1e-40, Vectors.sparse(1, [], []), 0.0)], ["label", "features", "censor"])
    >>> aftsr = AFTSurvivalRegression()
    >>> aftsr.setMaxIter(10)
    AFTSurvivalRegression...
    >>> aftsr.getMaxIter()
    10
    >>> aftsr.clear(aftsr.maxIter)
    >>> model = aftsr.fit(df)
    >>> model.setFeaturesCol("features")
    AFTSurvivalRegressionModel...
    >>> model.predict(Vectors.dense(6.3))
    1.0
    >>> model.predictQuantiles(Vectors.dense(6.3))
    DenseVector([0.0101, 0.0513, 0.1054, 0.2877, 0.6931, 1.3863, 2.3026, 2.9957, 4.6052])
    >>> model.transform(df).show()
    +-------+---------+------+----------+
    |  label| features|censor|prediction|
    +-------+---------+------+----------+
    |    1.0|    [1.0]|   1.0|       1.0|
    |1.0E-40|(1,[],[])|   0.0|       1.0|
    +-------+---------+------+----------+
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

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 fitIntercept=True, maxIter=100, tol=1E-6, censorCol="censor",
                 quantileProbabilities=list([0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]),
                 quantilesCol=None, aggregationDepth=2):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 fitIntercept=True, maxIter=100, tol=1E-6, censorCol="censor", \
                 quantileProbabilities=[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99], \
                 quantilesCol=None, aggregationDepth=2)
        """
        super(AFTSurvivalRegression, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.AFTSurvivalRegression", self.uid)
        self._setDefault(censorCol="censor",
                         quantileProbabilities=[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99],
                         maxIter=100, tol=1E-6)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  fitIntercept=True, maxIter=100, tol=1E-6, censorCol="censor",
                  quantileProbabilities=list([0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]),
                  quantilesCol=None, aggregationDepth=2):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  fitIntercept=True, maxIter=100, tol=1E-6, censorCol="censor", \
                  quantileProbabilities=[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99], \
                  quantilesCol=None, aggregationDepth=2):
        """
        kwargs = self._input_kwargs
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
    def setQuantileProbabilities(self, value):
        """
        Sets the value of :py:attr:`quantileProbabilities`.
        """
        return self._set(quantileProbabilities=value)

    @since("1.6.0")
    def setQuantilesCol(self, value):
        """
        Sets the value of :py:attr:`quantilesCol`.
        """
        return self._set(quantilesCol=value)

    @since("1.6.0")
    def setMaxIter(self, value):
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("1.6.0")
    def setTol(self, value):
        """
        Sets the value of :py:attr:`tol`.
        """
        return self._set(tol=value)

    @since("1.6.0")
    def setFitIntercept(self, value):
        """
        Sets the value of :py:attr:`fitIntercept`.
        """
        return self._set(fitIntercept=value)

    @since("2.1.0")
    def setAggregationDepth(self, value):
        """
        Sets the value of :py:attr:`aggregationDepth`.
        """
        return self._set(aggregationDepth=value)


class AFTSurvivalRegressionModel(JavaRegressionModel, _AFTSurvivalRegressionParams,
                                 JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`AFTSurvivalRegression`.

    .. versionadded:: 1.6.0
    """

    @since("3.0.0")
    def setQuantileProbabilities(self, value):
        """
        Sets the value of :py:attr:`quantileProbabilities`.
        """
        return self._set(quantileProbabilities=value)

    @since("3.0.0")
    def setQuantilesCol(self, value):
        """
        Sets the value of :py:attr:`quantilesCol`.
        """
        return self._set(quantilesCol=value)

    @property
    @since("2.0.0")
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
        Model scale parameter.
        """
        return self._call_java("scale")

    @since("2.0.0")
    def predictQuantiles(self, features):
        """
        Predicted Quantiles
        """
        return self._call_java("predictQuantiles", features)


class _GeneralizedLinearRegressionParams(_JavaPredictorParams, HasFitIntercept, HasMaxIter,
                                         HasTol, HasRegParam, HasWeightCol, HasSolver,
                                         HasAggregationDepth):
    """
    Params for :py:class:`GeneralizedLinearRegression` and
    :py:class:`GeneralizedLinearRegressionModel`.

    .. versionadded:: 3.0.0
    """

    family = Param(Params._dummy(), "family", "The name of family which is a description of " +
                   "the error distribution to be used in the model. Supported options: " +
                   "gaussian (default), binomial, poisson, gamma and tweedie.",
                   typeConverter=TypeConverters.toString)
    link = Param(Params._dummy(), "link", "The name of link function which provides the " +
                 "relationship between the linear predictor and the mean of the distribution " +
                 "function. Supported options: identity, log, inverse, logit, probit, cloglog " +
                 "and sqrt.", typeConverter=TypeConverters.toString)
    linkPredictionCol = Param(Params._dummy(), "linkPredictionCol", "link prediction (linear " +
                              "predictor) column name", typeConverter=TypeConverters.toString)
    variancePower = Param(Params._dummy(), "variancePower", "The power in the variance function " +
                          "of the Tweedie distribution which characterizes the relationship " +
                          "between the variance and mean of the distribution. Only applicable " +
                          "for the Tweedie family. Supported values: 0 and [1, Inf).",
                          typeConverter=TypeConverters.toFloat)
    linkPower = Param(Params._dummy(), "linkPower", "The index in the power link function. " +
                      "Only applicable to the Tweedie family.",
                      typeConverter=TypeConverters.toFloat)
    solver = Param(Params._dummy(), "solver", "The solver algorithm for optimization. Supported " +
                   "options: irls.", typeConverter=TypeConverters.toString)
    offsetCol = Param(Params._dummy(), "offsetCol", "The offset column name. If this is not set " +
                      "or empty, we treat all instance offsets as 0.0",
                      typeConverter=TypeConverters.toString)

    @since("2.0.0")
    def getFamily(self):
        """
        Gets the value of family or its default value.
        """
        return self.getOrDefault(self.family)

    @since("2.0.0")
    def getLinkPredictionCol(self):
        """
        Gets the value of linkPredictionCol or its default value.
        """
        return self.getOrDefault(self.linkPredictionCol)

    @since("2.0.0")
    def getLink(self):
        """
        Gets the value of link or its default value.
        """
        return self.getOrDefault(self.link)

    @since("2.2.0")
    def getVariancePower(self):
        """
        Gets the value of variancePower or its default value.
        """
        return self.getOrDefault(self.variancePower)

    @since("2.2.0")
    def getLinkPower(self):
        """
        Gets the value of linkPower or its default value.
        """
        return self.getOrDefault(self.linkPower)

    @since("2.3.0")
    def getOffsetCol(self):
        """
        Gets the value of offsetCol or its default value.
        """
        return self.getOrDefault(self.offsetCol)


@inherit_doc
class GeneralizedLinearRegression(JavaRegressor, _GeneralizedLinearRegressionParams,
                                  JavaMLWritable, JavaMLReadable):
    """
    Generalized Linear Regression.

    Fit a Generalized Linear Model specified by giving a symbolic description of the linear
    predictor (link function) and a description of the error distribution (family). It supports
    "gaussian", "binomial", "poisson", "gamma" and "tweedie" as family. Valid link functions for
    each family is listed below. The first link function of each family is the default one.

    * "gaussian" -> "identity", "log", "inverse"

    * "binomial" -> "logit", "probit", "cloglog"

    * "poisson"  -> "log", "identity", "sqrt"

    * "gamma"    -> "inverse", "identity", "log"

    * "tweedie"  -> power link function specified through "linkPower". \
                    The default link power in the tweedie family is 1 - variancePower.

    .. seealso:: `GLM <https://en.wikipedia.org/wiki/Generalized_linear_model>`_

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(0.0, 0.0)),
    ...     (1.0, Vectors.dense(1.0, 2.0)),
    ...     (2.0, Vectors.dense(0.0, 0.0)),
    ...     (2.0, Vectors.dense(1.0, 1.0)),], ["label", "features"])
    >>> glr = GeneralizedLinearRegression(family="gaussian", link="identity", linkPredictionCol="p")
    >>> glr.setRegParam(0.1)
    GeneralizedLinearRegression...
    >>> glr.getRegParam()
    0.1
    >>> glr.clear(glr.regParam)
    >>> glr.setMaxIter(10)
    GeneralizedLinearRegression...
    >>> glr.getMaxIter()
    10
    >>> glr.clear(glr.maxIter)
    >>> model = glr.fit(df)
    >>> model.setFeaturesCol("features")
    GeneralizedLinearRegressionModel...
    >>> model.getMaxIter()
    25
    >>> model.getAggregationDepth()
    2
    >>> transformed = model.transform(df)
    >>> abs(transformed.head().prediction - 1.5) < 0.001
    True
    >>> abs(transformed.head().p - 1.5) < 0.001
    True
    >>> model.coefficients
    DenseVector([1.5..., -1.0...])
    >>> model.numFeatures
    2
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

    @keyword_only
    def __init__(self, labelCol="label", featuresCol="features", predictionCol="prediction",
                 family="gaussian", link=None, fitIntercept=True, maxIter=25, tol=1e-6,
                 regParam=0.0, weightCol=None, solver="irls", linkPredictionCol=None,
                 variancePower=0.0, linkPower=None, offsetCol=None, aggregationDepth=2):
        """
        __init__(self, labelCol="label", featuresCol="features", predictionCol="prediction", \
                 family="gaussian", link=None, fitIntercept=True, maxIter=25, tol=1e-6, \
                 regParam=0.0, weightCol=None, solver="irls", linkPredictionCol=None, \
                 variancePower=0.0, linkPower=None, offsetCol=None, aggregationDepth=2)
        """
        super(GeneralizedLinearRegression, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.GeneralizedLinearRegression", self.uid)
        self._setDefault(family="gaussian", maxIter=25, tol=1e-6, regParam=0.0, solver="irls",
                         variancePower=0.0, aggregationDepth=2)
        kwargs = self._input_kwargs

        self.setParams(**kwargs)

    @keyword_only
    @since("2.0.0")
    def setParams(self, labelCol="label", featuresCol="features", predictionCol="prediction",
                  family="gaussian", link=None, fitIntercept=True, maxIter=25, tol=1e-6,
                  regParam=0.0, weightCol=None, solver="irls", linkPredictionCol=None,
                  variancePower=0.0, linkPower=None, offsetCol=None, aggregationDepth=2):
        """
        setParams(self, labelCol="label", featuresCol="features", predictionCol="prediction", \
                  family="gaussian", link=None, fitIntercept=True, maxIter=25, tol=1e-6, \
                  regParam=0.0, weightCol=None, solver="irls", linkPredictionCol=None, \
                  variancePower=0.0, linkPower=None, offsetCol=None, aggregationDepth=2)
        Sets params for generalized linear regression.
        """
        kwargs = self._input_kwargs
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
    def setLinkPredictionCol(self, value):
        """
        Sets the value of :py:attr:`linkPredictionCol`.
        """
        return self._set(linkPredictionCol=value)

    @since("2.0.0")
    def setLink(self, value):
        """
        Sets the value of :py:attr:`link`.
        """
        return self._set(link=value)

    @since("2.2.0")
    def setVariancePower(self, value):
        """
        Sets the value of :py:attr:`variancePower`.
        """
        return self._set(variancePower=value)

    @since("2.2.0")
    def setLinkPower(self, value):
        """
        Sets the value of :py:attr:`linkPower`.
        """
        return self._set(linkPower=value)

    @since("2.3.0")
    def setOffsetCol(self, value):
        """
        Sets the value of :py:attr:`offsetCol`.
        """
        return self._set(offsetCol=value)

    @since("2.0.0")
    def setMaxIter(self, value):
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("2.0.0")
    def setRegParam(self, value):
        """
        Sets the value of :py:attr:`regParam`.
        """
        return self._set(regParam=value)

    @since("2.0.0")
    def setTol(self, value):
        """
        Sets the value of :py:attr:`tol`.
        """
        return self._set(tol=value)

    @since("2.0.0")
    def setFitIntercept(self, value):
        """
        Sets the value of :py:attr:`fitIntercept`.
        """
        return self._set(fitIntercept=value)

    @since("2.0.0")
    def setWeightCol(self, value):
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @since("2.0.0")
    def setSolver(self, value):
        """
        Sets the value of :py:attr:`solver`.
        """
        return self._set(solver=value)

    @since("3.0.0")
    def setAggregationDepth(self, value):
        """
        Sets the value of :py:attr:`aggregationDepth`.
        """
        return self._set(aggregationDepth=value)


class GeneralizedLinearRegressionModel(JavaRegressionModel, _GeneralizedLinearRegressionParams,
                                       JavaMLWritable, JavaMLReadable, HasTrainingSummary):
    """
    Model fitted by :class:`GeneralizedLinearRegression`.

    .. versionadded:: 2.0.0
    """

    @since("3.0.0")
    def setLinkPredictionCol(self, value):
        """
        Sets the value of :py:attr:`linkPredictionCol`.
        """
        return self._set(linkPredictionCol=value)

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

    @property
    @since("2.0.0")
    def summary(self):
        """
        Gets summary (e.g. residuals, deviance, pValues) of model on
        training set. An exception is thrown if
        `trainingSummary is None`.
        """
        if self.hasSummary:
            return GeneralizedLinearRegressionTrainingSummary(
                super(GeneralizedLinearRegressionModel, self).summary)
        else:
            raise RuntimeError("No training summary available for this %s" %
                               self.__class__.__name__)

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
        java_glr_summary = self._call_java("evaluate", dataset)
        return GeneralizedLinearRegressionSummary(java_glr_summary)


class GeneralizedLinearRegressionSummary(JavaWrapper):
    """
    Generalized linear regression results evaluated on a dataset.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def predictions(self):
        """
        Predictions output by the model's `transform` method.
        """
        return self._call_java("predictions")

    @property
    @since("2.0.0")
    def predictionCol(self):
        """
        Field in :py:attr:`predictions` which gives the predicted value of each instance.
        This is set to a new column name if the original model's `predictionCol` is not set.
        """
        return self._call_java("predictionCol")

    @property
    @since("2.2.0")
    def numInstances(self):
        """
        Number of instances in DataFrame predictions.
        """
        return self._call_java("numInstances")

    @property
    @since("2.0.0")
    def rank(self):
        """
        The numeric rank of the fitted linear model.
        """
        return self._call_java("rank")

    @property
    @since("2.0.0")
    def degreesOfFreedom(self):
        """
        Degrees of freedom.
        """
        return self._call_java("degreesOfFreedom")

    @property
    @since("2.0.0")
    def residualDegreeOfFreedom(self):
        """
        The residual degrees of freedom.
        """
        return self._call_java("residualDegreeOfFreedom")

    @property
    @since("2.0.0")
    def residualDegreeOfFreedomNull(self):
        """
        The residual degrees of freedom for the null model.
        """
        return self._call_java("residualDegreeOfFreedomNull")

    @since("2.0.0")
    def residuals(self, residualsType="deviance"):
        """
        Get the residuals of the fitted model by type.

        :param residualsType: The type of residuals which should be returned.
                              Supported options: deviance (default), pearson, working, and response.
        """
        return self._call_java("residuals", residualsType)

    @property
    @since("2.0.0")
    def nullDeviance(self):
        """
        The deviance for the null model.
        """
        return self._call_java("nullDeviance")

    @property
    @since("2.0.0")
    def deviance(self):
        """
        The deviance for the fitted model.
        """
        return self._call_java("deviance")

    @property
    @since("2.0.0")
    def dispersion(self):
        """
        The dispersion of the fitted model.
        It is taken as 1.0 for the "binomial" and "poisson" families, and otherwise
        estimated by the residual Pearson's Chi-Squared statistic (which is defined as
        sum of the squares of the Pearson residuals) divided by the residual degrees of freedom.
        """
        return self._call_java("dispersion")

    @property
    @since("2.0.0")
    def aic(self):
        """
        Akaike's "An Information Criterion"(AIC) for the fitted model.
        """
        return self._call_java("aic")


@inherit_doc
class GeneralizedLinearRegressionTrainingSummary(GeneralizedLinearRegressionSummary):
    """
    Generalized linear regression training results.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def numIterations(self):
        """
        Number of training iterations.
        """
        return self._call_java("numIterations")

    @property
    @since("2.0.0")
    def solver(self):
        """
        The numeric solver used for training.
        """
        return self._call_java("solver")

    @property
    @since("2.0.0")
    def coefficientStandardErrors(self):
        """
        Standard error of estimated coefficients and intercept.

        If :py:attr:`GeneralizedLinearRegression.fitIntercept` is set to True,
        then the last element returned corresponds to the intercept.
        """
        return self._call_java("coefficientStandardErrors")

    @property
    @since("2.0.0")
    def tValues(self):
        """
        T-statistic of estimated coefficients and intercept.

        If :py:attr:`GeneralizedLinearRegression.fitIntercept` is set to True,
        then the last element returned corresponds to the intercept.
        """
        return self._call_java("tValues")

    @property
    @since("2.0.0")
    def pValues(self):
        """
        Two-sided p-value of estimated coefficients and intercept.

        If :py:attr:`GeneralizedLinearRegression.fitIntercept` is set to True,
        then the last element returned corresponds to the intercept.
        """
        return self._call_java("pValues")

    def __repr__(self):
        return self._call_java("toString")


class _FactorizationMachinesParams(_JavaPredictorParams, HasMaxIter, HasStepSize, HasTol,
                                   HasSolver, HasSeed, HasFitIntercept, HasRegParam):
    """
    Params for :py:class:`FMRegressor`, :py:class:`FMRegressionModel`, :py:class:`FMClassifier`
    and :py:class:`FMClassifierModel`.

    .. versionadded:: 3.0.0
    """

    factorSize = Param(Params._dummy(), "factorSize", "Dimensionality of the factor vectors, " +
                       "which are used to get pairwise interactions between variables",
                       typeConverter=TypeConverters.toInt)

    fitLinear = Param(Params._dummy(), "fitLinear", "whether to fit linear term (aka 1-way term)",
                      typeConverter=TypeConverters.toBoolean)

    miniBatchFraction = Param(Params._dummy(), "miniBatchFraction", "fraction of the input data " +
                              "set that should be used for one iteration of gradient descent",
                              typeConverter=TypeConverters.toFloat)

    initStd = Param(Params._dummy(), "initStd", "standard deviation of initial coefficients",
                    typeConverter=TypeConverters.toFloat)

    solver = Param(Params._dummy(), "solver", "The solver algorithm for optimization. Supported " +
                   "options: gd, adamW. (Default adamW)", typeConverter=TypeConverters.toString)

    @since("3.0.0")
    def getFactorSize(self):
        """
        Gets the value of factorSize or its default value.
        """
        return self.getOrDefault(self.factorSize)

    @since("3.0.0")
    def getFitLinear(self):
        """
        Gets the value of fitLinear or its default value.
        """
        return self.getOrDefault(self.fitLinear)

    @since("3.0.0")
    def getMiniBatchFraction(self):
        """
        Gets the value of miniBatchFraction or its default value.
        """
        return self.getOrDefault(self.miniBatchFraction)

    @since("3.0.0")
    def getInitStd(self):
        """
        Gets the value of initStd or its default value.
        """
        return self.getOrDefault(self.initStd)


@inherit_doc
class FMRegressor(JavaRegressor, _FactorizationMachinesParams, JavaMLWritable, JavaMLReadable):
    """
    Factorization Machines learning algorithm for regression.

    solver Supports:

    * gd (normal mini-batch gradient descent)
    * adamW (default)

    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.regression import FMRegressor
    >>> df = spark.createDataFrame([
    ...     (2.0, Vectors.dense(2.0)),
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>>
    >>> fm = FMRegressor(factorSize=2)
    >>> fm.setSeed(16)
    FMRegressor...
    >>> model = fm.fit(df)
    >>> model.getMaxIter()
    100
    >>> test0 = spark.createDataFrame([
    ...     (Vectors.dense(-2.0),),
    ...     (Vectors.dense(0.5),),
    ...     (Vectors.dense(1.0),),
    ...     (Vectors.dense(4.0),)], ["features"])
    >>> model.transform(test0).show(10, False)
    +--------+-------------------+
    |features|prediction         |
    +--------+-------------------+
    |[-2.0]  |-1.9989237712341565|
    |[0.5]   |0.4956682219523814 |
    |[1.0]   |0.994586620589689  |
    |[4.0]   |3.9880970124135344 |
    +--------+-------------------+
    ...
    >>> model.intercept
    -0.0032501766849261557
    >>> model.linear
    DenseVector([0.9978])
    >>> model.factors
    DenseMatrix(1, 2, [0.0173, 0.0021], 1)

    .. versionadded:: 3.0.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 factorSize=8, fitIntercept=True, fitLinear=True, regParam=0.0,
                 miniBatchFraction=1.0, initStd=0.01, maxIter=100, stepSize=1.0,
                 tol=1e-6, solver="adamW", seed=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 factorSize=8, fitIntercept=True, fitLinear=True, regParam=0.0, \
                 miniBatchFraction=1.0, initStd=0.01, maxIter=100, stepSize=1.0, \
                 tol=1e-6, solver="adamW", seed=None)
        """
        super(FMRegressor, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.regression.FMRegressor", self.uid)
        self._setDefault(factorSize=8, fitIntercept=True, fitLinear=True, regParam=0.0,
                         miniBatchFraction=1.0, initStd=0.01, maxIter=100, stepSize=1.0,
                         tol=1e-6, solver="adamW")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("3.0.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  factorSize=8, fitIntercept=True, fitLinear=True, regParam=0.0,
                  miniBatchFraction=1.0, initStd=0.01, maxIter=100, stepSize=1.0,
                  tol=1e-6, solver="adamW", seed=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  factorSize=8, fitIntercept=True, fitLinear=True, regParam=0.0, \
                  miniBatchFraction=1.0, initStd=0.01, maxIter=100, stepSize=1.0, \
                  tol=1e-6, solver="adamW", seed=None)
        Sets Params for FMRegressor.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return FMRegressionModel(java_model)

    @since("3.0.0")
    def setFactorSize(self, value):
        """
        Sets the value of :py:attr:`factorSize`.
        """
        return self._set(factorSize=value)

    @since("3.0.0")
    def setFitLinear(self, value):
        """
        Sets the value of :py:attr:`fitLinear`.
        """
        return self._set(fitLinear=value)

    @since("3.0.0")
    def setMiniBatchFraction(self, value):
        """
        Sets the value of :py:attr:`miniBatchFraction`.
        """
        return self._set(miniBatchFraction=value)

    @since("3.0.0")
    def setInitStd(self, value):
        """
        Sets the value of :py:attr:`initStd`.
        """
        return self._set(initStd=value)

    @since("3.0.0")
    def setMaxIter(self, value):
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("3.0.0")
    def setStepSize(self, value):
        """
        Sets the value of :py:attr:`stepSize`.
        """
        return self._set(stepSize=value)

    @since("3.0.0")
    def setTol(self, value):
        """
        Sets the value of :py:attr:`tol`.
        """
        return self._set(tol=value)

    @since("3.0.0")
    def setSolver(self, value):
        """
        Sets the value of :py:attr:`solver`.
        """
        return self._set(solver=value)

    @since("3.0.0")
    def setSeed(self, value):
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("3.0.0")
    def setFitIntercept(self, value):
        """
        Sets the value of :py:attr:`fitIntercept`.
        """
        return self._set(fitIntercept=value)

    @since("3.0.0")
    def setRegParam(self, value):
        """
        Sets the value of :py:attr:`regParam`.
        """
        return self._set(regParam=value)


class FMRegressionModel(JavaRegressionModel, _FactorizationMachinesParams, JavaMLWritable,
                        JavaMLReadable):
    """
    Model fitted by :class:`FMRegressor`.

    .. versionadded:: 3.0.0
    """

    @property
    @since("3.0.0")
    def intercept(self):
        """
        Model intercept.
        """
        return self._call_java("intercept")

    @property
    @since("3.0.0")
    def linear(self):
        """
        Model linear term.
        """
        return self._call_java("linear")

    @property
    @since("3.0.0")
    def factors(self):
        """
        Model factor term.
        """
        return self._call_java("factors")


if __name__ == "__main__":
    import doctest
    import pyspark.ml.regression
    from pyspark.sql import SparkSession
    globs = pyspark.ml.regression.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder\
        .master("local[2]")\
        .appName("ml.regression tests")\
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
        sys.exit(-1)
