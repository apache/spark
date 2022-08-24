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
import warnings
from typing import (
    Any,
    Callable,
    Iterable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
    TYPE_CHECKING,
)

import numpy as np

from pyspark import RDD, since
from pyspark.streaming.dstream import DStream
from pyspark.mllib.common import callMLlibFunc, _py2java, _java2py, inherit_doc
from pyspark.mllib.linalg import _convert_to_vector
from pyspark.mllib.util import Saveable, Loader
from pyspark.rdd import RDD
from pyspark.context import SparkContext
from pyspark.mllib.linalg import Vector

if TYPE_CHECKING:
    from pyspark.mllib._typing import VectorLike


LM = TypeVar("LM")
K = TypeVar("K")

__all__ = [
    "LabeledPoint",
    "LinearModel",
    "LinearRegressionModel",
    "LinearRegressionWithSGD",
    "RidgeRegressionModel",
    "RidgeRegressionWithSGD",
    "LassoModel",
    "LassoWithSGD",
    "IsotonicRegressionModel",
    "IsotonicRegression",
    "StreamingLinearAlgorithm",
    "StreamingLinearRegressionWithSGD",
]


class LabeledPoint:

    """
    Class that represents the features and labels of a data point.

    .. versionadded:: 1.0.0

    Parameters
    ----------
    label : int
        Label for this data point.
    features : :py:class:`pyspark.mllib.linalg.Vector` or convertible
        Vector of features for this point (NumPy array, list,
        pyspark.mllib.linalg.SparseVector, or scipy.sparse column matrix).

    Notes
    -----
    'label' and 'features' are accessible as class attributes.
    """

    def __init__(self, label: float, features: Iterable[float]):
        self.label = float(label)
        self.features = _convert_to_vector(features)

    def __reduce__(self) -> Tuple[Type["LabeledPoint"], Tuple[float, Vector]]:
        return (LabeledPoint, (self.label, self.features))

    def __str__(self) -> str:
        return "(" + ",".join((str(self.label), str(self.features))) + ")"

    def __repr__(self) -> str:
        return "LabeledPoint(%s, %s)" % (self.label, self.features)


class LinearModel:

    """
    A linear model that has a vector of coefficients and an intercept.

    .. versionadded:: 0.9.0

    Parameters
    ----------
    weights : :py:class:`pyspark.mllib.linalg.Vector`
        Weights computed for every feature.
    intercept : float
      Intercept computed for this model.
    """

    def __init__(self, weights: Vector, intercept: float):
        self._coeff = _convert_to_vector(weights)
        self._intercept = float(intercept)

    @property  # type: ignore[misc]
    @since("1.0.0")
    def weights(self) -> Vector:
        """Weights computed for every feature."""
        return self._coeff

    @property  # type: ignore[misc]
    @since("1.0.0")
    def intercept(self) -> float:
        """Intercept computed for this model."""
        return self._intercept

    def __repr__(self) -> str:
        return "(weights=%s, intercept=%r)" % (self._coeff, self._intercept)


@inherit_doc
class LinearRegressionModelBase(LinearModel):

    """A linear regression model.

    .. versionadded:: 0.9.0

    Examples
    --------
    >>> from pyspark.mllib.linalg import SparseVector
    >>> lrmb = LinearRegressionModelBase(np.array([1.0, 2.0]), 0.1)
    >>> abs(lrmb.predict(np.array([-1.03, 7.777])) - 14.624) < 1e-6
    True
    >>> abs(lrmb.predict(SparseVector(2, {0: -1.03, 1: 7.777})) - 14.624) < 1e-6
    True
    """

    @overload
    def predict(self, x: "VectorLike") -> float:
        ...

    @overload
    def predict(self, x: RDD["VectorLike"]) -> RDD[float]:
        ...

    def predict(self, x: Union["VectorLike", RDD["VectorLike"]]) -> Union[float, RDD[float]]:
        """
        Predict the value of the dependent variable given a vector or
        an RDD of vectors containing values for the independent variables.

        .. versionadded:: 0.9.0
        """
        if isinstance(x, RDD):
            return x.map(self.predict)
        x = _convert_to_vector(x)
        return self.weights.dot(x) + self.intercept  # type: ignore[attr-defined]


@inherit_doc
class LinearRegressionModel(LinearRegressionModelBase):

    """A linear regression model derived from a least-squares fit.

    .. versionadded:: 0.9.0

    Examples
    --------
    >>> from pyspark.mllib.linalg import SparseVector
    >>> from pyspark.mllib.regression import LabeledPoint
    >>> data = [
    ...     LabeledPoint(0.0, [0.0]),
    ...     LabeledPoint(1.0, [1.0]),
    ...     LabeledPoint(3.0, [2.0]),
    ...     LabeledPoint(2.0, [3.0])
    ... ]
    >>> lrm = LinearRegressionWithSGD.train(sc.parallelize(data), iterations=10,
    ...     initialWeights=np.array([1.0]))
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(np.array([1.0])) - 1) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> abs(lrm.predict(sc.parallelize([[1.0]])).collect()[0] - 1) < 0.5
    True
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> lrm.save(sc, path)
    >>> sameModel = LinearRegressionModel.load(sc, path)
    >>> abs(sameModel.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(sameModel.predict(np.array([1.0])) - 1) < 0.5
    True
    >>> abs(sameModel.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except BaseException:
    ...     pass
    >>> data = [
    ...     LabeledPoint(0.0, SparseVector(1, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(1, {0: 1.0})),
    ...     LabeledPoint(3.0, SparseVector(1, {0: 2.0})),
    ...     LabeledPoint(2.0, SparseVector(1, {0: 3.0}))
    ... ]
    >>> lrm = LinearRegressionWithSGD.train(sc.parallelize(data), iterations=10,
    ...     initialWeights=np.array([1.0]))
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> lrm = LinearRegressionWithSGD.train(sc.parallelize(data), iterations=10, step=1.0,
    ...    miniBatchFraction=1.0, initialWeights=np.array([1.0]), regParam=0.1, regType="l2",
    ...    intercept=True, validateData=True)
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    """

    @since("1.4.0")
    def save(self, sc: SparkContext, path: str) -> None:
        """Save a LinearRegressionModel."""
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.regression.LinearRegressionModel(
            _py2java(sc, self._coeff), self.intercept
        )
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since("1.4.0")
    def load(cls, sc: SparkContext, path: str) -> "LinearRegressionModel":
        """Load a LinearRegressionModel."""
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.regression.LinearRegressionModel.load(
            sc._jsc.sc(), path
        )
        weights = _java2py(sc, java_model.weights())
        intercept = java_model.intercept()
        model = LinearRegressionModel(weights, intercept)
        return model


# train_func should take two parameters, namely data and initial_weights, and
# return the result of a call to the appropriate JVM stub.
# _regression_train_wrapper is responsible for setup and error checking.
def _regression_train_wrapper(
    train_func: Callable[[RDD[LabeledPoint], Vector], Iterable[Any]],
    modelClass: Type[LM],
    data: RDD[LabeledPoint],
    initial_weights: Optional["VectorLike"],
) -> LM:
    from pyspark.mllib.classification import LogisticRegressionModel

    first = data.first()
    if not isinstance(first, LabeledPoint):
        raise TypeError("data should be an RDD of LabeledPoint, but got %s" % type(first))
    if initial_weights is None:
        initial_weights = [0.0] * len(data.first().features)
    if modelClass == LogisticRegressionModel:
        weights, intercept, numFeatures, numClasses = train_func(
            data, _convert_to_vector(initial_weights)
        )
        return modelClass(weights, intercept, numFeatures, numClasses)  # type: ignore[call-arg]
    else:
        weights, intercept = train_func(data, _convert_to_vector(initial_weights))
        return modelClass(weights, intercept)  # type: ignore[call-arg]


class LinearRegressionWithSGD:
    """
    Train a linear regression model with no regularization using Stochastic Gradient Descent.

    .. versionadded:: 0.9.0
    .. deprecated:: 2.0.0
        Use :py:class:`pyspark.ml.regression.LinearRegression`.
    """

    @classmethod
    def train(
        cls,
        data: RDD[LabeledPoint],
        iterations: int = 100,
        step: float = 1.0,
        miniBatchFraction: float = 1.0,
        initialWeights: Optional["VectorLike"] = None,
        regParam: float = 0.0,
        regType: Optional[str] = None,
        intercept: bool = False,
        validateData: bool = True,
        convergenceTol: float = 0.001,
    ) -> LinearRegressionModel:
        """
        Train a linear regression model using Stochastic Gradient
        Descent (SGD). This solves the least squares regression
        formulation

            f(weights) = 1/(2n) ||A weights - y||^2

        which is the mean squared error. Here the data matrix has n rows,
        and the input RDD holds the set of rows of A, each with its
        corresponding right hand side label y.
        See also the documentation for the precise formulation.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            The training data, an RDD of LabeledPoint.
        iterations : int, optional
            The number of iterations.
            (default: 100)
        step : float, optional
            The step parameter used in SGD.
            (default: 1.0)
        miniBatchFraction : float, optional
            Fraction of data to be used for each SGD iteration.
            (default: 1.0)
        initialWeights : :py:class:`pyspark.mllib.linalg.Vector` or convertible, optional
            The initial weights.
            (default: None)
        regParam : float, optional
            The regularizer parameter.
            (default: 0.0)
        regType : str, optional
            The type of regularizer used for training our model.
            Supported values:

            - "l1" for using L1 regularization
            - "l2" for using L2 regularization
            - None for no regularization (default)

        intercept : bool, optional
            Boolean parameter which indicates the use or not of the
            augmented representation for training data (i.e., whether bias
            features are activated or not).
            (default: False)
        validateData : bool, optional
            Boolean parameter which indicates if the algorithm should
            validate data before training.
            (default: True)
        convergenceTol : float, optional
            A condition which decides iteration termination.
            (default: 0.001)
        """
        warnings.warn("Deprecated in 2.0.0. Use ml.regression.LinearRegression.", FutureWarning)

        def train(rdd: RDD[LabeledPoint], i: Vector) -> Iterable[Any]:
            return callMLlibFunc(
                "trainLinearRegressionModelWithSGD",
                rdd,
                int(iterations),
                float(step),
                float(miniBatchFraction),
                i,
                float(regParam),
                regType,
                bool(intercept),
                bool(validateData),
                float(convergenceTol),
            )

        return _regression_train_wrapper(train, LinearRegressionModel, data, initialWeights)


@inherit_doc
class LassoModel(LinearRegressionModelBase):

    """A linear regression model derived from a least-squares fit with
    an l_1 penalty term.

    .. versionadded:: 0.9.0

    Examples
    --------
    >>> from pyspark.mllib.linalg import SparseVector
    >>> from pyspark.mllib.regression import LabeledPoint
    >>> data = [
    ...     LabeledPoint(0.0, [0.0]),
    ...     LabeledPoint(1.0, [1.0]),
    ...     LabeledPoint(3.0, [2.0]),
    ...     LabeledPoint(2.0, [3.0])
    ... ]
    >>> lrm = LassoWithSGD.train(
    ...     sc.parallelize(data), iterations=10, initialWeights=np.array([1.0]))
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(np.array([1.0])) - 1) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> abs(lrm.predict(sc.parallelize([[1.0]])).collect()[0] - 1) < 0.5
    True
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> lrm.save(sc, path)
    >>> sameModel = LassoModel.load(sc, path)
    >>> abs(sameModel.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(sameModel.predict(np.array([1.0])) - 1) < 0.5
    True
    >>> abs(sameModel.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> from shutil import rmtree
    >>> try:
    ...    rmtree(path)
    ... except BaseException:
    ...    pass
    >>> data = [
    ...     LabeledPoint(0.0, SparseVector(1, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(1, {0: 1.0})),
    ...     LabeledPoint(3.0, SparseVector(1, {0: 2.0})),
    ...     LabeledPoint(2.0, SparseVector(1, {0: 3.0}))
    ... ]
    >>> lrm = LinearRegressionWithSGD.train(sc.parallelize(data), iterations=10,
    ...     initialWeights=np.array([1.0]))
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> lrm = LassoWithSGD.train(sc.parallelize(data), iterations=10, step=1.0,
    ...     regParam=0.01, miniBatchFraction=1.0, initialWeights=np.array([1.0]), intercept=True,
    ...     validateData=True)
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    """

    @since("1.4.0")
    def save(self, sc: SparkContext, path: str) -> None:
        """Save a LassoModel."""
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.regression.LassoModel(
            _py2java(sc, self._coeff), self.intercept
        )
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since("1.4.0")
    def load(cls, sc: SparkContext, path: str) -> "LassoModel":
        """Load a LassoModel."""
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.regression.LassoModel.load(sc._jsc.sc(), path)
        weights = _java2py(sc, java_model.weights())
        intercept = java_model.intercept()
        model = LassoModel(weights, intercept)
        return model


class LassoWithSGD:
    """
    Train a regression model with L1-regularization using Stochastic Gradient Descent.

    .. versionadded:: 0.9.0
    .. deprecated:: 2.0.0
        Use :py:class:`pyspark.ml.regression.LinearRegression` with elasticNetParam = 1.0.
        Note the default regParam is 0.01 for LassoWithSGD, but is 0.0 for LinearRegression.
    """

    @classmethod
    def train(
        cls,
        data: RDD[LabeledPoint],
        iterations: int = 100,
        step: float = 1.0,
        regParam: float = 0.01,
        miniBatchFraction: float = 1.0,
        initialWeights: Optional["VectorLike"] = None,
        intercept: bool = False,
        validateData: bool = True,
        convergenceTol: float = 0.001,
    ) -> LassoModel:
        """
        Train a regression model with L1-regularization using Stochastic
        Gradient Descent. This solves the l1-regularized least squares
        regression formulation

            f(weights) = 1/(2n) ||A weights - y||^2  + regParam ||weights||_1

        Here the data matrix has n rows, and the input RDD holds the set
        of rows of A, each with its corresponding right hand side label y.
        See also the documentation for the precise formulation.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            The training data, an RDD of LabeledPoint.
        iterations : int, optional
            The number of iterations.
            (default: 100)
        step : float, optional
            The step parameter used in SGD.
            (default: 1.0)
        regParam : float, optional
            The regularizer parameter.
            (default: 0.01)
        miniBatchFraction : float, optional
            Fraction of data to be used for each SGD iteration.
            (default: 1.0)
        initialWeights : :py:class:`pyspark.mllib.linalg.Vector` or convertible, optional
            The initial weights.
            (default: None)
        intercept : bool, optional
            Boolean parameter which indicates the use or not of the
            augmented representation for training data (i.e. whether bias
            features are activated or not).
            (default: False)
        validateData : bool, optional
            Boolean parameter which indicates if the algorithm should
            validate data before training.
            (default: True)
        convergenceTol : float, optional
            A condition which decides iteration termination.
            (default: 0.001)
        """
        warnings.warn(
            "Deprecated in 2.0.0. Use ml.regression.LinearRegression with elasticNetParam = 1.0. "
            "Note the default regParam is 0.01 for LassoWithSGD, but is 0.0 for LinearRegression.",
            FutureWarning,
        )

        def train(rdd: RDD[LabeledPoint], i: Vector) -> Iterable[Any]:
            return callMLlibFunc(
                "trainLassoModelWithSGD",
                rdd,
                int(iterations),
                float(step),
                float(regParam),
                float(miniBatchFraction),
                i,
                bool(intercept),
                bool(validateData),
                float(convergenceTol),
            )

        return _regression_train_wrapper(train, LassoModel, data, initialWeights)


@inherit_doc
class RidgeRegressionModel(LinearRegressionModelBase):

    """A linear regression model derived from a least-squares fit with
    an l_2 penalty term.

    .. versionadded:: 0.9.0

    Examples
    --------
    >>> from pyspark.mllib.linalg import SparseVector
    >>> from pyspark.mllib.regression import LabeledPoint
    >>> data = [
    ...     LabeledPoint(0.0, [0.0]),
    ...     LabeledPoint(1.0, [1.0]),
    ...     LabeledPoint(3.0, [2.0]),
    ...     LabeledPoint(2.0, [3.0])
    ... ]
    >>> lrm = RidgeRegressionWithSGD.train(sc.parallelize(data), iterations=10,
    ...     initialWeights=np.array([1.0]))
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(np.array([1.0])) - 1) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> abs(lrm.predict(sc.parallelize([[1.0]])).collect()[0] - 1) < 0.5
    True
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> lrm.save(sc, path)
    >>> sameModel = RidgeRegressionModel.load(sc, path)
    >>> abs(sameModel.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(sameModel.predict(np.array([1.0])) - 1) < 0.5
    True
    >>> abs(sameModel.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> from shutil import rmtree
    >>> try:
    ...    rmtree(path)
    ... except BaseException:
    ...    pass
    >>> data = [
    ...     LabeledPoint(0.0, SparseVector(1, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(1, {0: 1.0})),
    ...     LabeledPoint(3.0, SparseVector(1, {0: 2.0})),
    ...     LabeledPoint(2.0, SparseVector(1, {0: 3.0}))
    ... ]
    >>> lrm = LinearRegressionWithSGD.train(sc.parallelize(data), iterations=10,
    ...     initialWeights=np.array([1.0]))
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> lrm = RidgeRegressionWithSGD.train(sc.parallelize(data), iterations=10, step=1.0,
    ...     regParam=0.01, miniBatchFraction=1.0, initialWeights=np.array([1.0]), intercept=True,
    ...     validateData=True)
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    """

    @since("1.4.0")
    def save(self, sc: SparkContext, path: str) -> None:
        """Save a RidgeRegressionMode."""
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.regression.RidgeRegressionModel(
            _py2java(sc, self._coeff), self.intercept
        )
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since("1.4.0")
    def load(cls, sc: SparkContext, path: str) -> "RidgeRegressionModel":
        """Load a RidgeRegressionMode."""
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.regression.RidgeRegressionModel.load(
            sc._jsc.sc(), path
        )
        weights = _java2py(sc, java_model.weights())
        intercept = java_model.intercept()
        model = RidgeRegressionModel(weights, intercept)
        return model


class RidgeRegressionWithSGD:
    """
    Train a regression model with L2-regularization using Stochastic Gradient Descent.

    .. versionadded:: 0.9.0
    .. deprecated:: 2.0.0
        Use :py:class:`pyspark.ml.regression.LinearRegression` with elasticNetParam = 0.0.
        Note the default regParam is 0.01 for RidgeRegressionWithSGD, but is 0.0 for
        LinearRegression.
    """

    @classmethod
    def train(
        cls,
        data: RDD[LabeledPoint],
        iterations: int = 100,
        step: float = 1.0,
        regParam: float = 0.01,
        miniBatchFraction: float = 1.0,
        initialWeights: Optional["VectorLike"] = None,
        intercept: bool = False,
        validateData: bool = True,
        convergenceTol: float = 0.001,
    ) -> RidgeRegressionModel:
        """
        Train a regression model with L2-regularization using Stochastic
        Gradient Descent. This solves the l2-regularized least squares
        regression formulation

            f(weights) = 1/(2n) ||A weights - y||^2 + regParam/2 ||weights||^2

        Here the data matrix has n rows, and the input RDD holds the set
        of rows of A, each with its corresponding right hand side label y.
        See also the documentation for the precise formulation.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            The training data, an RDD of LabeledPoint.
        iterations : int, optional
            The number of iterations.
            (default: 100)
        step : float, optional
            The step parameter used in SGD.
            (default: 1.0)
        regParam : float, optional
            The regularizer parameter.
            (default: 0.01)
        miniBatchFraction : float, optional
            Fraction of data to be used for each SGD iteration.
            (default: 1.0)
        initialWeights : :py:class:`pyspark.mllib.linalg.Vector` or convertible, optional
            The initial weights.
            (default: None)
        intercept : bool, optional
            Boolean parameter which indicates the use or not of the
            augmented representation for training data (i.e. whether bias
            features are activated or not).
            (default: False)
        validateData : bool, optional
            Boolean parameter which indicates if the algorithm should
            validate data before training.
            (default: True)
        convergenceTol : float, optional
            A condition which decides iteration termination.
            (default: 0.001)
        """
        warnings.warn(
            "Deprecated in 2.0.0. Use ml.regression.LinearRegression with elasticNetParam = 0.0. "
            "Note the default regParam is 0.01 for RidgeRegressionWithSGD, but is 0.0 for "
            "LinearRegression.",
            FutureWarning,
        )

        def train(rdd: RDD[LabeledPoint], i: Vector) -> Iterable[Any]:
            return callMLlibFunc(
                "trainRidgeModelWithSGD",
                rdd,
                int(iterations),
                float(step),
                float(regParam),
                float(miniBatchFraction),
                i,
                bool(intercept),
                bool(validateData),
                float(convergenceTol),
            )

        return _regression_train_wrapper(train, RidgeRegressionModel, data, initialWeights)


class IsotonicRegressionModel(Saveable, Loader["IsotonicRegressionModel"]):

    """
    Regression model for isotonic regression.

    .. versionadded:: 1.4.0

    Parameters
    ----------
    boundaries : ndarray
        Array of boundaries for which predictions are known. Boundaries
        must be sorted in increasing order.
    predictions : ndarray
        Array of predictions associated to the boundaries at the same
        index. Results of isotonic regression and therefore monotone.
    isotonic : true
        Indicates whether this is isotonic or antitonic.

    Examples
    --------
    >>> data = [(1, 0, 1), (2, 1, 1), (3, 2, 1), (1, 3, 1), (6, 4, 1), (17, 5, 1), (16, 6, 1)]
    >>> irm = IsotonicRegression.train(sc.parallelize(data))
    >>> irm.predict(3)
    2.0
    >>> irm.predict(5)
    16.5
    >>> irm.predict(sc.parallelize([3, 5])).collect()
    [2.0, 16.5]
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> irm.save(sc, path)
    >>> sameModel = IsotonicRegressionModel.load(sc, path)
    >>> sameModel.predict(3)
    2.0
    >>> sameModel.predict(5)
    16.5
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass
    """

    def __init__(self, boundaries: np.ndarray, predictions: np.ndarray, isotonic: bool):
        self.boundaries = boundaries
        self.predictions = predictions
        self.isotonic = isotonic

    @overload
    def predict(self, x: float) -> np.float64:
        ...

    @overload
    def predict(self, x: "VectorLike") -> np.ndarray:
        ...

    @overload
    def predict(self, x: RDD[float]) -> RDD[np.float64]:
        ...

    @overload
    def predict(self, x: RDD["VectorLike"]) -> RDD[np.ndarray]:
        ...

    def predict(
        self, x: Union[float, "VectorLike", RDD[float], RDD["VectorLike"]]
    ) -> Union[np.float64, np.ndarray, RDD[np.float64], RDD[np.ndarray]]:
        """
        Predict labels for provided features.
        Using a piecewise linear function.
        1) If x exactly matches a boundary then associated prediction
        is returned. In case there are multiple predictions with the
        same boundary then one of them is returned. Which one is
        undefined (same as java.util.Arrays.binarySearch).
        2) If x is lower or higher than all boundaries then first or
        last prediction is returned respectively. In case there are
        multiple predictions with the same boundary then the lowest
        or highest is returned respectively.
        3) If x falls between two values in boundary array then
        prediction is treated as piecewise linear function and
        interpolated value is returned. In case there are multiple
        values with the same boundary then the same rules as in 2)
        are used.


        .. versionadded:: 1.4.0

        Parameters
        ----------
        x : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            Feature or RDD of Features to be labeled.
        """
        if isinstance(x, RDD):
            return x.map(lambda v: self.predict(v))
        return np.interp(x, self.boundaries, self.predictions)  # type: ignore[arg-type]

    @since("1.4.0")
    def save(self, sc: SparkContext, path: str) -> None:
        """Save an IsotonicRegressionModel."""
        java_boundaries = _py2java(sc, self.boundaries.tolist())
        java_predictions = _py2java(sc, self.predictions.tolist())
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.regression.IsotonicRegressionModel(
            java_boundaries, java_predictions, self.isotonic
        )
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since("1.4.0")
    def load(cls, sc: SparkContext, path: str) -> "IsotonicRegressionModel":
        """Load an IsotonicRegressionModel."""
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.regression.IsotonicRegressionModel.load(
            sc._jsc.sc(), path
        )
        py_boundaries = _java2py(sc, java_model.boundaryVector()).toArray()
        py_predictions = _java2py(sc, java_model.predictionVector()).toArray()
        return IsotonicRegressionModel(py_boundaries, py_predictions, java_model.isotonic)


class IsotonicRegression:
    """
    Isotonic regression.
    Currently implemented using parallelized pool adjacent violators
    algorithm. Only univariate (single feature) algorithm supported.

    .. versionadded:: 1.4.0

    Notes
    -----
    Sequential PAV implementation based on
    Tibshirani, Ryan J., Holger Hoefling, and Robert Tibshirani (2011) [1]_

    Sequential PAV parallelization based on
    Kearsley, Anthony J., Richard A. Tapia, and Michael W. Trosset (1996) [2]_

    See also
    `Isotonic regression (Wikipedia) <http://en.wikipedia.org/wiki/Isotonic_regression>`_.

    .. [1] Tibshirani, Ryan J., Holger Hoefling, and Robert Tibshirani.
        "Nearly-isotonic regression." Technometrics 53.1 (2011): 54-61.
        Available from http://www.stat.cmu.edu/~ryantibs/papers/neariso.pdf
    .. [2] Kearsley, Anthony J., Richard A. Tapia, and Michael W. Trosset
        "An approach to parallelizing isotonic regression."
        Applied Mathematics and Parallel Computing. Physica-Verlag HD, 1996. 141-147.
        Available from http://softlib.rice.edu/pub/CRPC-TRs/reports/CRPC-TR96640.pdf
    """

    @classmethod
    def train(cls, data: RDD["VectorLike"], isotonic: bool = True) -> IsotonicRegressionModel:
        """
        Train an isotonic regression model on the given data.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            RDD of (label, feature, weight) tuples.
        isotonic : bool, optional
            Whether this is isotonic (which is default) or antitonic.
            (default: True)
        """
        boundaries, predictions = callMLlibFunc(
            "trainIsotonicRegressionModel", data.map(_convert_to_vector), bool(isotonic)
        )
        return IsotonicRegressionModel(boundaries.toArray(), predictions.toArray(), isotonic)


class StreamingLinearAlgorithm:
    """
    Base class that has to be inherited by any StreamingLinearAlgorithm.

    Prevents reimplementation of methods predictOn and predictOnValues.

    .. versionadded:: 1.5.0
    """

    def __init__(self, model: Optional[LinearModel]):
        self._model = model

    @since("1.5.0")
    def latestModel(self) -> Optional[LinearModel]:
        """
        Returns the latest model.
        """
        return self._model

    def _validate(self, dstream: Any) -> None:
        if not isinstance(dstream, DStream):
            raise TypeError("dstream should be a DStream object, got %s" % type(dstream))
        if not self._model:
            raise ValueError("Model must be initialized using setInitialWeights")

    def predictOn(self, dstream: "DStream[VectorLike]") -> "DStream[float]":
        """
        Use the model to make predictions on batches of data from a
        DStream.

        .. versionadded:: 1.5.0

        Returns
        -------
        :py:class:`pyspark.streaming.DStream`
            DStream containing predictions.
        """
        self._validate(dstream)
        return dstream.map(lambda x: self._model.predict(x))  # type: ignore[union-attr]

    def predictOnValues(
        self, dstream: "DStream[Tuple[K, VectorLike]]"
    ) -> "DStream[Tuple[K, float]]":
        """
        Use the model to make predictions on the values of a DStream and
        carry over its keys.

        .. versionadded:: 1.5.0

        Returns
        -------
        :py:class:`pyspark.streaming.DStream`
            DStream containing predictions.
        """
        self._validate(dstream)
        return dstream.mapValues(lambda x: self._model.predict(x))  # type: ignore[union-attr]


@inherit_doc
class StreamingLinearRegressionWithSGD(StreamingLinearAlgorithm):
    """
    Train or predict a linear regression model on streaming data.
    Training uses Stochastic Gradient Descent to update the model
    based on each new batch of incoming data from a DStream
    (see `LinearRegressionWithSGD` for model equation).

    Each batch of data is assumed to be an RDD of LabeledPoints.
    The number of data points per batch can vary, but the number
    of features must be constant. An initial weight vector must
    be provided.

    .. versionadded:: 1.5.0

    Parameters
    ----------
    stepSize : float, optional
        Step size for each iteration of gradient descent.
        (default: 0.1)
    numIterations : int, optional
        Number of iterations run for each batch of data.
        (default: 50)
    miniBatchFraction : float, optional
        Fraction of each batch of data to use for updates.
        (default: 1.0)
    convergenceTol : float, optional
        Value used to determine when to terminate iterations.
        (default: 0.001)
    """

    def __init__(
        self,
        stepSize: float = 0.1,
        numIterations: int = 50,
        miniBatchFraction: float = 1.0,
        convergenceTol: float = 0.001,
    ):
        self.stepSize = stepSize
        self.numIterations = numIterations
        self.miniBatchFraction = miniBatchFraction
        self.convergenceTol = convergenceTol
        self._model: Optional[LinearModel] = None
        super(StreamingLinearRegressionWithSGD, self).__init__(model=self._model)

    @since("1.5.0")
    def setInitialWeights(self, initialWeights: "VectorLike") -> "StreamingLinearRegressionWithSGD":
        """
        Set the initial value of weights.

        This must be set before running trainOn and predictOn
        """
        initialWeights = _convert_to_vector(initialWeights)
        self._model = LinearRegressionModel(initialWeights, 0)
        return self

    @since("1.5.0")
    def trainOn(self, dstream: "DStream[LabeledPoint]") -> None:
        """Train the model on the incoming dstream."""
        self._validate(dstream)

        def update(rdd: RDD[LabeledPoint]) -> None:
            # LinearRegressionWithSGD.train raises an error for an empty RDD.
            if not rdd.isEmpty():
                assert self._model is not None
                self._model = LinearRegressionWithSGD.train(
                    rdd,
                    self.numIterations,
                    self.stepSize,
                    self.miniBatchFraction,
                    self._model.weights,
                    intercept=self._model.intercept,  # type: ignore[arg-type]
                    convergenceTol=self.convergenceTol,
                )

        dstream.foreachRDD(update)


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.mllib.regression

    globs = pyspark.mllib.regression.__dict__.copy()
    spark = SparkSession.builder.master("local[2]").appName("mllib.regression tests").getOrCreate()
    globs["sc"] = spark.sparkContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
