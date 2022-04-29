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

from math import exp
import sys
import warnings
from typing import Any, Iterable, Optional, Union, overload, TYPE_CHECKING

import numpy

from pyspark import RDD, SparkContext, since
from pyspark.streaming.dstream import DStream
from pyspark.mllib.common import callMLlibFunc, _py2java, _java2py
from pyspark.mllib.linalg import _convert_to_vector
from pyspark.mllib.regression import (
    LabeledPoint,
    LinearModel,
    _regression_train_wrapper,
    StreamingLinearAlgorithm,
)
from pyspark.mllib.util import Saveable, Loader, inherit_doc
from pyspark.mllib.linalg import Vector
from pyspark.mllib.regression import LabeledPoint

if TYPE_CHECKING:
    from pyspark.mllib._typing import VectorLike


__all__ = [
    "LogisticRegressionModel",
    "LogisticRegressionWithSGD",
    "LogisticRegressionWithLBFGS",
    "SVMModel",
    "SVMWithSGD",
    "NaiveBayesModel",
    "NaiveBayes",
    "StreamingLogisticRegressionWithSGD",
]


class LinearClassificationModel(LinearModel):
    """
    A private abstract class representing a multiclass classification
    model. The categories are represented by int values: 0, 1, 2, etc.
    """

    def __init__(self, weights: Vector, intercept: float) -> None:
        super(LinearClassificationModel, self).__init__(weights, intercept)
        self._threshold: Optional[float] = None

    @since("1.4.0")
    def setThreshold(self, value: float) -> None:
        """
        Sets the threshold that separates positive predictions from
        negative predictions. An example with prediction score greater
        than or equal to this threshold is identified as a positive,
        and negative otherwise. It is used for binary classification
        only.
        """
        self._threshold = value

    @property  # type: ignore[misc]
    @since("1.4.0")
    def threshold(self) -> Optional[float]:
        """
        Returns the threshold (if any) used for converting raw
        prediction scores into 0/1 predictions. It is used for
        binary classification only.
        """
        return self._threshold

    @since("1.4.0")
    def clearThreshold(self) -> None:
        """
        Clears the threshold so that `predict` will output raw
        prediction scores. It is used for binary classification only.
        """
        self._threshold = None

    @overload
    def predict(self, test: "VectorLike") -> Union[int, float]:
        ...

    @overload
    def predict(self, test: RDD["VectorLike"]) -> RDD[Union[int, float]]:
        ...

    def predict(
        self, test: Union["VectorLike", RDD["VectorLike"]]
    ) -> Union[RDD[Union[int, float]], Union[int, float]]:
        """
        Predict values for a single data point or an RDD of points
        using the model trained.

        .. versionadded:: 1.4.0
        """
        raise NotImplementedError


class LogisticRegressionModel(LinearClassificationModel):

    """
    Classification model trained using Multinomial/Binary Logistic
    Regression.

    .. versionadded:: 0.9.0

    Parameters
    ----------
    weights : :py:class:`pyspark.mllib.linalg.Vector`
        Weights computed for every feature.
    intercept : float
        Intercept computed for this model. (Only used in Binary Logistic
        Regression. In Multinomial Logistic Regression, the intercepts will
        not be a single value, so the intercepts will be part of the
        weights.)
    numFeatures : int
        The dimension of the features.
    numClasses : int
        The number of possible outcomes for k classes classification problem
        in Multinomial Logistic Regression. By default, it is binary
        logistic regression so numClasses will be set to 2.

    Examples
    --------
    >>> from pyspark.mllib.linalg import SparseVector
    >>> data = [
    ...     LabeledPoint(0.0, [0.0, 1.0]),
    ...     LabeledPoint(1.0, [1.0, 0.0]),
    ... ]
    >>> lrm = LogisticRegressionWithSGD.train(sc.parallelize(data), iterations=10)
    >>> lrm.predict([1.0, 0.0])
    1
    >>> lrm.predict([0.0, 1.0])
    0
    >>> lrm.predict(sc.parallelize([[1.0, 0.0], [0.0, 1.0]])).collect()
    [1, 0]
    >>> lrm.clearThreshold()
    >>> lrm.predict([0.0, 1.0])
    0.279...

    >>> sparse_data = [
    ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
    ...     LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
    ... ]
    >>> lrm = LogisticRegressionWithSGD.train(sc.parallelize(sparse_data), iterations=10)
    >>> lrm.predict(numpy.array([0.0, 1.0]))
    1
    >>> lrm.predict(numpy.array([1.0, 0.0]))
    0
    >>> lrm.predict(SparseVector(2, {1: 1.0}))
    1
    >>> lrm.predict(SparseVector(2, {0: 1.0}))
    0
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> lrm.save(sc, path)
    >>> sameModel = LogisticRegressionModel.load(sc, path)
    >>> sameModel.predict(numpy.array([0.0, 1.0]))
    1
    >>> sameModel.predict(SparseVector(2, {0: 1.0}))
    0
    >>> from shutil import rmtree
    >>> try:
    ...    rmtree(path)
    ... except BaseException:
    ...    pass
    >>> multi_class_data = [
    ...     LabeledPoint(0.0, [0.0, 1.0, 0.0]),
    ...     LabeledPoint(1.0, [1.0, 0.0, 0.0]),
    ...     LabeledPoint(2.0, [0.0, 0.0, 1.0])
    ... ]
    >>> data = sc.parallelize(multi_class_data)
    >>> mcm = LogisticRegressionWithLBFGS.train(data, iterations=10, numClasses=3)
    >>> mcm.predict([0.0, 0.5, 0.0])
    0
    >>> mcm.predict([0.8, 0.0, 0.0])
    1
    >>> mcm.predict([0.0, 0.0, 0.3])
    2
    """

    def __init__(
        self, weights: Vector, intercept: float, numFeatures: int, numClasses: int
    ) -> None:
        super(LogisticRegressionModel, self).__init__(weights, intercept)
        self._numFeatures = int(numFeatures)
        self._numClasses = int(numClasses)
        self._threshold = 0.5
        if self._numClasses == 2:
            self._dataWithBiasSize = None
            self._weightsMatrix = None
        else:
            self._dataWithBiasSize = self._coeff.size // (  # type: ignore[attr-defined]
                self._numClasses - 1
            )
            self._weightsMatrix = self._coeff.toArray().reshape(
                self._numClasses - 1, self._dataWithBiasSize
            )

    @property  # type: ignore[misc]
    @since("1.4.0")
    def numFeatures(self) -> int:
        """
        Dimension of the features.
        """
        return self._numFeatures

    @property  # type: ignore[misc]
    @since("1.4.0")
    def numClasses(self) -> int:
        """
        Number of possible outcomes for k classes classification problem
        in Multinomial Logistic Regression.
        """
        return self._numClasses

    @overload
    def predict(self, x: "VectorLike") -> Union[int, float]:
        ...

    @overload
    def predict(self, x: RDD["VectorLike"]) -> RDD[Union[int, float]]:
        ...

    def predict(
        self, x: Union["VectorLike", RDD["VectorLike"]]
    ) -> Union[RDD[Union[int, float]], Union[int, float]]:
        """
        Predict values for a single data point or an RDD of points
        using the model trained.

        .. versionadded:: 0.9.0
        """
        if isinstance(x, RDD):
            return x.map(lambda v: self.predict(v))

        x = _convert_to_vector(x)
        if self.numClasses == 2:
            margin = self.weights.dot(x) + self._intercept  # type: ignore[attr-defined]
            if margin > 0:
                prob = 1 / (1 + exp(-margin))
            else:
                exp_margin = exp(margin)
                prob = exp_margin / (1 + exp_margin)
            if self._threshold is None:
                return prob
            else:
                return 1 if prob > self._threshold else 0
        else:
            assert self._weightsMatrix is not None

            best_class = 0
            max_margin = 0.0
            if x.size + 1 == self._dataWithBiasSize:  # type: ignore[attr-defined]
                for i in range(0, self._numClasses - 1):
                    margin = (
                        x.dot(self._weightsMatrix[i][0 : x.size])  # type: ignore[attr-defined]
                        + self._weightsMatrix[i][x.size]  # type: ignore[attr-defined]
                    )
                    if margin > max_margin:
                        max_margin = margin
                        best_class = i + 1
            else:
                for i in range(0, self._numClasses - 1):
                    margin = x.dot(self._weightsMatrix[i])  # type: ignore[attr-defined]
                    if margin > max_margin:
                        max_margin = margin
                        best_class = i + 1
            return best_class

    @since("1.4.0")
    def save(self, sc: SparkContext, path: str) -> None:
        """
        Save this model to the given path.
        """
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.classification.LogisticRegressionModel(
            _py2java(sc, self._coeff), self.intercept, self.numFeatures, self.numClasses
        )
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since("1.4.0")
    def load(cls, sc: SparkContext, path: str) -> "LogisticRegressionModel":
        """
        Load a model from the given path.
        """
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.classification.LogisticRegressionModel.load(
            sc._jsc.sc(), path
        )
        weights = _java2py(sc, java_model.weights())
        intercept = java_model.intercept()
        numFeatures = java_model.numFeatures()
        numClasses = java_model.numClasses()
        threshold = java_model.getThreshold().get()
        model = LogisticRegressionModel(weights, intercept, numFeatures, numClasses)
        model.setThreshold(threshold)
        return model

    def __repr__(self) -> str:
        return (
            "pyspark.mllib.LogisticRegressionModel: intercept = {}, "
            "numFeatures = {}, numClasses = {}, threshold = {}"
        ).format(self._intercept, self._numFeatures, self._numClasses, self._threshold)


class LogisticRegressionWithSGD:
    """
    Train a classification model for Binary Logistic Regression using Stochastic Gradient Descent.

    .. versionadded:: 0.9.0
    .. deprecated:: 2.0.0
        Use ml.classification.LogisticRegression or LogisticRegressionWithLBFGS.
    """

    @classmethod
    def train(
        cls,
        data: RDD[LabeledPoint],
        iterations: int = 100,
        step: float = 1.0,
        miniBatchFraction: float = 1.0,
        initialWeights: Optional["VectorLike"] = None,
        regParam: float = 0.01,
        regType: str = "l2",
        intercept: bool = False,
        validateData: bool = True,
        convergenceTol: float = 0.001,
    ) -> LogisticRegressionModel:
        """
        Train a logistic regression model on the given data.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            The training data, an RDD of :py:class:`pyspark.mllib.regression.LabeledPoint`.
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
            (default: 0.01)
        regType : str, optional
            The type of regularizer used for training our model.
            Supported values:

            - "l1" for using L1 regularization
            - "l2" for using L2 regularization (default)
            - None for no regularization

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
        warnings.warn(
            "Deprecated in 2.0.0. Use ml.classification.LogisticRegression or "
            "LogisticRegressionWithLBFGS.",
            FutureWarning,
        )

        def train(rdd: RDD[LabeledPoint], i: Vector) -> Iterable[Any]:
            return callMLlibFunc(
                "trainLogisticRegressionModelWithSGD",
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

        return _regression_train_wrapper(train, LogisticRegressionModel, data, initialWeights)


class LogisticRegressionWithLBFGS:
    """
    Train a classification model for Multinomial/Binary Logistic Regression
    using Limited-memory BFGS.

    Standard feature scaling and L2 regularization are used by default.
    .. versionadded:: 1.2.0
    """

    @classmethod
    def train(
        cls,
        data: RDD[LabeledPoint],
        iterations: int = 100,
        initialWeights: Optional["VectorLike"] = None,
        regParam: float = 0.0,
        regType: str = "l2",
        intercept: bool = False,
        corrections: int = 10,
        tolerance: float = 1e-6,
        validateData: bool = True,
        numClasses: int = 2,
    ) -> LogisticRegressionModel:
        """
        Train a logistic regression model on the given data.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            The training data, an RDD of :py:class:`pyspark.mllib.regression.LabeledPoint`.
        iterations : int, optional
            The number of iterations.
            (default: 100)
        initialWeights : :py:class:`pyspark.mllib.linalg.Vector` or convertible, optional
            The initial weights.
            (default: None)
        regParam : float, optional
            The regularizer parameter.
            (default: 0.01)
        regType : str, optional
            The type of regularizer used for training our model.
            Supported values:

            - "l1" for using L1 regularization
            - "l2" for using L2 regularization (default)
            - None for no regularization

        intercept : bool, optional
            Boolean parameter which indicates the use or not of the
            augmented representation for training data (i.e., whether bias
            features are activated or not).
            (default: False)
        corrections : int, optional
            The number of corrections used in the LBFGS update.
            If a known updater is used for binary classification,
            it calls the ml implementation and this parameter will
            have no effect. (default: 10)
        tolerance : float, optional
            The convergence tolerance of iterations for L-BFGS.
            (default: 1e-6)
        validateData : bool, optional
            Boolean parameter which indicates if the algorithm should
            validate data before training.
            (default: True)
        numClasses : int, optional
            The number of classes (i.e., outcomes) a label can take in
            Multinomial Logistic Regression.
            (default: 2)

        Examples
        --------
        >>> data = [
        ...     LabeledPoint(0.0, [0.0, 1.0]),
        ...     LabeledPoint(1.0, [1.0, 0.0]),
        ... ]
        >>> lrm = LogisticRegressionWithLBFGS.train(sc.parallelize(data), iterations=10)
        >>> lrm.predict([1.0, 0.0])
        1
        >>> lrm.predict([0.0, 1.0])
        0
        """

        def train(rdd: RDD[LabeledPoint], i: Vector) -> Iterable[Any]:
            return callMLlibFunc(
                "trainLogisticRegressionModelWithLBFGS",
                rdd,
                int(iterations),
                i,
                float(regParam),
                regType,
                bool(intercept),
                int(corrections),
                float(tolerance),
                bool(validateData),
                int(numClasses),
            )

        if initialWeights is None:
            if numClasses == 2:
                initialWeights = [0.0] * len(data.first().features)
            else:
                if intercept:
                    initialWeights = [0.0] * (len(data.first().features) + 1) * (numClasses - 1)
                else:
                    initialWeights = [0.0] * len(data.first().features) * (numClasses - 1)
        return _regression_train_wrapper(train, LogisticRegressionModel, data, initialWeights)


class SVMModel(LinearClassificationModel):

    """
    Model for Support Vector Machines (SVMs).

    .. versionadded:: 0.9.0

    Parameters
    ----------
    weights : :py:class:`pyspark.mllib.linalg.Vector`
        Weights computed for every feature.
    intercept : float
        Intercept computed for this model.

    Examples
    --------
    >>> from pyspark.mllib.linalg import SparseVector
    >>> data = [
    ...     LabeledPoint(0.0, [0.0]),
    ...     LabeledPoint(1.0, [1.0]),
    ...     LabeledPoint(1.0, [2.0]),
    ...     LabeledPoint(1.0, [3.0])
    ... ]
    >>> svm = SVMWithSGD.train(sc.parallelize(data), iterations=10)
    >>> svm.predict([1.0])
    1
    >>> svm.predict(sc.parallelize([[1.0]])).collect()
    [1]
    >>> svm.clearThreshold()
    >>> svm.predict(numpy.array([1.0]))
    1.44...

    >>> sparse_data = [
    ...     LabeledPoint(0.0, SparseVector(2, {0: -1.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
    ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
    ... ]
    >>> svm = SVMWithSGD.train(sc.parallelize(sparse_data), iterations=10)
    >>> svm.predict(SparseVector(2, {1: 1.0}))
    1
    >>> svm.predict(SparseVector(2, {0: -1.0}))
    0
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> svm.save(sc, path)
    >>> sameModel = SVMModel.load(sc, path)
    >>> sameModel.predict(SparseVector(2, {1: 1.0}))
    1
    >>> sameModel.predict(SparseVector(2, {0: -1.0}))
    0
    >>> from shutil import rmtree
    >>> try:
    ...    rmtree(path)
    ... except BaseException:
    ...    pass
    """

    def __init__(self, weights: Vector, intercept: float) -> None:
        super(SVMModel, self).__init__(weights, intercept)
        self._threshold = 0.0

    @overload
    def predict(self, x: "VectorLike") -> Union[int, float]:
        ...

    @overload
    def predict(self, x: RDD["VectorLike"]) -> RDD[Union[int, float]]:
        ...

    def predict(
        self, x: Union["VectorLike", RDD["VectorLike"]]
    ) -> Union[RDD[Union[int, float]], Union[int, float]]:
        """
        Predict values for a single data point or an RDD of points
        using the model trained.

        .. versionadded:: 0.9.0
        """
        if isinstance(x, RDD):
            return x.map(lambda v: self.predict(v))

        x = _convert_to_vector(x)
        margin = self.weights.dot(x) + self.intercept  # type: ignore[attr-defined]
        if self._threshold is None:
            return margin
        else:
            return 1 if margin > self._threshold else 0

    @since("1.4.0")
    def save(self, sc: SparkContext, path: str) -> None:
        """
        Save this model to the given path.
        """
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.classification.SVMModel(
            _py2java(sc, self._coeff), self.intercept
        )
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since("1.4.0")
    def load(cls, sc: SparkContext, path: str) -> "SVMModel":
        """
        Load a model from the given path.
        """
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.classification.SVMModel.load(sc._jsc.sc(), path)
        weights = _java2py(sc, java_model.weights())
        intercept = java_model.intercept()
        threshold = java_model.getThreshold().get()
        model = SVMModel(weights, intercept)
        model.setThreshold(threshold)
        return model


class SVMWithSGD:
    """
    Train a Support Vector Machine (SVM) using Stochastic Gradient Descent.

    .. versionadded:: 0.9.0
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
        regType: str = "l2",
        intercept: bool = False,
        validateData: bool = True,
        convergenceTol: float = 0.001,
    ) -> SVMModel:
        """
        Train a support vector machine on the given data.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            The training data, an RDD of :py:class:`pyspark.mllib.regression.LabeledPoint`.
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
        regType : str, optional
            The type of regularizer used for training our model.
            Allowed values:

            - "l1" for using L1 regularization
            - "l2" for using L2 regularization (default)
            - None for no regularization

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

        def train(rdd: RDD[LabeledPoint], i: Vector) -> Iterable[Any]:
            return callMLlibFunc(
                "trainSVMModelWithSGD",
                rdd,
                int(iterations),
                float(step),
                float(regParam),
                float(miniBatchFraction),
                i,
                regType,
                bool(intercept),
                bool(validateData),
                float(convergenceTol),
            )

        return _regression_train_wrapper(train, SVMModel, data, initialWeights)


@inherit_doc
class NaiveBayesModel(Saveable, Loader["NaiveBayesModel"]):

    """
    Model for Naive Bayes classifiers.

    .. versionadded:: 0.9.0

    Parameters
    ----------
    labels : :py:class:`numpy.ndarray`
        List of labels.
    pi : :py:class:`numpy.ndarray`
        Log of class priors, whose dimension is C, number of labels.
    theta : :py:class:`numpy.ndarray`
        Log of class conditional probabilities, whose dimension is C-by-D,
        where D is number of features.

    Examples
    --------
    >>> from pyspark.mllib.linalg import SparseVector
    >>> data = [
    ...     LabeledPoint(0.0, [0.0, 0.0]),
    ...     LabeledPoint(0.0, [0.0, 1.0]),
    ...     LabeledPoint(1.0, [1.0, 0.0]),
    ... ]
    >>> model = NaiveBayes.train(sc.parallelize(data))
    >>> model.predict(numpy.array([0.0, 1.0]))
    0.0
    >>> model.predict(numpy.array([1.0, 0.0]))
    1.0
    >>> model.predict(sc.parallelize([[1.0, 0.0]])).collect()
    [1.0]
    >>> sparse_data = [
    ...     LabeledPoint(0.0, SparseVector(2, {1: 0.0})),
    ...     LabeledPoint(0.0, SparseVector(2, {1: 1.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {0: 1.0}))
    ... ]
    >>> model = NaiveBayes.train(sc.parallelize(sparse_data))
    >>> model.predict(SparseVector(2, {1: 1.0}))
    0.0
    >>> model.predict(SparseVector(2, {0: 1.0}))
    1.0
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> model.save(sc, path)
    >>> sameModel = NaiveBayesModel.load(sc, path)
    >>> sameModel.predict(SparseVector(2, {0: 1.0})) == model.predict(SparseVector(2, {0: 1.0}))
    True
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass
    """

    def __init__(self, labels: numpy.ndarray, pi: numpy.ndarray, theta: numpy.ndarray) -> None:
        self.labels = labels
        self.pi = pi
        self.theta = theta

    @overload
    def predict(self, x: "VectorLike") -> numpy.float64:
        ...

    @overload
    def predict(self, x: RDD["VectorLike"]) -> RDD[numpy.float64]:
        ...

    @since("0.9.0")
    def predict(
        self, x: Union["VectorLike", RDD["VectorLike"]]
    ) -> Union[numpy.float64, RDD[numpy.float64]]:
        """
        Return the most likely class for a data vector
        or an RDD of vectors
        """
        if isinstance(x, RDD):
            return x.map(lambda v: self.predict(v))
        x = _convert_to_vector(x)
        return self.labels[
            numpy.argmax(self.pi + x.dot(self.theta.transpose()))  # type: ignore[attr-defined]
        ]

    def save(self, sc: SparkContext, path: str) -> None:
        """
        Save this model to the given path.
        """
        assert sc._jvm is not None

        java_labels = _py2java(sc, self.labels.tolist())
        java_pi = _py2java(sc, self.pi.tolist())
        java_theta = _py2java(sc, self.theta.tolist())
        java_model = sc._jvm.org.apache.spark.mllib.classification.NaiveBayesModel(
            java_labels, java_pi, java_theta
        )
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since("1.4.0")
    def load(cls, sc: SparkContext, path: str) -> "NaiveBayesModel":
        """
        Load a model from the given path.
        """
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.classification.NaiveBayesModel.load(
            sc._jsc.sc(), path
        )
        # Can not unpickle array.array from Pickle in Python3 with "bytes"
        py_labels = _java2py(sc, java_model.labels(), "latin1")
        py_pi = _java2py(sc, java_model.pi(), "latin1")
        py_theta = _java2py(sc, java_model.theta(), "latin1")
        return NaiveBayesModel(py_labels, py_pi, numpy.array(py_theta))


class NaiveBayes:
    """
    Train a Multinomial Naive Bayes model.

    .. versionadded:: 0.9.0
    """

    @classmethod
    def train(cls, data: RDD[LabeledPoint], lambda_: float = 1.0) -> NaiveBayesModel:
        """
        Train a Naive Bayes model given an RDD of (label, features)
        vectors.

        This is the `Multinomial NB <http://tinyurl.com/lsdw6p>`_ which
        can handle all kinds of discrete data.  For example, by
        converting documents into TF-IDF vectors, it can be used for
        document classification. By making every vector a 0-1 vector,
        it can also be used as `Bernoulli NB <http://tinyurl.com/p7c96j6>`_.
        The input feature values must be nonnegative.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            The training data, an RDD of :py:class:`pyspark.mllib.regression.LabeledPoint`.
        lambda\\_ : float, optional
            The smoothing parameter.
            (default: 1.0)
        """
        first = data.first()
        if not isinstance(first, LabeledPoint):
            raise ValueError("`data` should be an RDD of LabeledPoint")
        labels, pi, theta = callMLlibFunc("trainNaiveBayesModel", data, lambda_)
        return NaiveBayesModel(labels.toArray(), pi.toArray(), numpy.array(theta))


@inherit_doc
class StreamingLogisticRegressionWithSGD(StreamingLinearAlgorithm):
    """
    Train or predict a logistic regression model on streaming data.
    Training uses Stochastic Gradient Descent to update the model based on
    each new batch of incoming data from a DStream.

    Each batch of data is assumed to be an RDD of LabeledPoints.
    The number of data points per batch can vary, but the number
    of features must be constant. An initial weight
    vector must be provided.

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
    regParam : float, optional
        L2 Regularization parameter.
        (default: 0.0)
    convergenceTol : float, optional
        Value used to determine when to terminate iterations.
        (default: 0.001)
    """

    def __init__(
        self,
        stepSize: float = 0.1,
        numIterations: int = 50,
        miniBatchFraction: float = 1.0,
        regParam: float = 0.0,
        convergenceTol: float = 0.001,
    ) -> None:
        self.stepSize = stepSize
        self.numIterations = numIterations
        self.regParam = regParam
        self.miniBatchFraction = miniBatchFraction
        self.convergenceTol = convergenceTol
        self._model: Optional[LogisticRegressionModel] = None
        super(StreamingLogisticRegressionWithSGD, self).__init__(model=self._model)

    @since("1.5.0")
    def setInitialWeights(
        self, initialWeights: "VectorLike"
    ) -> "StreamingLogisticRegressionWithSGD":
        """
        Set the initial value of weights.

        This must be set before running trainOn and predictOn.
        """
        initialWeights = _convert_to_vector(initialWeights)

        # LogisticRegressionWithSGD does only binary classification.
        self._model = LogisticRegressionModel(
            initialWeights, 0, initialWeights.size, 2  # type: ignore[attr-defined]
        )
        return self

    @since("1.5.0")
    def trainOn(self, dstream: "DStream[LabeledPoint]") -> None:
        """Train the model on the incoming dstream."""
        self._validate(dstream)

        def update(rdd: RDD[LabeledPoint]) -> None:
            # LogisticRegressionWithSGD.train raises an error for an empty RDD.
            if not rdd.isEmpty():
                self._model = LogisticRegressionWithSGD.train(
                    rdd,
                    self.numIterations,
                    self.stepSize,
                    self.miniBatchFraction,
                    self._model.weights,  # type: ignore[union-attr]
                    regParam=self.regParam,
                    convergenceTol=self.convergenceTol,
                )

        dstream.foreachRDD(update)


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.mllib.classification

    globs = pyspark.mllib.classification.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[4]").appName("mllib.classification tests").getOrCreate()
    )
    globs["sc"] = spark.sparkContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
