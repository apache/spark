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

import os
import operator
import sys
import uuid
import warnings
from abc import ABCMeta, abstractmethod
from multiprocessing.pool import ThreadPool
from functools import cached_property
from typing import (
    Any,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
    TYPE_CHECKING,
    Tuple,
    Callable,
)

from pyspark import keyword_only, since, inheritable_thread_target
from pyspark.ml import Estimator, Predictor, PredictionModel, Model
from pyspark.ml.param.shared import (
    HasRawPredictionCol,
    HasProbabilityCol,
    HasThresholds,
    HasRegParam,
    HasMaxIter,
    HasFitIntercept,
    HasTol,
    HasStandardization,
    HasWeightCol,
    HasAggregationDepth,
    HasThreshold,
    HasBlockSize,
    HasMaxBlockSizeInMB,
    Param,
    Params,
    TypeConverters,
    HasElasticNetParam,
    HasSeed,
    HasStepSize,
    HasSolver,
    HasParallelism,
)
from pyspark.ml.tree import (
    _DecisionTreeModel,
    _DecisionTreeParams,
    _TreeEnsembleModel,
    _RandomForestParams,
    _GBTParams,
    _HasVarianceImpurity,
    _TreeClassifierParams,
)
from pyspark.ml.regression import _FactorizationMachinesParams, DecisionTreeRegressionModel
from pyspark.ml.base import _PredictorParams
from pyspark.ml.util import (
    DefaultParamsReader,
    DefaultParamsWriter,
    JavaMLReadable,
    JavaMLReader,
    JavaMLWritable,
    JavaMLWriter,
    MLReader,
    MLReadable,
    MLWriter,
    MLWritable,
    HasTrainingSummary,
    try_remote_read,
    try_remote_write,
    try_remote_attribute_relation,
)
from pyspark.ml.wrapper import JavaParams, JavaPredictor, JavaPredictionModel, JavaWrapper
from pyspark.ml.common import inherit_doc
from pyspark.ml.linalg import Matrix, Vector, Vectors, VectorUDT
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import udf, when
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.storagelevel import StorageLevel
from pyspark.sql.utils import is_remote

if TYPE_CHECKING:
    from pyspark.ml._typing import P, ParamMap
    from py4j.java_gateway import JavaObject
    from pyspark.core.context import SparkContext


T = TypeVar("T")
JPM = TypeVar("JPM", bound=JavaPredictionModel)
CM = TypeVar("CM", bound="ClassificationModel")

__all__ = [
    "LinearSVC",
    "LinearSVCModel",
    "LinearSVCSummary",
    "LinearSVCTrainingSummary",
    "LogisticRegression",
    "LogisticRegressionModel",
    "LogisticRegressionSummary",
    "LogisticRegressionTrainingSummary",
    "BinaryLogisticRegressionSummary",
    "BinaryLogisticRegressionTrainingSummary",
    "DecisionTreeClassifier",
    "DecisionTreeClassificationModel",
    "GBTClassifier",
    "GBTClassificationModel",
    "RandomForestClassifier",
    "RandomForestClassificationModel",
    "RandomForestClassificationSummary",
    "RandomForestClassificationTrainingSummary",
    "BinaryRandomForestClassificationSummary",
    "BinaryRandomForestClassificationTrainingSummary",
    "NaiveBayes",
    "NaiveBayesModel",
    "MultilayerPerceptronClassifier",
    "MultilayerPerceptronClassificationModel",
    "MultilayerPerceptronClassificationSummary",
    "MultilayerPerceptronClassificationTrainingSummary",
    "OneVsRest",
    "OneVsRestModel",
    "FMClassifier",
    "FMClassificationModel",
    "FMClassificationSummary",
    "FMClassificationTrainingSummary",
]


class _ClassifierParams(HasRawPredictionCol, _PredictorParams):
    """
    Classifier Params for classification tasks.

    .. versionadded:: 3.0.0
    """

    pass


@inherit_doc
class Classifier(Predictor[CM], _ClassifierParams, Generic[CM], metaclass=ABCMeta):
    """
    Classifier for classification tasks.
    Classes are indexed {0, 1, ..., numClasses - 1}.
    """

    @since("3.0.0")
    def setRawPredictionCol(self: "P", value: str) -> "P":
        """
        Sets the value of :py:attr:`rawPredictionCol`.
        """
        return self._set(rawPredictionCol=value)


@inherit_doc
class ClassificationModel(PredictionModel, _ClassifierParams, metaclass=ABCMeta):
    """
    Model produced by a ``Classifier``.
    Classes are indexed {0, 1, ..., numClasses - 1}.
    """

    @since("3.0.0")
    def setRawPredictionCol(self: "P", value: str) -> "P":
        """
        Sets the value of :py:attr:`rawPredictionCol`.
        """
        return self._set(rawPredictionCol=value)

    @property
    @abstractmethod
    @since("2.1.0")
    def numClasses(self) -> int:
        """
        Number of classes (values which the label can take).
        """
        raise NotImplementedError()

    @abstractmethod
    @since("3.0.0")
    def predictRaw(self, value: Vector) -> Vector:
        """
        Raw prediction for each possible label.
        """
        raise NotImplementedError()


class _ProbabilisticClassifierParams(HasProbabilityCol, HasThresholds, _ClassifierParams):
    """
    Params for :py:class:`ProbabilisticClassifier` and
    :py:class:`ProbabilisticClassificationModel`.

    .. versionadded:: 3.0.0
    """

    pass


@inherit_doc
class ProbabilisticClassifier(Classifier, _ProbabilisticClassifierParams, metaclass=ABCMeta):
    """
    Probabilistic Classifier for classification tasks.
    """

    @since("3.0.0")
    def setProbabilityCol(self: "P", value: str) -> "P":
        """
        Sets the value of :py:attr:`probabilityCol`.
        """
        return self._set(probabilityCol=value)

    @since("3.0.0")
    def setThresholds(self: "P", value: List[float]) -> "P":
        """
        Sets the value of :py:attr:`thresholds`.
        """
        return self._set(thresholds=value)


@inherit_doc
class ProbabilisticClassificationModel(
    ClassificationModel, _ProbabilisticClassifierParams, metaclass=ABCMeta
):
    """
    Model produced by a ``ProbabilisticClassifier``.
    """

    @since("3.0.0")
    def setProbabilityCol(self: CM, value: str) -> CM:
        """
        Sets the value of :py:attr:`probabilityCol`.
        """
        return self._set(probabilityCol=value)

    @since("3.0.0")
    def setThresholds(self: CM, value: List[float]) -> CM:
        """
        Sets the value of :py:attr:`thresholds`.
        """
        return self._set(thresholds=value)

    @abstractmethod
    @since("3.0.0")
    def predictProbability(self, value: Vector) -> Vector:
        """
        Predict the probability of each class given the features.
        """
        raise NotImplementedError()


@inherit_doc
class _JavaClassifier(Classifier, JavaPredictor[JPM], Generic[JPM], metaclass=ABCMeta):
    """
    Java Classifier for classification tasks.
    Classes are indexed {0, 1, ..., numClasses - 1}.
    """

    @since("3.0.0")
    def setRawPredictionCol(self: "P", value: str) -> "P":
        """
        Sets the value of :py:attr:`rawPredictionCol`.
        """
        return self._set(rawPredictionCol=value)


@inherit_doc
class _JavaClassificationModel(ClassificationModel, JavaPredictionModel[T]):
    """
    Java Model produced by a ``Classifier``.
    Classes are indexed {0, 1, ..., numClasses - 1}.
    To be mixed in with :class:`pyspark.ml.JavaModel`
    """

    @property
    @since("2.1.0")
    def numClasses(self) -> int:
        """
        Number of classes (values which the label can take).
        """
        return self._call_java("numClasses")

    @since("3.0.0")
    def predictRaw(self, value: Vector) -> Vector:
        """
        Raw prediction for each possible label.
        """
        return self._call_java("predictRaw", value)


@inherit_doc
class _JavaProbabilisticClassifier(
    ProbabilisticClassifier, _JavaClassifier[JPM], Generic[JPM], metaclass=ABCMeta
):
    """
    Java Probabilistic Classifier for classification tasks.
    """

    pass


@inherit_doc
class _JavaProbabilisticClassificationModel(
    ProbabilisticClassificationModel, _JavaClassificationModel[T]
):
    """
    Java Model produced by a ``ProbabilisticClassifier``.
    """

    @since("3.0.0")
    def predictProbability(self, value: Vector) -> Vector:
        """
        Predict the probability of each class given the features.
        """
        return self._call_java("predictProbability", value)


@inherit_doc
class _ClassificationSummary(JavaWrapper):
    """
    Abstraction for multiclass classification results for a given model.

    .. versionadded:: 3.1.0
    """

    @property
    @since("3.1.0")
    @try_remote_attribute_relation
    def predictions(self) -> DataFrame:
        """
        Dataframe outputted by the model's `transform` method.
        """
        return self._call_java("predictions")

    @property
    @since("3.1.0")
    def predictionCol(self) -> str:
        """
        Field in "predictions" which gives the prediction of each class.
        """
        return self._call_java("predictionCol")

    @property
    @since("3.1.0")
    def labelCol(self) -> str:
        """
        Field in "predictions" which gives the true label of each
        instance.
        """
        return self._call_java("labelCol")

    @property
    @since("3.1.0")
    def weightCol(self) -> str:
        """
        Field in "predictions" which gives the weight of each instance
        as a vector.
        """
        return self._call_java("weightCol")

    @property
    def labels(self) -> List[str]:
        """
        Returns the sequence of labels in ascending order. This order matches the order used
        in metrics which are specified as arrays over labels, e.g., truePositiveRateByLabel.

        .. versionadded:: 3.1.0

        Notes
        -----
        In most cases, it will be values {0.0, 1.0, ..., numClasses-1}, However, if the
        training set is missing a label, then all of the arrays over labels
        (e.g., from truePositiveRateByLabel) will be of length numClasses-1 instead of the
        expected numClasses.
        """
        return self._call_java("labels")

    @property
    @since("3.1.0")
    def truePositiveRateByLabel(self) -> List[float]:
        """
        Returns true positive rate for each label (category).
        """
        return self._call_java("truePositiveRateByLabel")

    @property
    @since("3.1.0")
    def falsePositiveRateByLabel(self) -> List[float]:
        """
        Returns false positive rate for each label (category).
        """
        return self._call_java("falsePositiveRateByLabel")

    @property
    @since("3.1.0")
    def precisionByLabel(self) -> List[float]:
        """
        Returns precision for each label (category).
        """
        return self._call_java("precisionByLabel")

    @property
    @since("3.1.0")
    def recallByLabel(self) -> List[float]:
        """
        Returns recall for each label (category).
        """
        return self._call_java("recallByLabel")

    @since("3.1.0")
    def fMeasureByLabel(self, beta: float = 1.0) -> List[float]:
        """
        Returns f-measure for each label (category).
        """
        return self._call_java("fMeasureByLabel", beta)

    @property
    @since("3.1.0")
    def accuracy(self) -> float:
        """
        Returns accuracy.
        (equals to the total number of correctly classified instances
        out of the total number of instances.)
        """
        return self._call_java("accuracy")

    @property
    @since("3.1.0")
    def weightedTruePositiveRate(self) -> float:
        """
        Returns weighted true positive rate.
        (equals to precision, recall and f-measure)
        """
        return self._call_java("weightedTruePositiveRate")

    @property
    @since("3.1.0")
    def weightedFalsePositiveRate(self) -> float:
        """
        Returns weighted false positive rate.
        """
        return self._call_java("weightedFalsePositiveRate")

    @property
    @since("3.1.0")
    def weightedRecall(self) -> float:
        """
        Returns weighted averaged recall.
        (equals to precision, recall and f-measure)
        """
        return self._call_java("weightedRecall")

    @property
    @since("3.1.0")
    def weightedPrecision(self) -> float:
        """
        Returns weighted averaged precision.
        """
        return self._call_java("weightedPrecision")

    @since("3.1.0")
    def weightedFMeasure(self, beta: float = 1.0) -> float:
        """
        Returns weighted averaged f-measure.
        """
        return self._call_java("weightedFMeasure", beta)


@inherit_doc
class _TrainingSummary(JavaWrapper):
    """
    Abstraction for Training results.

    .. versionadded:: 3.1.0
    """

    @property
    @since("3.1.0")
    def objectiveHistory(self) -> List[float]:
        """
        Objective function (scaled loss + regularization) at each
        iteration. It contains one more element, the initial state,
        than number of iterations.
        """
        return self._call_java("objectiveHistory")

    @property
    @since("3.1.0")
    def totalIterations(self) -> int:
        """
        Number of training iterations until termination.
        """
        return self._call_java("totalIterations")


@inherit_doc
class _BinaryClassificationSummary(_ClassificationSummary):
    """
    Binary classification results for a given model.

    .. versionadded:: 3.1.0
    """

    @property
    @since("3.1.0")
    def scoreCol(self) -> str:
        """
        Field in "predictions" which gives the probability or raw prediction
        of each class as a vector.
        """
        return self._call_java("scoreCol")

    @property
    @try_remote_attribute_relation
    def roc(self) -> DataFrame:
        """
        Returns the receiver operating characteristic (ROC) curve,
        which is a Dataframe having two fields (FPR, TPR) with
        (0.0, 0.0) prepended and (1.0, 1.0) appended to it.

        .. versionadded:: 3.1.0

        Notes
        -----
        `Wikipedia reference <http://en.wikipedia.org/wiki/Receiver_operating_characteristic>`_
        """
        return self._call_java("roc")

    @property
    @since("3.1.0")
    def areaUnderROC(self) -> float:
        """
        Computes the area under the receiver operating characteristic
        (ROC) curve.
        """
        return self._call_java("areaUnderROC")

    @property
    @since("3.1.0")
    @try_remote_attribute_relation
    def pr(self) -> DataFrame:
        """
        Returns the precision-recall curve, which is a Dataframe
        containing two fields recall, precision with (0.0, 1.0) prepended
        to it.
        """
        return self._call_java("pr")

    @property
    @since("3.1.0")
    @try_remote_attribute_relation
    def fMeasureByThreshold(self) -> DataFrame:
        """
        Returns a dataframe with two fields (threshold, F-Measure) curve
        with beta = 1.0.
        """
        return self._call_java("fMeasureByThreshold")

    @property
    @since("3.1.0")
    @try_remote_attribute_relation
    def precisionByThreshold(self) -> DataFrame:
        """
        Returns a dataframe with two fields (threshold, precision) curve.
        Every possible probability obtained in transforming the dataset
        are used as thresholds used in calculating the precision.
        """
        return self._call_java("precisionByThreshold")

    @property
    @since("3.1.0")
    @try_remote_attribute_relation
    def recallByThreshold(self) -> DataFrame:
        """
        Returns a dataframe with two fields (threshold, recall) curve.
        Every possible probability obtained in transforming the dataset
        are used as thresholds used in calculating the recall.
        """
        return self._call_java("recallByThreshold")


class _LinearSVCParams(
    _ClassifierParams,
    HasRegParam,
    HasMaxIter,
    HasFitIntercept,
    HasTol,
    HasStandardization,
    HasWeightCol,
    HasAggregationDepth,
    HasThreshold,
    HasMaxBlockSizeInMB,
):
    """
    Params for :py:class:`LinearSVC` and :py:class:`LinearSVCModel`.

    .. versionadded:: 3.0.0
    """

    threshold: Param[float] = Param(
        Params._dummy(),
        "threshold",
        "The threshold in binary classification applied to the linear model"
        " prediction.  This threshold can be any real number, where Inf will make"
        " all predictions 0.0 and -Inf will make all predictions 1.0.",
        typeConverter=TypeConverters.toFloat,
    )

    def __init__(self, *args: Any) -> None:
        super(_LinearSVCParams, self).__init__(*args)
        self._setDefault(
            maxIter=100,
            regParam=0.0,
            tol=1e-6,
            fitIntercept=True,
            standardization=True,
            threshold=0.0,
            aggregationDepth=2,
            maxBlockSizeInMB=0.0,
        )


@inherit_doc
class LinearSVC(
    _JavaClassifier["LinearSVCModel"],
    _LinearSVCParams,
    JavaMLWritable,
    JavaMLReadable["LinearSVC"],
):
    """
    This binary classifier optimizes the Hinge Loss using the OWLQN optimizer.
    Only supports L2 regularization currently.

    .. versionadded:: 2.2.0

    Notes
    -----
    `Linear SVM Classifier <https://en.wikipedia.org/wiki/Support_vector_machine#Linear_SVM>`_

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> from pyspark.ml.linalg import Vectors
    >>> df = sc.parallelize([
    ...     Row(label=1.0, features=Vectors.dense(1.0, 1.0, 1.0)),
    ...     Row(label=0.0, features=Vectors.dense(1.0, 2.0, 3.0))]).toDF()
    >>> svm = LinearSVC()
    >>> svm.getMaxIter()
    100
    >>> svm.setMaxIter(5)
    LinearSVC...
    >>> svm.getMaxIter()
    5
    >>> svm.getRegParam()
    0.0
    >>> svm.setRegParam(0.01)
    LinearSVC...
    >>> svm.getRegParam()
    0.01
    >>> model = svm.fit(df)
    >>> model.setPredictionCol("newPrediction")
    LinearSVCModel...
    >>> model.getPredictionCol()
    'newPrediction'
    >>> model.setThreshold(0.5)
    LinearSVCModel...
    >>> model.getThreshold()
    0.5
    >>> model.getMaxBlockSizeInMB()
    0.0
    >>> model.coefficients
    DenseVector([0.0, -1.0319, -0.5159])
    >>> model.intercept
    2.579645978780695
    >>> model.numClasses
    2
    >>> model.numFeatures
    3
    >>> test0 = sc.parallelize([Row(features=Vectors.dense(-1.0, -1.0, -1.0))]).toDF()
    >>> model.predict(test0.head().features)
    1.0
    >>> model.predictRaw(test0.head().features)
    DenseVector([-4.1274, 4.1274])
    >>> result = model.transform(test0).head()
    >>> result.newPrediction
    1.0
    >>> result.rawPrediction
    DenseVector([-4.1274, 4.1274])
    >>> svm_path = temp_path + "/svm"
    >>> svm.save(svm_path)
    >>> svm2 = LinearSVC.load(svm_path)
    >>> svm2.getMaxIter()
    5
    >>> model_path = temp_path + "/svm_model"
    >>> model.save(model_path)
    >>> model2 = LinearSVCModel.load(model_path)
    >>> bool(model.coefficients[0] == model2.coefficients[0])
    True
    >>> model.intercept == model2.intercept
    True
    >>> model.transform(test0).take(1) == model2.transform(test0).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        maxIter: int = 100,
        regParam: float = 0.0,
        tol: float = 1e-6,
        rawPredictionCol: str = "rawPrediction",
        fitIntercept: bool = True,
        standardization: bool = True,
        threshold: float = 0.0,
        weightCol: Optional[str] = None,
        aggregationDepth: int = 2,
        maxBlockSizeInMB: float = 0.0,
    ):
        """
        __init__(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, regParam=0.0, tol=1e-6, rawPredictionCol="rawPrediction", \
                 fitIntercept=True, standardization=True, threshold=0.0, weightCol=None, \
                 aggregationDepth=2, maxBlockSizeInMB=0.0):
        """
        super(LinearSVC, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.LinearSVC", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.2.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        maxIter: int = 100,
        regParam: float = 0.0,
        tol: float = 1e-6,
        rawPredictionCol: str = "rawPrediction",
        fitIntercept: bool = True,
        standardization: bool = True,
        threshold: float = 0.0,
        weightCol: Optional[str] = None,
        aggregationDepth: int = 2,
        maxBlockSizeInMB: float = 0.0,
    ) -> "LinearSVC":
        """
        setParams(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, regParam=0.0, tol=1e-6, rawPredictionCol="rawPrediction", \
                  fitIntercept=True, standardization=True, threshold=0.0, weightCol=None, \
                  aggregationDepth=2, maxBlockSizeInMB=0.0):
        Sets params for Linear SVM Classifier.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> "LinearSVCModel":
        return LinearSVCModel(java_model)

    @since("2.2.0")
    def setMaxIter(self, value: int) -> "LinearSVC":
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("2.2.0")
    def setRegParam(self, value: float) -> "LinearSVC":
        """
        Sets the value of :py:attr:`regParam`.
        """
        return self._set(regParam=value)

    @since("2.2.0")
    def setTol(self, value: float) -> "LinearSVC":
        """
        Sets the value of :py:attr:`tol`.
        """
        return self._set(tol=value)

    @since("2.2.0")
    def setFitIntercept(self, value: bool) -> "LinearSVC":
        """
        Sets the value of :py:attr:`fitIntercept`.
        """
        return self._set(fitIntercept=value)

    @since("2.2.0")
    def setStandardization(self, value: bool) -> "LinearSVC":
        """
        Sets the value of :py:attr:`standardization`.
        """
        return self._set(standardization=value)

    @since("2.2.0")
    def setThreshold(self, value: float) -> "LinearSVC":
        """
        Sets the value of :py:attr:`threshold`.
        """
        return self._set(threshold=value)

    @since("2.2.0")
    def setWeightCol(self, value: str) -> "LinearSVC":
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @since("2.2.0")
    def setAggregationDepth(self, value: int) -> "LinearSVC":
        """
        Sets the value of :py:attr:`aggregationDepth`.
        """
        return self._set(aggregationDepth=value)

    @since("3.1.0")
    def setMaxBlockSizeInMB(self, value: float) -> "LinearSVC":
        """
        Sets the value of :py:attr:`maxBlockSizeInMB`.
        """
        return self._set(maxBlockSizeInMB=value)


class LinearSVCModel(
    _JavaClassificationModel[Vector],
    _LinearSVCParams,
    JavaMLWritable,
    JavaMLReadable["LinearSVCModel"],
    HasTrainingSummary["LinearSVCTrainingSummary"],
):
    """
    Model fitted by LinearSVC.

    .. versionadded:: 2.2.0
    """

    @since("3.0.0")
    def setThreshold(self, value: float) -> "LinearSVCModel":
        """
        Sets the value of :py:attr:`threshold`.
        """
        return self._set(threshold=value)

    @property
    @since("2.2.0")
    def coefficients(self) -> Vector:
        """
        Model coefficients of Linear SVM Classifier.
        """
        return self._call_java("coefficients")

    @property
    @since("2.2.0")
    def intercept(self) -> float:
        """
        Model intercept of Linear SVM Classifier.
        """
        return self._call_java("intercept")

    @since("3.1.0")
    def summary(self) -> "LinearSVCTrainingSummary":  # type: ignore[override]
        """
        Gets summary (accuracy/precision/recall, objective history, total iterations) of model
        trained on the training set. An exception is thrown if `trainingSummary is None`.
        """
        if self.hasSummary:
            return LinearSVCTrainingSummary(super(LinearSVCModel, self).summary)
        else:
            raise RuntimeError(
                "No training summary available for this %s" % self.__class__.__name__
            )

    def evaluate(self, dataset: DataFrame) -> "LinearSVCSummary":
        """
        Evaluates the model on a test dataset.

        .. versionadded:: 3.1.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            Test dataset to evaluate model on.
        """
        if not isinstance(dataset, DataFrame):
            raise TypeError("dataset must be a DataFrame but got %s." % type(dataset))
        java_lsvc_summary = self._call_java("evaluate", dataset)
        return LinearSVCSummary(java_lsvc_summary)


class LinearSVCSummary(_BinaryClassificationSummary):
    """
    Abstraction for LinearSVC Results for a given model.

    .. versionadded:: 3.1.0
    """

    pass


@inherit_doc
class LinearSVCTrainingSummary(LinearSVCSummary, _TrainingSummary):
    """
    Abstraction for LinearSVC Training results.

    .. versionadded:: 3.1.0
    """

    pass


class _LogisticRegressionParams(
    _ProbabilisticClassifierParams,
    HasRegParam,
    HasElasticNetParam,
    HasMaxIter,
    HasFitIntercept,
    HasTol,
    HasStandardization,
    HasWeightCol,
    HasAggregationDepth,
    HasThreshold,
    HasMaxBlockSizeInMB,
):
    """
    Params for :py:class:`LogisticRegression` and :py:class:`LogisticRegressionModel`.

    .. versionadded:: 3.0.0
    """

    threshold: Param[float] = Param(
        Params._dummy(),
        "threshold",
        "Threshold in binary classification prediction, in range [0, 1]."
        + " If threshold and thresholds are both set, they must match."
        + "e.g. if threshold is p, then thresholds must be equal to [1-p, p].",
        typeConverter=TypeConverters.toFloat,
    )

    family: Param[str] = Param(
        Params._dummy(),
        "family",
        "The name of family which is a description of the label distribution to "
        + "be used in the model. Supported options: auto, binomial, multinomial",
        typeConverter=TypeConverters.toString,
    )

    lowerBoundsOnCoefficients: Param[Matrix] = Param(
        Params._dummy(),
        "lowerBoundsOnCoefficients",
        "The lower bounds on coefficients if fitting under bound "
        "constrained optimization. The bound matrix must be "
        "compatible with the shape "
        "(1, number of features) for binomial regression, or "
        "(number of classes, number of features) "
        "for multinomial regression.",
        typeConverter=TypeConverters.toMatrix,
    )

    upperBoundsOnCoefficients: Param[Matrix] = Param(
        Params._dummy(),
        "upperBoundsOnCoefficients",
        "The upper bounds on coefficients if fitting under bound "
        "constrained optimization. The bound matrix must be "
        "compatible with the shape "
        "(1, number of features) for binomial regression, or "
        "(number of classes, number of features) "
        "for multinomial regression.",
        typeConverter=TypeConverters.toMatrix,
    )

    lowerBoundsOnIntercepts: Param[Vector] = Param(
        Params._dummy(),
        "lowerBoundsOnIntercepts",
        "The lower bounds on intercepts if fitting under bound "
        "constrained optimization. The bounds vector size must be"
        "equal with 1 for binomial regression, or the number of"
        "lasses for multinomial regression.",
        typeConverter=TypeConverters.toVector,
    )

    upperBoundsOnIntercepts: Param[Vector] = Param(
        Params._dummy(),
        "upperBoundsOnIntercepts",
        "The upper bounds on intercepts if fitting under bound "
        "constrained optimization. The bound vector size must be "
        "equal with 1 for binomial regression, or the number of "
        "classes for multinomial regression.",
        typeConverter=TypeConverters.toVector,
    )

    def __init__(self, *args: Any):
        super(_LogisticRegressionParams, self).__init__(*args)
        self._setDefault(
            maxIter=100, regParam=0.0, tol=1e-6, threshold=0.5, family="auto", maxBlockSizeInMB=0.0
        )

    @since("1.4.0")
    def setThreshold(self: "P", value: float) -> "P":
        """
        Sets the value of :py:attr:`threshold`.
        Clears value of :py:attr:`thresholds` if it has been set.
        """
        self._set(threshold=value)
        self.clear(self.thresholds)  # type: ignore[attr-defined]
        return self

    @since("1.4.0")
    def getThreshold(self) -> float:
        """
        Get threshold for binary classification.

        If :py:attr:`thresholds` is set with length 2 (i.e., binary classification),
        this returns the equivalent threshold:
        :math:`\\frac{1}{1 + \\frac{thresholds(0)}{thresholds(1)}}`.
        Otherwise, returns :py:attr:`threshold` if set or its default value if unset.
        """
        self._checkThresholdConsistency()
        if self.isSet(self.thresholds):
            ts = self.getOrDefault(self.thresholds)
            if len(ts) != 2:
                raise ValueError(
                    "Logistic Regression getThreshold only applies to"
                    + " binary classification, but thresholds has length != 2."
                    + "  thresholds: {ts}".format(ts=ts)
                )
            return 1.0 / (1.0 + ts[0] / ts[1])
        else:
            return self.getOrDefault(self.threshold)

    @since("1.5.0")
    def setThresholds(self: "P", value: List[float]) -> "P":
        """
        Sets the value of :py:attr:`thresholds`.
        Clears value of :py:attr:`threshold` if it has been set.
        """
        self._set(thresholds=value)
        self.clear(self.threshold)  # type: ignore[attr-defined]
        return self

    @since("1.5.0")
    def getThresholds(self) -> List[float]:
        """
        If :py:attr:`thresholds` is set, return its value.
        Otherwise, if :py:attr:`threshold` is set, return the equivalent thresholds for binary
        classification: (1-threshold, threshold).
        If neither are set, throw an error.
        """
        self._checkThresholdConsistency()
        if not self.isSet(self.thresholds) and self.isSet(self.threshold):
            t = self.getOrDefault(self.threshold)
            return [1.0 - t, t]
        else:
            return self.getOrDefault(self.thresholds)

    def _checkThresholdConsistency(self) -> None:
        if self.isSet(self.threshold) and self.isSet(self.thresholds):
            ts = self.getOrDefault(self.thresholds)
            if len(ts) != 2:
                raise ValueError(
                    "Logistic Regression getThreshold only applies to"
                    + " binary classification, but thresholds has length != 2."
                    + " thresholds: {0}".format(str(ts))
                )
            t = 1.0 / (1.0 + ts[0] / ts[1])
            t2 = self.getOrDefault(self.threshold)
            if abs(t2 - t) >= 1e-5:
                raise ValueError(
                    "Logistic Regression getThreshold found inconsistent values for"
                    + " threshold (%g) and thresholds (equivalent to %g)" % (t2, t)
                )

    @since("2.1.0")
    def getFamily(self) -> str:
        """
        Gets the value of :py:attr:`family` or its default value.
        """
        return self.getOrDefault(self.family)

    @since("2.3.0")
    def getLowerBoundsOnCoefficients(self) -> Matrix:
        """
        Gets the value of :py:attr:`lowerBoundsOnCoefficients`
        """
        return self.getOrDefault(self.lowerBoundsOnCoefficients)

    @since("2.3.0")
    def getUpperBoundsOnCoefficients(self) -> Matrix:
        """
        Gets the value of :py:attr:`upperBoundsOnCoefficients`
        """
        return self.getOrDefault(self.upperBoundsOnCoefficients)

    @since("2.3.0")
    def getLowerBoundsOnIntercepts(self) -> Vector:
        """
        Gets the value of :py:attr:`lowerBoundsOnIntercepts`
        """
        return self.getOrDefault(self.lowerBoundsOnIntercepts)

    @since("2.3.0")
    def getUpperBoundsOnIntercepts(self) -> Vector:
        """
        Gets the value of :py:attr:`upperBoundsOnIntercepts`
        """
        return self.getOrDefault(self.upperBoundsOnIntercepts)


@inherit_doc
class LogisticRegression(
    _JavaProbabilisticClassifier["LogisticRegressionModel"],
    _LogisticRegressionParams,
    JavaMLWritable,
    JavaMLReadable["LogisticRegression"],
):
    """
    Logistic regression.
    This class supports multinomial logistic (softmax) and binomial logistic regression.

    .. versionadded:: 1.3.0

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> from pyspark.ml.linalg import Vectors
    >>> bdf = sc.parallelize([
    ...     Row(label=1.0, weight=1.0, features=Vectors.dense(0.0, 5.0)),
    ...     Row(label=0.0, weight=2.0, features=Vectors.dense(1.0, 2.0)),
    ...     Row(label=1.0, weight=3.0, features=Vectors.dense(2.0, 1.0)),
    ...     Row(label=0.0, weight=4.0, features=Vectors.dense(3.0, 3.0))]).toDF()
    >>> blor = LogisticRegression(weightCol="weight")
    >>> blor.getRegParam()
    0.0
    >>> blor.setRegParam(0.01)
    LogisticRegression...
    >>> blor.getRegParam()
    0.01
    >>> blor.setMaxIter(10)
    LogisticRegression...
    >>> blor.getMaxIter()
    10
    >>> blor.clear(blor.maxIter)
    >>> blorModel = blor.fit(bdf)
    >>> blorModel.setFeaturesCol("features")
    LogisticRegressionModel...
    >>> blorModel.setProbabilityCol("newProbability")
    LogisticRegressionModel...
    >>> blorModel.getProbabilityCol()
    'newProbability'
    >>> blorModel.getMaxBlockSizeInMB()
    0.0
    >>> blorModel.setThreshold(0.1)
    LogisticRegressionModel...
    >>> blorModel.getThreshold()
    0.1
    >>> blorModel.coefficients
    DenseVector([-1.080..., -0.646...])
    >>> blorModel.intercept
    3.112...
    >>> blorModel.evaluate(bdf).accuracy == blorModel.summary.accuracy
    True
    >>> data_path = "data/mllib/sample_multiclass_classification_data.txt"
    >>> mdf = spark.read.format("libsvm").load(data_path)
    >>> mlor = LogisticRegression(regParam=0.1, elasticNetParam=1.0, family="multinomial")
    >>> mlorModel = mlor.fit(mdf)
    >>> mlorModel.coefficientMatrix
    SparseMatrix(3, 4, [0, 1, 2, 3], [3, 2, 1], [1.87..., -2.75..., -0.50...], 1)
    >>> mlorModel.interceptVector
    DenseVector([0.04..., -0.42..., 0.37...])
    >>> test0 = sc.parallelize([Row(features=Vectors.dense(-1.0, 1.0))]).toDF()
    >>> blorModel.predict(test0.head().features)
    1.0
    >>> blorModel.predictRaw(test0.head().features)
    DenseVector([-3.54..., 3.54...])
    >>> blorModel.predictProbability(test0.head().features)
    DenseVector([0.028, 0.972])
    >>> result = blorModel.transform(test0).head()
    >>> result.prediction
    1.0
    >>> result.newProbability
    DenseVector([0.02..., 0.97...])
    >>> result.rawPrediction
    DenseVector([-3.54..., 3.54...])
    >>> test1 = sc.parallelize([Row(features=Vectors.sparse(2, [0], [1.0]))]).toDF()
    >>> blorModel.transform(test1).head().prediction
    1.0
    >>> blor.setParams("vector")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    >>> lr_path = temp_path + "/lr"
    >>> blor.save(lr_path)
    >>> lr2 = LogisticRegression.load(lr_path)
    >>> lr2.getRegParam()
    0.01
    >>> model_path = temp_path + "/lr_model"
    >>> blorModel.save(model_path)
    >>> model2 = LogisticRegressionModel.load(model_path)
    >>> bool(blorModel.coefficients[0] == model2.coefficients[0])
    True
    >>> blorModel.intercept == model2.intercept
    True
    >>> model2
    LogisticRegressionModel: uid=..., numClasses=2, numFeatures=2
    >>> blorModel.transform(test0).take(1) == model2.transform(test0).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @overload
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        regParam: float = ...,
        elasticNetParam: float = ...,
        tol: float = ...,
        fitIntercept: bool = ...,
        threshold: float = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        standardization: bool = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        family: str = ...,
        lowerBoundsOnCoefficients: Optional[Matrix] = ...,
        upperBoundsOnCoefficients: Optional[Matrix] = ...,
        lowerBoundsOnIntercepts: Optional[Vector] = ...,
        upperBoundsOnIntercepts: Optional[Vector] = ...,
        maxBlockSizeInMB: float = ...,
    ):
        ...

    @overload
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        regParam: float = ...,
        elasticNetParam: float = ...,
        tol: float = ...,
        fitIntercept: bool = ...,
        thresholds: Optional[List[float]] = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        standardization: bool = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        family: str = ...,
        lowerBoundsOnCoefficients: Optional[Matrix] = ...,
        upperBoundsOnCoefficients: Optional[Matrix] = ...,
        lowerBoundsOnIntercepts: Optional[Vector] = ...,
        upperBoundsOnIntercepts: Optional[Vector] = ...,
        maxBlockSizeInMB: float = ...,
    ):
        ...

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        maxIter: int = 100,
        regParam: float = 0.0,
        elasticNetParam: float = 0.0,
        tol: float = 1e-6,
        fitIntercept: bool = True,
        threshold: float = 0.5,
        thresholds: Optional[List[float]] = None,
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
        standardization: bool = True,
        weightCol: Optional[str] = None,
        aggregationDepth: int = 2,
        family: str = "auto",
        lowerBoundsOnCoefficients: Optional[Matrix] = None,
        upperBoundsOnCoefficients: Optional[Matrix] = None,
        lowerBoundsOnIntercepts: Optional[Vector] = None,
        upperBoundsOnIntercepts: Optional[Vector] = None,
        maxBlockSizeInMB: float = 0.0,
    ):
        """
        __init__(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                 threshold=0.5, thresholds=None, probabilityCol="probability", \
                 rawPredictionCol="rawPrediction", standardization=True, weightCol=None, \
                 aggregationDepth=2, family="auto", \
                 lowerBoundsOnCoefficients=None, upperBoundsOnCoefficients=None, \
                 lowerBoundsOnIntercepts=None, upperBoundsOnIntercepts=None, \
                 maxBlockSizeInMB=0.0):
        If the threshold and thresholds Params are both set, they must be equivalent.
        """
        super(LogisticRegression, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.LogisticRegression", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self._checkThresholdConsistency()

    @overload
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        regParam: float = ...,
        elasticNetParam: float = ...,
        tol: float = ...,
        fitIntercept: bool = ...,
        threshold: float = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        standardization: bool = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        family: str = ...,
        lowerBoundsOnCoefficients: Optional[Matrix] = ...,
        upperBoundsOnCoefficients: Optional[Matrix] = ...,
        lowerBoundsOnIntercepts: Optional[Vector] = ...,
        upperBoundsOnIntercepts: Optional[Vector] = ...,
        maxBlockSizeInMB: float = ...,
    ) -> "LogisticRegression":
        ...

    @overload
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        regParam: float = ...,
        elasticNetParam: float = ...,
        tol: float = ...,
        fitIntercept: bool = ...,
        thresholds: Optional[List[float]] = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        standardization: bool = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        family: str = ...,
        lowerBoundsOnCoefficients: Optional[Matrix] = ...,
        upperBoundsOnCoefficients: Optional[Matrix] = ...,
        lowerBoundsOnIntercepts: Optional[Vector] = ...,
        upperBoundsOnIntercepts: Optional[Vector] = ...,
        maxBlockSizeInMB: float = ...,
    ) -> "LogisticRegression":
        ...

    @keyword_only
    @since("1.3.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        maxIter: int = 100,
        regParam: float = 0.0,
        elasticNetParam: float = 0.0,
        tol: float = 1e-6,
        fitIntercept: bool = True,
        threshold: float = 0.5,
        thresholds: Optional[List[float]] = None,
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
        standardization: bool = True,
        weightCol: Optional[str] = None,
        aggregationDepth: int = 2,
        family: str = "auto",
        lowerBoundsOnCoefficients: Optional[Matrix] = None,
        upperBoundsOnCoefficients: Optional[Matrix] = None,
        lowerBoundsOnIntercepts: Optional[Vector] = None,
        upperBoundsOnIntercepts: Optional[Vector] = None,
        maxBlockSizeInMB: float = 0.0,
    ) -> "LogisticRegression":
        """
        setParams(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                  threshold=0.5, thresholds=None, probabilityCol="probability", \
                  rawPredictionCol="rawPrediction", standardization=True, weightCol=None, \
                  aggregationDepth=2, family="auto", \
                  lowerBoundsOnCoefficients=None, upperBoundsOnCoefficients=None, \
                  lowerBoundsOnIntercepts=None, upperBoundsOnIntercepts=None, \
                  maxBlockSizeInMB=0.0):
        Sets params for logistic regression.
        If the threshold and thresholds Params are both set, they must be equivalent.
        """
        kwargs = self._input_kwargs
        self._set(**kwargs)
        self._checkThresholdConsistency()
        return self

    def _create_model(self, java_model: "JavaObject") -> "LogisticRegressionModel":
        return LogisticRegressionModel(java_model)

    @since("2.1.0")
    def setFamily(self, value: str) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`family`.
        """
        return self._set(family=value)

    @since("2.3.0")
    def setLowerBoundsOnCoefficients(self, value: Matrix) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`lowerBoundsOnCoefficients`
        """
        return self._set(lowerBoundsOnCoefficients=value)

    @since("2.3.0")
    def setUpperBoundsOnCoefficients(self, value: Matrix) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`upperBoundsOnCoefficients`
        """
        return self._set(upperBoundsOnCoefficients=value)

    @since("2.3.0")
    def setLowerBoundsOnIntercepts(self, value: Vector) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`lowerBoundsOnIntercepts`
        """
        return self._set(lowerBoundsOnIntercepts=value)

    @since("2.3.0")
    def setUpperBoundsOnIntercepts(self, value: Vector) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`upperBoundsOnIntercepts`
        """
        return self._set(upperBoundsOnIntercepts=value)

    def setMaxIter(self, value: int) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    def setRegParam(self, value: float) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`regParam`.
        """
        return self._set(regParam=value)

    def setTol(self, value: float) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`tol`.
        """
        return self._set(tol=value)

    def setElasticNetParam(self, value: float) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`elasticNetParam`.
        """
        return self._set(elasticNetParam=value)

    def setFitIntercept(self, value: bool) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`fitIntercept`.
        """
        return self._set(fitIntercept=value)

    def setStandardization(self, value: bool) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`standardization`.
        """
        return self._set(standardization=value)

    def setWeightCol(self, value: str) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    def setAggregationDepth(self, value: int) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`aggregationDepth`.
        """
        return self._set(aggregationDepth=value)

    @since("3.1.0")
    def setMaxBlockSizeInMB(self, value: float) -> "LogisticRegression":
        """
        Sets the value of :py:attr:`maxBlockSizeInMB`.
        """
        return self._set(maxBlockSizeInMB=value)


class LogisticRegressionModel(
    _JavaProbabilisticClassificationModel[Vector],
    _LogisticRegressionParams,
    JavaMLWritable,
    JavaMLReadable["LogisticRegressionModel"],
    HasTrainingSummary["LogisticRegressionTrainingSummary"],
):
    """
    Model fitted by LogisticRegression.

    .. versionadded:: 1.3.0
    """

    @property
    @since("2.0.0")
    def coefficients(self) -> Vector:
        """
        Model coefficients of binomial logistic regression.
        An exception is thrown in the case of multinomial logistic regression.
        """
        return self._call_java("coefficients")

    @property
    @since("1.4.0")
    def intercept(self) -> float:
        """
        Model intercept of binomial logistic regression.
        An exception is thrown in the case of multinomial logistic regression.
        """
        return self._call_java("intercept")

    @property
    @since("2.1.0")
    def coefficientMatrix(self) -> Matrix:
        """
        Model coefficients.
        """
        return self._call_java("coefficientMatrix")

    @property
    @since("2.1.0")
    def interceptVector(self) -> Vector:
        """
        Model intercept.
        """
        return self._call_java("interceptVector")

    @property
    @since("2.0.0")
    def summary(self) -> "LogisticRegressionTrainingSummary":
        """
        Gets summary (accuracy/precision/recall, objective history, total iterations) of model
        trained on the training set. An exception is thrown if `trainingSummary is None`.
        """
        if self.hasSummary:
            if self.numClasses <= 2:
                return BinaryLogisticRegressionTrainingSummary(
                    super(LogisticRegressionModel, self).summary
                )
            else:
                return LogisticRegressionTrainingSummary(
                    super(LogisticRegressionModel, self).summary
                )
        else:
            raise RuntimeError(
                "No training summary available for this %s" % self.__class__.__name__
            )

    def evaluate(self, dataset: DataFrame) -> "LogisticRegressionSummary":
        """
        Evaluates the model on a test dataset.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            Test dataset to evaluate model on.
        """
        if not isinstance(dataset, DataFrame):
            raise TypeError("dataset must be a DataFrame but got %s." % type(dataset))
        java_blr_summary = self._call_java("evaluate", dataset)
        if self.numClasses <= 2:
            return BinaryLogisticRegressionSummary(java_blr_summary)
        else:
            return LogisticRegressionSummary(java_blr_summary)


class LogisticRegressionSummary(_ClassificationSummary):
    """
    Abstraction for Logistic Regression Results for a given model.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def probabilityCol(self) -> str:
        """
        Field in "predictions" which gives the probability
        of each class as a vector.
        """
        return self._call_java("probabilityCol")

    @property
    @since("2.0.0")
    def featuresCol(self) -> str:
        """
        Field in "predictions" which gives the features of each instance
        as a vector.
        """
        return self._call_java("featuresCol")


@inherit_doc
class LogisticRegressionTrainingSummary(LogisticRegressionSummary, _TrainingSummary):
    """
    Abstraction for multinomial Logistic Regression Training results.

    .. versionadded:: 2.0.0
    """

    pass


@inherit_doc
class BinaryLogisticRegressionSummary(_BinaryClassificationSummary, LogisticRegressionSummary):
    """
    Binary Logistic regression results for a given model.

    .. versionadded:: 2.0.0
    """

    pass


@inherit_doc
class BinaryLogisticRegressionTrainingSummary(
    BinaryLogisticRegressionSummary, LogisticRegressionTrainingSummary
):
    """
    Binary Logistic regression training results for a given model.

    .. versionadded:: 2.0.0
    """

    pass


@inherit_doc
class _DecisionTreeClassifierParams(_DecisionTreeParams, _TreeClassifierParams):
    """
    Params for :py:class:`DecisionTreeClassifier` and :py:class:`DecisionTreeClassificationModel`.
    """

    def __init__(self, *args: Any):
        super(_DecisionTreeClassifierParams, self).__init__(*args)
        self._setDefault(
            maxDepth=5,
            maxBins=32,
            minInstancesPerNode=1,
            minInfoGain=0.0,
            maxMemoryInMB=256,
            cacheNodeIds=False,
            checkpointInterval=10,
            impurity="gini",
            leafCol="",
            minWeightFractionPerNode=0.0,
        )


@inherit_doc
class DecisionTreeClassifier(
    _JavaProbabilisticClassifier["DecisionTreeClassificationModel"],
    _DecisionTreeClassifierParams,
    JavaMLWritable,
    JavaMLReadable["DecisionTreeClassifier"],
):
    """
    `Decision tree <http://en.wikipedia.org/wiki/Decision_tree_learning>`_
    learning algorithm for classification.
    It supports both binary and multiclass labels, as well as both continuous and categorical
    features.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> dt = DecisionTreeClassifier(maxDepth=2, labelCol="indexed", leafCol="leafId")
    >>> model = dt.fit(td)
    >>> model.getLabelCol()
    'indexed'
    >>> model.setFeaturesCol("features")
    DecisionTreeClassificationModel...
    >>> model.numNodes
    3
    >>> model.depth
    1
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> model.numFeatures
    1
    >>> model.numClasses
    2
    >>> print(model.toDebugString)
    DecisionTreeClassificationModel...depth=1, numNodes=3...
    >>> test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.predict(test0.head().features)
    0.0
    >>> model.predictRaw(test0.head().features)
    DenseVector([1.0, 0.0])
    >>> model.predictProbability(test0.head().features)
    DenseVector([1.0, 0.0])
    >>> result = model.transform(test0).head()
    >>> result.prediction
    0.0
    >>> result.probability
    DenseVector([1.0, 0.0])
    >>> result.rawPrediction
    DenseVector([1.0, 0.0])
    >>> result.leafId
    0.0
    >>> test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    >>> dtc_path = temp_path + "/dtc"
    >>> dt.save(dtc_path)
    >>> dt2 = DecisionTreeClassifier.load(dtc_path)
    >>> dt2.getMaxDepth()
    2
    >>> model_path = temp_path + "/dtc_model"
    >>> model.save(model_path)
    >>> model2 = DecisionTreeClassificationModel.load(model_path)
    >>> model.featureImportances == model2.featureImportances
    True
    >>> model.transform(test0).take(1) == model2.transform(test0).take(1)
    True
    >>> df3 = spark.createDataFrame([
    ...     (1.0, 0.2, Vectors.dense(1.0)),
    ...     (1.0, 0.8, Vectors.dense(1.0)),
    ...     (0.0, 1.0, Vectors.sparse(1, [], []))], ["label", "weight", "features"])
    >>> si3 = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model3 = si3.fit(df3)
    >>> td3 = si_model3.transform(df3)
    >>> dt3 = DecisionTreeClassifier(maxDepth=2, weightCol="weight", labelCol="indexed")
    >>> model3 = dt3.fit(td3)
    >>> print(model3.toDebugString)
    DecisionTreeClassificationModel...depth=1, numNodes=3...
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
        maxDepth: int = 5,
        maxBins: int = 32,
        minInstancesPerNode: int = 1,
        minInfoGain: float = 0.0,
        maxMemoryInMB: int = 256,
        cacheNodeIds: bool = False,
        checkpointInterval: int = 10,
        impurity: str = "gini",
        seed: Optional[int] = None,
        weightCol: Optional[str] = None,
        leafCol: str = "",
        minWeightFractionPerNode: float = 0.0,
    ):
        """
        __init__(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini", \
                 seed=None, weightCol=None, leafCol="", minWeightFractionPerNode=0.0)
        """
        super(DecisionTreeClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.DecisionTreeClassifier", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
        maxDepth: int = 5,
        maxBins: int = 32,
        minInstancesPerNode: int = 1,
        minInfoGain: float = 0.0,
        maxMemoryInMB: int = 256,
        cacheNodeIds: bool = False,
        checkpointInterval: int = 10,
        impurity: str = "gini",
        seed: Optional[int] = None,
        weightCol: Optional[str] = None,
        leafCol: str = "",
        minWeightFractionPerNode: float = 0.0,
    ) -> "DecisionTreeClassifier":
        """
        setParams(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  probabilityCol="probability", rawPredictionCol="rawPrediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini", \
                  seed=None, weightCol=None, leafCol="", minWeightFractionPerNode=0.0)
        Sets params for the DecisionTreeClassifier.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> "DecisionTreeClassificationModel":
        return DecisionTreeClassificationModel(java_model)

    def setMaxDepth(self, value: int) -> "DecisionTreeClassifier":
        """
        Sets the value of :py:attr:`maxDepth`.
        """
        return self._set(maxDepth=value)

    def setMaxBins(self, value: int) -> "DecisionTreeClassifier":
        """
        Sets the value of :py:attr:`maxBins`.
        """
        return self._set(maxBins=value)

    def setMinInstancesPerNode(self, value: int) -> "DecisionTreeClassifier":
        """
        Sets the value of :py:attr:`minInstancesPerNode`.
        """
        return self._set(minInstancesPerNode=value)

    @since("3.0.0")
    def setMinWeightFractionPerNode(self, value: float) -> "DecisionTreeClassifier":
        """
        Sets the value of :py:attr:`minWeightFractionPerNode`.
        """
        return self._set(minWeightFractionPerNode=value)

    def setMinInfoGain(self, value: float) -> "DecisionTreeClassifier":
        """
        Sets the value of :py:attr:`minInfoGain`.
        """
        return self._set(minInfoGain=value)

    def setMaxMemoryInMB(self, value: int) -> "DecisionTreeClassifier":
        """
        Sets the value of :py:attr:`maxMemoryInMB`.
        """
        return self._set(maxMemoryInMB=value)

    def setCacheNodeIds(self, value: bool) -> "DecisionTreeClassifier":
        """
        Sets the value of :py:attr:`cacheNodeIds`.
        """
        return self._set(cacheNodeIds=value)

    @since("1.4.0")
    def setImpurity(self, value: str) -> "DecisionTreeClassifier":
        """
        Sets the value of :py:attr:`impurity`.
        """
        return self._set(impurity=value)

    @since("1.4.0")
    def setCheckpointInterval(self, value: int) -> "DecisionTreeClassifier":
        """
        Sets the value of :py:attr:`checkpointInterval`.
        """
        return self._set(checkpointInterval=value)

    def setSeed(self, value: int) -> "DecisionTreeClassifier":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("3.0.0")
    def setWeightCol(self, value: str) -> "DecisionTreeClassifier":
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)


@inherit_doc
class DecisionTreeClassificationModel(
    _DecisionTreeModel,
    _JavaProbabilisticClassificationModel[Vector],
    _DecisionTreeClassifierParams,
    JavaMLWritable,
    JavaMLReadable["DecisionTreeClassificationModel"],
):
    """
    Model fitted by DecisionTreeClassifier.

    .. versionadded:: 1.4.0
    """

    @property
    def featureImportances(self) -> Vector:
        """
        Estimate of the importance of each feature.

        This generalizes the idea of "Gini" importance to other losses,
        following the explanation of Gini importance from "Random Forests" documentation
        by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.

        This feature importance is calculated as follows:
          - importance(feature j) = sum (over nodes which split on feature j) of the gain,
            where gain is scaled by the number of instances passing through node
          - Normalize importances for tree to sum to 1.

        .. versionadded:: 2.0.0

        Notes
        -----
        Feature importance for single decision trees can have high variance due to
        correlated predictor variables. Consider using a :py:class:`RandomForestClassifier`
        to determine feature importance instead.
        """
        return self._call_java("featureImportances")


@inherit_doc
class _RandomForestClassifierParams(_RandomForestParams, _TreeClassifierParams):
    """
    Params for :py:class:`RandomForestClassifier` and :py:class:`RandomForestClassificationModel`.
    """

    def __init__(self, *args: Any):
        super(_RandomForestClassifierParams, self).__init__(*args)
        self._setDefault(
            maxDepth=5,
            maxBins=32,
            minInstancesPerNode=1,
            minInfoGain=0.0,
            maxMemoryInMB=256,
            cacheNodeIds=False,
            checkpointInterval=10,
            impurity="gini",
            numTrees=20,
            featureSubsetStrategy="auto",
            subsamplingRate=1.0,
            leafCol="",
            minWeightFractionPerNode=0.0,
            bootstrap=True,
        )


@inherit_doc
class RandomForestClassifier(
    _JavaProbabilisticClassifier["RandomForestClassificationModel"],
    _RandomForestClassifierParams,
    JavaMLWritable,
    JavaMLReadable["RandomForestClassifier"],
):
    """
    `Random Forest <http://en.wikipedia.org/wiki/Random_forest>`_
    learning algorithm for classification.
    It supports both binary and multiclass labels, as well as both continuous and categorical
    features.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> import numpy
    >>> from numpy import allclose
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> rf = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol="indexed", seed=42,
    ...     leafCol="leafId")
    >>> rf.getMinWeightFractionPerNode()
    0.0
    >>> model = rf.fit(td)
    >>> model.getLabelCol()
    'indexed'
    >>> model.setFeaturesCol("features")
    RandomForestClassificationModel...
    >>> model.setRawPredictionCol("newRawPrediction")
    RandomForestClassificationModel...
    >>> model.getBootstrap()
    True
    >>> model.getRawPredictionCol()
    'newRawPrediction'
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> allclose(model.treeWeights, [1.0, 1.0, 1.0])
    True
    >>> test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.predict(test0.head().features)
    0.0
    >>> model.predictRaw(test0.head().features)
    DenseVector([2.0, 0.0])
    >>> model.predictProbability(test0.head().features)
    DenseVector([1.0, 0.0])
    >>> result = model.transform(test0).head()
    >>> result.prediction
    0.0
    >>> int(numpy.argmax(result.probability))
    0
    >>> int(numpy.argmax(result.newRawPrediction))
    0
    >>> result.leafId
    DenseVector([0.0, 0.0, 0.0])
    >>> test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    >>> model.trees
    [DecisionTreeClassificationModel...depth=..., DecisionTreeClassificationModel...]
    >>> rfc_path = temp_path + "/rfc"
    >>> rf.save(rfc_path)
    >>> rf2 = RandomForestClassifier.load(rfc_path)
    >>> rf2.getNumTrees()
    3
    >>> model_path = temp_path + "/rfc_model"
    >>> model.save(model_path)
    >>> model2 = RandomForestClassificationModel.load(model_path)
    >>> model.featureImportances == model2.featureImportances
    True
    >>> model.transform(test0).take(1) == model2.transform(test0).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
        maxDepth: int = 5,
        maxBins: int = 32,
        minInstancesPerNode: int = 1,
        minInfoGain: float = 0.0,
        maxMemoryInMB: int = 256,
        cacheNodeIds: bool = False,
        checkpointInterval: int = 10,
        impurity: str = "gini",
        numTrees: int = 20,
        featureSubsetStrategy: str = "auto",
        seed: Optional[int] = None,
        subsamplingRate: float = 1.0,
        leafCol: str = "",
        minWeightFractionPerNode: float = 0.0,
        weightCol: Optional[str] = None,
        bootstrap: Optional[bool] = True,
    ):
        """
        __init__(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini", \
                 numTrees=20, featureSubsetStrategy="auto", seed=None, subsamplingRate=1.0, \
                 leafCol="", minWeightFractionPerNode=0.0, weightCol=None, bootstrap=True)
        """
        super(RandomForestClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.RandomForestClassifier", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
        maxDepth: int = 5,
        maxBins: int = 32,
        minInstancesPerNode: int = 1,
        minInfoGain: float = 0.0,
        maxMemoryInMB: int = 256,
        cacheNodeIds: bool = False,
        checkpointInterval: int = 10,
        impurity: str = "gini",
        numTrees: int = 20,
        featureSubsetStrategy: str = "auto",
        seed: Optional[int] = None,
        subsamplingRate: float = 1.0,
        leafCol: str = "",
        minWeightFractionPerNode: float = 0.0,
        weightCol: Optional[str] = None,
        bootstrap: Optional[bool] = True,
    ) -> "RandomForestClassifier":
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=None, \
                  impurity="gini", numTrees=20, featureSubsetStrategy="auto", subsamplingRate=1.0, \
                  leafCol="", minWeightFractionPerNode=0.0, weightCol=None, bootstrap=True)
        Sets params for linear classification.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> "RandomForestClassificationModel":
        return RandomForestClassificationModel(java_model)

    def setMaxDepth(self, value: int) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`maxDepth`.
        """
        return self._set(maxDepth=value)

    def setMaxBins(self, value: int) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`maxBins`.
        """
        return self._set(maxBins=value)

    def setMinInstancesPerNode(self, value: int) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`minInstancesPerNode`.
        """
        return self._set(minInstancesPerNode=value)

    def setMinInfoGain(self, value: float) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`minInfoGain`.
        """
        return self._set(minInfoGain=value)

    def setMaxMemoryInMB(self, value: int) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`maxMemoryInMB`.
        """
        return self._set(maxMemoryInMB=value)

    def setCacheNodeIds(self, value: bool) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`cacheNodeIds`.
        """
        return self._set(cacheNodeIds=value)

    @since("1.4.0")
    def setImpurity(self, value: str) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`impurity`.
        """
        return self._set(impurity=value)

    @since("1.4.0")
    def setNumTrees(self, value: int) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`numTrees`.
        """
        return self._set(numTrees=value)

    @since("3.0.0")
    def setBootstrap(self, value: bool) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`bootstrap`.
        """
        return self._set(bootstrap=value)

    @since("1.4.0")
    def setSubsamplingRate(self, value: float) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`subsamplingRate`.
        """
        return self._set(subsamplingRate=value)

    @since("2.4.0")
    def setFeatureSubsetStrategy(self, value: str) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`featureSubsetStrategy`.
        """
        return self._set(featureSubsetStrategy=value)

    def setSeed(self, value: int) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    def setCheckpointInterval(self, value: int) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`checkpointInterval`.
        """
        return self._set(checkpointInterval=value)

    @since("3.0.0")
    def setWeightCol(self, value: str) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @since("3.0.0")
    def setMinWeightFractionPerNode(self, value: float) -> "RandomForestClassifier":
        """
        Sets the value of :py:attr:`minWeightFractionPerNode`.
        """
        return self._set(minWeightFractionPerNode=value)


class RandomForestClassificationModel(
    _TreeEnsembleModel,
    _JavaProbabilisticClassificationModel[Vector],
    _RandomForestClassifierParams,
    JavaMLWritable,
    JavaMLReadable["RandomForestClassificationModel"],
    HasTrainingSummary["RandomForestClassificationTrainingSummary"],
):
    """
    Model fitted by RandomForestClassifier.

    .. versionadded:: 1.4.0
    """

    @property
    def featureImportances(self) -> Vector:
        """
        Estimate of the importance of each feature.

        Each feature's importance is the average of its importance across all trees in the ensemble
        The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
        (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
        and follows the implementation from scikit-learn.

        .. versionadded:: 2.0.0

        See Also
        --------
        DecisionTreeClassificationModel.featureImportances
        """
        return self._call_java("featureImportances")

    @cached_property
    @since("2.0.0")
    def trees(self) -> List[DecisionTreeClassificationModel]:
        """Trees in this ensemble. Warning: These have null parent Estimators."""
        if is_remote():
            return [DecisionTreeClassificationModel(m) for m in self._call_java("trees").split(",")]
        return [DecisionTreeClassificationModel(m) for m in list(self._call_java("trees"))]

    @property
    @since("3.1.0")
    def summary(self) -> "RandomForestClassificationTrainingSummary":
        """
        Gets summary (accuracy/precision/recall, objective history, total iterations) of model
        trained on the training set. An exception is thrown if `trainingSummary is None`.
        """
        if self.hasSummary:
            if self.numClasses <= 2:
                return BinaryRandomForestClassificationTrainingSummary(
                    super(RandomForestClassificationModel, self).summary
                )
            else:
                return RandomForestClassificationTrainingSummary(
                    super(RandomForestClassificationModel, self).summary
                )
        else:
            raise RuntimeError(
                "No training summary available for this %s" % self.__class__.__name__
            )

    def evaluate(
        self, dataset: DataFrame
    ) -> Union["BinaryRandomForestClassificationSummary", "RandomForestClassificationSummary"]:
        """
        Evaluates the model on a test dataset.

        .. versionadded:: 3.1.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            Test dataset to evaluate model on.
        """
        if not isinstance(dataset, DataFrame):
            raise TypeError("dataset must be a DataFrame but got %s." % type(dataset))
        java_rf_summary = self._call_java("evaluate", dataset)
        if self.numClasses <= 2:
            return BinaryRandomForestClassificationSummary(java_rf_summary)
        else:
            return RandomForestClassificationSummary(java_rf_summary)


class RandomForestClassificationSummary(_ClassificationSummary):
    """
    Abstraction for RandomForestClassification Results for a given model.

    .. versionadded:: 3.1.0
    """

    pass


@inherit_doc
class RandomForestClassificationTrainingSummary(
    RandomForestClassificationSummary, _TrainingSummary
):
    """
    Abstraction for RandomForestClassificationTraining Training results.

    .. versionadded:: 3.1.0
    """

    pass


@inherit_doc
class BinaryRandomForestClassificationSummary(_BinaryClassificationSummary):
    """
    BinaryRandomForestClassification results for a given model.

    .. versionadded:: 3.1.0
    """

    pass


@inherit_doc
class BinaryRandomForestClassificationTrainingSummary(
    BinaryRandomForestClassificationSummary, RandomForestClassificationTrainingSummary
):
    """
    BinaryRandomForestClassification training results for a given model.

    .. versionadded:: 3.1.0
    """

    pass


class _GBTClassifierParams(_GBTParams, _HasVarianceImpurity):
    """
    Params for :py:class:`GBTClassifier` and :py:class:`GBTClassifierModel`.

    .. versionadded:: 3.0.0
    """

    supportedLossTypes: List[str] = ["logistic"]

    lossType: Param[str] = Param(
        Params._dummy(),
        "lossType",
        "Loss function which GBT tries to minimize (case-insensitive). "
        + "Supported options: "
        + ", ".join(supportedLossTypes),
        typeConverter=TypeConverters.toString,
    )

    def __init__(self, *args: Any):
        super(_GBTClassifierParams, self).__init__(*args)
        self._setDefault(
            maxDepth=5,
            maxBins=32,
            minInstancesPerNode=1,
            minInfoGain=0.0,
            maxMemoryInMB=256,
            cacheNodeIds=False,
            checkpointInterval=10,
            lossType="logistic",
            maxIter=20,
            stepSize=0.1,
            subsamplingRate=1.0,
            impurity="variance",
            featureSubsetStrategy="all",
            validationTol=0.01,
            leafCol="",
            minWeightFractionPerNode=0.0,
        )

    @since("1.4.0")
    def getLossType(self) -> str:
        """
        Gets the value of lossType or its default value.
        """
        return self.getOrDefault(self.lossType)


@inherit_doc
class GBTClassifier(
    _JavaProbabilisticClassifier["GBTClassificationModel"],
    _GBTClassifierParams,
    JavaMLWritable,
    JavaMLReadable["GBTClassifier"],
):
    """
    `Gradient-Boosted Trees (GBTs) <http://en.wikipedia.org/wiki/Gradient_boosting>`_
    learning algorithm for classification.
    It supports binary labels, as well as both continuous and categorical features.

    .. versionadded:: 1.4.0

    Notes
    -----
    Multiclass labels are not currently supported.

    The implementation is based upon: J.H. Friedman. "Stochastic Gradient Boosting." 1999.

    Gradient Boosting vs. TreeBoost:

    - This implementation is for Stochastic Gradient Boosting, not for TreeBoost.
    - Both algorithms learn tree ensembles by minimizing loss functions.
    - TreeBoost (Friedman, 1999) additionally modifies the outputs at tree leaf nodes
      based on the loss function, whereas the original gradient boosting method does not.
    - We expect to implement TreeBoost in the future:
      `SPARK-4240 <https://issues.apache.org/jira/browse/SPARK-4240>`_

    Examples
    --------
    >>> from numpy import allclose
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> gbt = GBTClassifier(maxIter=5, maxDepth=2, labelCol="indexed", seed=42,
    ...     leafCol="leafId")
    >>> gbt.setMaxIter(5)
    GBTClassifier...
    >>> gbt.setMinWeightFractionPerNode(0.049)
    GBTClassifier...
    >>> gbt.getMaxIter()
    5
    >>> gbt.getFeatureSubsetStrategy()
    'all'
    >>> model = gbt.fit(td)
    >>> model.getLabelCol()
    'indexed'
    >>> model.setFeaturesCol("features")
    GBTClassificationModel...
    >>> model.setThresholds([0.3, 0.7])
    GBTClassificationModel...
    >>> model.getThresholds()
    [0.3, 0.7]
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> allclose(model.treeWeights, [1.0, 0.1, 0.1, 0.1, 0.1])
    True
    >>> test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.predict(test0.head().features)
    0.0
    >>> model.predictRaw(test0.head().features)
    DenseVector([1.1697, -1.1697])
    >>> model.predictProbability(test0.head().features)
    DenseVector([0.9121, 0.0879])
    >>> result = model.transform(test0).head()
    >>> result.prediction
    0.0
    >>> result.leafId
    DenseVector([0.0, 0.0, 0.0, 0.0, 0.0])
    >>> test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    >>> model.totalNumNodes
    15
    >>> print(model.toDebugString)
    GBTClassificationModel...numTrees=5...
    >>> gbtc_path = temp_path + "gbtc"
    >>> gbt.save(gbtc_path)
    >>> gbt2 = GBTClassifier.load(gbtc_path)
    >>> gbt2.getMaxDepth()
    2
    >>> model_path = temp_path + "gbtc_model"
    >>> model.save(model_path)
    >>> model2 = GBTClassificationModel.load(model_path)
    >>> model.featureImportances == model2.featureImportances
    True
    >>> model.treeWeights == model2.treeWeights
    True
    >>> model.transform(test0).take(1) == model2.transform(test0).take(1)
    True
    >>> model.trees
    [DecisionTreeRegressionModel...depth=..., DecisionTreeRegressionModel...]
    >>> validation = spark.createDataFrame([(0.0, Vectors.dense(-1.0),)],
    ...              ["indexed", "features"])
    >>> model.evaluateEachIteration(validation)
    [0.25..., 0.23..., 0.21..., 0.19..., 0.18...]
    >>> model.numClasses
    2
    >>> gbt = gbt.setValidationIndicatorCol("validationIndicator")
    >>> gbt.getValidationIndicatorCol()
    'validationIndicator'
    >>> gbt.getValidationTol()
    0.01
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        maxDepth: int = 5,
        maxBins: int = 32,
        minInstancesPerNode: int = 1,
        minInfoGain: float = 0.0,
        maxMemoryInMB: int = 256,
        cacheNodeIds: bool = False,
        checkpointInterval: int = 10,
        lossType: str = "logistic",
        maxIter: int = 20,
        stepSize: float = 0.1,
        seed: Optional[int] = None,
        subsamplingRate: float = 1.0,
        impurity: str = "variance",
        featureSubsetStrategy: str = "all",
        validationTol: float = 0.01,
        validationIndicatorCol: Optional[str] = None,
        leafCol: str = "",
        minWeightFractionPerNode: float = 0.0,
        weightCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                 lossType="logistic", maxIter=20, stepSize=0.1, seed=None, subsamplingRate=1.0, \
                 impurity="variance", featureSubsetStrategy="all", validationTol=0.01, \
                 validationIndicatorCol=None, leafCol="", minWeightFractionPerNode=0.0, \
                 weightCol=None)
        """
        super(GBTClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.GBTClassifier", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        maxDepth: int = 5,
        maxBins: int = 32,
        minInstancesPerNode: int = 1,
        minInfoGain: float = 0.0,
        maxMemoryInMB: int = 256,
        cacheNodeIds: bool = False,
        checkpointInterval: int = 10,
        lossType: str = "logistic",
        maxIter: int = 20,
        stepSize: float = 0.1,
        seed: Optional[int] = None,
        subsamplingRate: float = 1.0,
        impurity: str = "variance",
        featureSubsetStrategy: str = "all",
        validationTol: float = 0.01,
        validationIndicatorCol: Optional[str] = None,
        leafCol: str = "",
        minWeightFractionPerNode: float = 0.0,
        weightCol: Optional[str] = None,
    ) -> "GBTClassifier":
        """
        setParams(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                  lossType="logistic", maxIter=20, stepSize=0.1, seed=None, subsamplingRate=1.0, \
                  impurity="variance", featureSubsetStrategy="all", validationTol=0.01, \
                  validationIndicatorCol=None, leafCol="", minWeightFractionPerNode=0.0, \
                  weightCol=None)
        Sets params for Gradient Boosted Tree Classification.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> "GBTClassificationModel":
        return GBTClassificationModel(java_model)

    def setMaxDepth(self, value: int) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`maxDepth`.
        """
        return self._set(maxDepth=value)

    def setMaxBins(self, value: int) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`maxBins`.
        """
        return self._set(maxBins=value)

    def setMinInstancesPerNode(self, value: int) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`minInstancesPerNode`.
        """
        return self._set(minInstancesPerNode=value)

    def setMinInfoGain(self, value: float) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`minInfoGain`.
        """
        return self._set(minInfoGain=value)

    def setMaxMemoryInMB(self, value: int) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`maxMemoryInMB`.
        """
        return self._set(maxMemoryInMB=value)

    def setCacheNodeIds(self, value: bool) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`cacheNodeIds`.
        """
        return self._set(cacheNodeIds=value)

    @since("1.4.0")
    def setImpurity(self, value: str) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`impurity`.
        """
        return self._set(impurity=value)

    @since("1.4.0")
    def setLossType(self, value: str) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`lossType`.
        """
        return self._set(lossType=value)

    @since("1.4.0")
    def setSubsamplingRate(self, value: float) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`subsamplingRate`.
        """
        return self._set(subsamplingRate=value)

    @since("2.4.0")
    def setFeatureSubsetStrategy(self, value: str) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`featureSubsetStrategy`.
        """
        return self._set(featureSubsetStrategy=value)

    @since("3.0.0")
    def setValidationIndicatorCol(self, value: str) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`validationIndicatorCol`.
        """
        return self._set(validationIndicatorCol=value)

    @since("1.4.0")
    def setMaxIter(self, value: int) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("1.4.0")
    def setCheckpointInterval(self, value: int) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`checkpointInterval`.
        """
        return self._set(checkpointInterval=value)

    @since("1.4.0")
    def setSeed(self, value: int) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("1.4.0")
    def setStepSize(self, value: int) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`stepSize`.
        """
        return self._set(stepSize=value)

    @since("3.0.0")
    def setWeightCol(self, value: str) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @since("3.0.0")
    def setMinWeightFractionPerNode(self, value: float) -> "GBTClassifier":
        """
        Sets the value of :py:attr:`minWeightFractionPerNode`.
        """
        return self._set(minWeightFractionPerNode=value)


class GBTClassificationModel(
    _TreeEnsembleModel,
    _JavaProbabilisticClassificationModel[Vector],
    _GBTClassifierParams,
    JavaMLWritable,
    JavaMLReadable["GBTClassificationModel"],
):
    """
    Model fitted by GBTClassifier.

    .. versionadded:: 1.4.0
    """

    @property
    def featureImportances(self) -> Vector:
        """
        Estimate of the importance of each feature.

        Each feature's importance is the average of its importance across all trees in the ensemble
        The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
        (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
        and follows the implementation from scikit-learn.

        .. versionadded:: 2.0.0

        See Also
        --------
        DecisionTreeClassificationModel.featureImportances
        """
        return self._call_java("featureImportances")

    @cached_property
    @since("2.0.0")
    def trees(self) -> List[DecisionTreeRegressionModel]:
        """Trees in this ensemble. Warning: These have null parent Estimators."""
        if is_remote():
            return [DecisionTreeRegressionModel(m) for m in self._call_java("trees").split(",")]
        return [DecisionTreeRegressionModel(m) for m in list(self._call_java("trees"))]

    def evaluateEachIteration(self, dataset: DataFrame) -> List[float]:
        """
        Method to compute error or loss for every iteration of gradient boosting.

        .. versionadded:: 2.4.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            Test dataset to evaluate model on.
        """
        return self._call_java("evaluateEachIteration", dataset)


class _NaiveBayesParams(_PredictorParams, HasWeightCol):
    """
    Params for :py:class:`NaiveBayes` and :py:class:`NaiveBayesModel`.

    .. versionadded:: 3.0.0
    """

    smoothing: Param[float] = Param(
        Params._dummy(),
        "smoothing",
        "The smoothing parameter, should be >= 0, " + "default is 1.0",
        typeConverter=TypeConverters.toFloat,
    )
    modelType: Param[str] = Param(
        Params._dummy(),
        "modelType",
        "The model type which is a string "
        + "(case-sensitive). Supported options: multinomial (default), bernoulli "
        + "and gaussian.",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self, *args: Any):
        super(_NaiveBayesParams, self).__init__(*args)
        self._setDefault(smoothing=1.0, modelType="multinomial")

    @since("1.5.0")
    def getSmoothing(self) -> float:
        """
        Gets the value of smoothing or its default value.
        """
        return self.getOrDefault(self.smoothing)

    @since("1.5.0")
    def getModelType(self) -> str:
        """
        Gets the value of modelType or its default value.
        """
        return self.getOrDefault(self.modelType)


@inherit_doc
class NaiveBayes(
    _JavaProbabilisticClassifier["NaiveBayesModel"],
    _NaiveBayesParams,
    HasThresholds,
    HasWeightCol,
    JavaMLWritable,
    JavaMLReadable["NaiveBayes"],
):
    """
    Naive Bayes Classifiers.
    It supports both Multinomial and Bernoulli NB. `Multinomial NB \
    <http://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html>`_
    can handle finitely supported discrete data. For example, by converting documents into
    TF-IDF vectors, it can be used for document classification. By making every vector a
    binary (0/1) data, it can also be used as `Bernoulli NB \
    <http://nlp.stanford.edu/IR-book/html/htmledition/the-bernoulli-model-1.html>`_.

    The input feature values for Multinomial NB and Bernoulli NB must be nonnegative.
    Since 3.0.0, it supports Complement NB which is an adaptation of the Multinomial NB.
    Specifically, Complement NB uses statistics from the complement of each class to compute
    the model's coefficients. The inventors of Complement NB show empirically that the parameter
    estimates for CNB are more stable than those for Multinomial NB. Like Multinomial NB, the
    input feature values for Complement NB must be nonnegative.
    Since 3.0.0, it also supports `Gaussian NB \
    <https://en.wikipedia.org/wiki/Naive_Bayes_classifier#Gaussian_naive_Bayes>`_.
    which can handle continuous data.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     Row(label=0.0, weight=0.1, features=Vectors.dense([0.0, 0.0])),
    ...     Row(label=0.0, weight=0.5, features=Vectors.dense([0.0, 1.0])),
    ...     Row(label=1.0, weight=1.0, features=Vectors.dense([1.0, 0.0]))])
    >>> nb = NaiveBayes(smoothing=1.0, modelType="multinomial", weightCol="weight")
    >>> model = nb.fit(df)
    >>> model.setFeaturesCol("features")
    NaiveBayesModel...
    >>> model.getSmoothing()
    1.0
    >>> model.pi
    DenseVector([-0.81..., -0.58...])
    >>> model.theta
    DenseMatrix(2, 2, [-0.91..., -0.51..., -0.40..., -1.09...], 1)
    >>> model.sigma
    DenseMatrix(0, 0, [...], ...)
    >>> test0 = sc.parallelize([Row(features=Vectors.dense([1.0, 0.0]))]).toDF()
    >>> model.predict(test0.head().features)
    1.0
    >>> model.predictRaw(test0.head().features)
    DenseVector([-1.72..., -0.99...])
    >>> model.predictProbability(test0.head().features)
    DenseVector([0.32..., 0.67...])
    >>> result = model.transform(test0).head()
    >>> result.prediction
    1.0
    >>> result.probability
    DenseVector([0.32..., 0.67...])
    >>> result.rawPrediction
    DenseVector([-1.72..., -0.99...])
    >>> test1 = sc.parallelize([Row(features=Vectors.sparse(2, [0], [1.0]))]).toDF()
    >>> model.transform(test1).head().prediction
    1.0
    >>> nb_path = temp_path + "/nb"
    >>> nb.save(nb_path)
    >>> nb2 = NaiveBayes.load(nb_path)
    >>> nb2.getSmoothing()
    1.0
    >>> model_path = temp_path + "/nb_model"
    >>> model.save(model_path)
    >>> model2 = NaiveBayesModel.load(model_path)
    >>> model.pi == model2.pi
    True
    >>> model.theta == model2.theta
    True
    >>> model.transform(test0).take(1) == model2.transform(test0).take(1)
    True
    >>> nb = nb.setThresholds([0.01, 10.00])
    >>> model3 = nb.fit(df)
    >>> result = model3.transform(test0).head()
    >>> result.prediction
    0.0
    >>> nb3 = NaiveBayes().setModelType("gaussian")
    >>> model4 = nb3.fit(df)
    >>> model4.getModelType()
    'gaussian'
    >>> model4.sigma
    DenseMatrix(2, 2, [0.0, 0.25, 0.0, 0.0], 1)
    >>> nb5 = NaiveBayes(smoothing=1.0, modelType="complement", weightCol="weight")
    >>> model5 = nb5.fit(df)
    >>> model5.getModelType()
    'complement'
    >>> model5.theta
    DenseMatrix(2, 2, [...], 1)
    >>> model5.sigma
    DenseMatrix(0, 0, [...], ...)
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
        smoothing: float = 1.0,
        modelType: str = "multinomial",
        thresholds: Optional[List[float]] = None,
        weightCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", smoothing=1.0, \
                 modelType="multinomial", thresholds=None, weightCol=None)
        """
        super(NaiveBayes, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.NaiveBayes", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.5.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
        smoothing: float = 1.0,
        modelType: str = "multinomial",
        thresholds: Optional[List[float]] = None,
        weightCol: Optional[str] = None,
    ) -> "NaiveBayes":
        """
        setParams(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  probabilityCol="probability", rawPredictionCol="rawPrediction", smoothing=1.0, \
                  modelType="multinomial", thresholds=None, weightCol=None)
        Sets params for Naive Bayes.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> "NaiveBayesModel":
        return NaiveBayesModel(java_model)

    @since("1.5.0")
    def setSmoothing(self, value: float) -> "NaiveBayes":
        """
        Sets the value of :py:attr:`smoothing`.
        """
        return self._set(smoothing=value)

    @since("1.5.0")
    def setModelType(self, value: str) -> "NaiveBayes":
        """
        Sets the value of :py:attr:`modelType`.
        """
        return self._set(modelType=value)

    def setWeightCol(self, value: str) -> "NaiveBayes":
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)


class NaiveBayesModel(
    _JavaProbabilisticClassificationModel[Vector],
    _NaiveBayesParams,
    JavaMLWritable,
    JavaMLReadable["NaiveBayesModel"],
):
    """
    Model fitted by NaiveBayes.

    .. versionadded:: 1.5.0
    """

    @property
    @since("2.0.0")
    def pi(self) -> Vector:
        """
        log of class priors.
        """
        return self._call_java("pi")

    @property
    @since("2.0.0")
    def theta(self) -> Matrix:
        """
        log of class conditional probabilities.
        """
        return self._call_java("theta")

    @property
    @since("3.0.0")
    def sigma(self) -> Matrix:
        """
        variance of each feature.
        """
        return self._call_java("sigma")


class _MultilayerPerceptronParams(
    _ProbabilisticClassifierParams,
    HasSeed,
    HasMaxIter,
    HasTol,
    HasStepSize,
    HasSolver,
    HasBlockSize,
):
    """
    Params for :py:class:`MultilayerPerceptronClassifier`.

    .. versionadded:: 3.0.0
    """

    layers: Param[List[int]] = Param(
        Params._dummy(),
        "layers",
        "Sizes of layers from input layer to output layer "
        + "E.g., Array(780, 100, 10) means 780 inputs, one hidden layer with 100 "
        + "neurons and output layer of 10 neurons.",
        typeConverter=TypeConverters.toListInt,
    )
    solver: Param[str] = Param(
        Params._dummy(),
        "solver",
        "The solver algorithm for optimization. Supported " + "options: l-bfgs, gd.",
        typeConverter=TypeConverters.toString,
    )
    initialWeights: Param[Vector] = Param(
        Params._dummy(),
        "initialWeights",
        "The initial weights of the model.",
        typeConverter=TypeConverters.toVector,
    )

    def __init__(self, *args: Any):
        super(_MultilayerPerceptronParams, self).__init__(*args)
        self._setDefault(maxIter=100, tol=1e-6, blockSize=128, stepSize=0.03, solver="l-bfgs")

    @since("1.6.0")
    def getLayers(self) -> List[int]:
        """
        Gets the value of layers or its default value.
        """
        return self.getOrDefault(self.layers)

    @since("2.0.0")
    def getInitialWeights(self) -> Vector:
        """
        Gets the value of initialWeights or its default value.
        """
        return self.getOrDefault(self.initialWeights)


@inherit_doc
class MultilayerPerceptronClassifier(
    _JavaProbabilisticClassifier["MultilayerPerceptronClassificationModel"],
    _MultilayerPerceptronParams,
    JavaMLWritable,
    JavaMLReadable["MultilayerPerceptronClassifier"],
):
    """
    Classifier trainer based on the Multilayer Perceptron.
    Each layer has sigmoid activation function, output layer has softmax.
    Number of inputs has to be equal to the size of feature vectors.
    Number of outputs has to be equal to the total number of labels.

    .. versionadded:: 1.6.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     (0.0, Vectors.dense([0.0, 0.0])),
    ...     (1.0, Vectors.dense([0.0, 1.0])),
    ...     (1.0, Vectors.dense([1.0, 0.0])),
    ...     (0.0, Vectors.dense([1.0, 1.0]))], ["label", "features"])
    >>> mlp = MultilayerPerceptronClassifier(layers=[2, 2, 2], seed=123)
    >>> mlp.setMaxIter(100)
    MultilayerPerceptronClassifier...
    >>> mlp.getMaxIter()
    100
    >>> mlp.getBlockSize()
    128
    >>> mlp.setBlockSize(1)
    MultilayerPerceptronClassifier...
    >>> mlp.getBlockSize()
    1
    >>> model = mlp.fit(df)
    >>> model.setFeaturesCol("features")
    MultilayerPerceptronClassificationModel...
    >>> model.getMaxIter()
    100
    >>> model.getLayers()
    [2, 2, 2]
    >>> model.weights.size
    12
    >>> testDF = spark.createDataFrame([
    ...     (Vectors.dense([1.0, 0.0]),),
    ...     (Vectors.dense([0.0, 0.0]),)], ["features"])
    >>> model.predict(testDF.head().features)
    1.0
    >>> model.predictRaw(testDF.head().features)
    DenseVector([-16.208, 16.344])
    >>> model.predictProbability(testDF.head().features)
    DenseVector([0.0, 1.0])
    >>> model.transform(testDF).select("features", "prediction").show()
    +---------+----------+
    | features|prediction|
    +---------+----------+
    |[1.0,0.0]|       1.0|
    |[0.0,0.0]|       0.0|
    +---------+----------+
    ...
    >>> mlp_path = temp_path + "/mlp"
    >>> mlp.save(mlp_path)
    >>> mlp2 = MultilayerPerceptronClassifier.load(mlp_path)
    >>> mlp2.getBlockSize()
    1
    >>> model_path = temp_path + "/mlp_model"
    >>> model.save(model_path)
    >>> model2 = MultilayerPerceptronClassificationModel.load(model_path)
    >>> model.getLayers() == model2.getLayers()
    True
    >>> model.weights == model2.weights
    True
    >>> model.transform(testDF).take(1) == model2.transform(testDF).take(1)
    True
    >>> mlp2 = mlp2.setInitialWeights(list(range(0, 12)))
    >>> model3 = mlp2.fit(df)
    >>> model3.weights != model2.weights
    True
    >>> model3.getLayers() == model.getLayers()
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        maxIter: int = 100,
        tol: float = 1e-6,
        seed: Optional[int] = None,
        layers: Optional[List[int]] = None,
        blockSize: int = 128,
        stepSize: float = 0.03,
        solver: str = "l-bfgs",
        initialWeights: Optional[Vector] = None,
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
    ):
        """
        __init__(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, tol=1e-6, seed=None, layers=None, blockSize=128, stepSize=0.03, \
                 solver="l-bfgs", initialWeights=None, probabilityCol="probability", \
                 rawPredictionCol="rawPrediction")
        """
        super(MultilayerPerceptronClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.MultilayerPerceptronClassifier", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        maxIter: int = 100,
        tol: float = 1e-6,
        seed: Optional[int] = None,
        layers: Optional[List[int]] = None,
        blockSize: int = 128,
        stepSize: float = 0.03,
        solver: str = "l-bfgs",
        initialWeights: Optional[Vector] = None,
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
    ) -> "MultilayerPerceptronClassifier":
        """
        setParams(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, tol=1e-6, seed=None, layers=None, blockSize=128, stepSize=0.03, \
                  solver="l-bfgs", initialWeights=None, probabilityCol="probability", \
                  rawPredictionCol="rawPrediction"):
        Sets params for MultilayerPerceptronClassifier.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> "MultilayerPerceptronClassificationModel":
        return MultilayerPerceptronClassificationModel(java_model)

    @since("1.6.0")
    def setLayers(self, value: List[int]) -> "MultilayerPerceptronClassifier":
        """
        Sets the value of :py:attr:`layers`.
        """
        return self._set(layers=value)

    @since("1.6.0")
    def setBlockSize(self, value: int) -> "MultilayerPerceptronClassifier":
        """
        Sets the value of :py:attr:`blockSize`.
        """
        return self._set(blockSize=value)

    @since("2.0.0")
    def setInitialWeights(self, value: Vector) -> "MultilayerPerceptronClassifier":
        """
        Sets the value of :py:attr:`initialWeights`.
        """
        return self._set(initialWeights=value)

    def setMaxIter(self, value: int) -> "MultilayerPerceptronClassifier":
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    def setSeed(self, value: int) -> "MultilayerPerceptronClassifier":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    def setTol(self, value: float) -> "MultilayerPerceptronClassifier":
        """
        Sets the value of :py:attr:`tol`.
        """
        return self._set(tol=value)

    @since("2.0.0")
    def setStepSize(self, value: float) -> "MultilayerPerceptronClassifier":
        """
        Sets the value of :py:attr:`stepSize`.
        """
        return self._set(stepSize=value)

    def setSolver(self, value: str) -> "MultilayerPerceptronClassifier":
        """
        Sets the value of :py:attr:`solver`.
        """
        return self._set(solver=value)


class MultilayerPerceptronClassificationModel(
    _JavaProbabilisticClassificationModel[Vector],
    _MultilayerPerceptronParams,
    JavaMLWritable,
    JavaMLReadable["MultilayerPerceptronClassificationModel"],
    HasTrainingSummary["MultilayerPerceptronClassificationTrainingSummary"],
):
    """
    Model fitted by MultilayerPerceptronClassifier.

    .. versionadded:: 1.6.0
    """

    @property
    @since("2.0.0")
    def weights(self) -> Vector:
        """
        the weights of layers.
        """
        return self._call_java("weights")

    @since("3.1.0")
    def summary(  # type: ignore[override]
        self,
    ) -> "MultilayerPerceptronClassificationTrainingSummary":
        """
        Gets summary (accuracy/precision/recall, objective history, total iterations) of model
        trained on the training set. An exception is thrown if `trainingSummary is None`.
        """
        if self.hasSummary:
            return MultilayerPerceptronClassificationTrainingSummary(
                super(MultilayerPerceptronClassificationModel, self).summary
            )
        else:
            raise RuntimeError(
                "No training summary available for this %s" % self.__class__.__name__
            )

    def evaluate(self, dataset: DataFrame) -> "MultilayerPerceptronClassificationSummary":
        """
        Evaluates the model on a test dataset.

        .. versionadded:: 3.1.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            Test dataset to evaluate model on.
        """
        if not isinstance(dataset, DataFrame):
            raise TypeError("dataset must be a DataFrame but got %s." % type(dataset))
        java_mlp_summary = self._call_java("evaluate", dataset)
        return MultilayerPerceptronClassificationSummary(java_mlp_summary)


class MultilayerPerceptronClassificationSummary(_ClassificationSummary):
    """
    Abstraction for MultilayerPerceptronClassifier Results for a given model.

    .. versionadded:: 3.1.0
    """

    pass


@inherit_doc
class MultilayerPerceptronClassificationTrainingSummary(
    MultilayerPerceptronClassificationSummary, _TrainingSummary
):
    """
    Abstraction for MultilayerPerceptronClassifier Training results.

    .. versionadded:: 3.1.0
    """

    pass


class _OneVsRestParams(_ClassifierParams, HasWeightCol):
    """
    Params for :py:class:`OneVsRest` and :py:class:`OneVsRestModelModel`.
    """

    classifier: Param[Classifier] = Param(Params._dummy(), "classifier", "base binary classifier")

    @since("2.0.0")
    def getClassifier(self) -> Classifier:
        """
        Gets the value of classifier or its default value.
        """
        return self.getOrDefault(self.classifier)


@inherit_doc
class OneVsRest(
    Estimator["OneVsRestModel"],
    _OneVsRestParams,
    HasParallelism,
    MLReadable["OneVsRest"],
    MLWritable,
    Generic[CM],
):
    """
    Reduction of Multiclass Classification to Binary Classification.
    Performs reduction using one against all strategy.
    For a multiclass classification with k classes, train k models (one per class).
    Each example is scored against all k models and the model with highest score
    is picked to label the example.

    .. versionadded:: 2.0.0

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> from pyspark.ml.linalg import Vectors
    >>> data_path = "data/mllib/sample_multiclass_classification_data.txt"
    >>> df = spark.read.format("libsvm").load(data_path)
    >>> lr = LogisticRegression(regParam=0.01)
    >>> ovr = OneVsRest(classifier=lr)
    >>> ovr.getRawPredictionCol()
    'rawPrediction'
    >>> ovr.setPredictionCol("newPrediction")
    OneVsRest...
    >>> model = ovr.fit(df)
    >>> model.models[0].coefficients
    DenseVector([0.5..., -1.0..., 3.4..., 4.2...])
    >>> model.models[1].coefficients
    DenseVector([-2.1..., 3.1..., -2.6..., -2.3...])
    >>> model.models[2].coefficients
    DenseVector([0.3..., -3.4..., 1.0..., -1.1...])
    >>> [x.intercept for x in model.models]
    [-2.7..., -2.5..., -1.3...]
    >>> test0 = sc.parallelize([Row(features=Vectors.dense(-1.0, 0.0, 1.0, 1.0))]).toDF()
    >>> model.transform(test0).head().newPrediction
    0.0
    >>> test1 = sc.parallelize([Row(features=Vectors.sparse(4, [0], [1.0]))]).toDF()
    >>> model.transform(test1).head().newPrediction
    2.0
    >>> test2 = sc.parallelize([Row(features=Vectors.dense(0.5, 0.4, 0.3, 0.2))]).toDF()
    >>> model.transform(test2).head().newPrediction
    0.0
    >>> model_path = temp_path + "/ovr_model"
    >>> model.save(model_path)
    >>> model2 = OneVsRestModel.load(model_path)
    >>> model2.transform(test0).head().newPrediction
    0.0
    >>> model.transform(test0).take(1) == model2.transform(test0).take(1)
    True
    >>> model.transform(test2).columns
    ['features', 'rawPrediction', 'newPrediction']
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        rawPredictionCol: str = "rawPrediction",
        classifier: Optional[Classifier[CM]] = None,
        weightCol: Optional[str] = None,
        parallelism: int = 1,
    ):
        """
        __init__(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 rawPredictionCol="rawPrediction", classifier=None, weightCol=None, parallelism=1):
        """
        super(OneVsRest, self).__init__()
        self._setDefault(parallelism=1)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @keyword_only
    @since("2.0.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        rawPredictionCol: str = "rawPrediction",
        classifier: Optional[Classifier[CM]] = None,
        weightCol: Optional[str] = None,
        parallelism: int = 1,
    ) -> "OneVsRest":
        """
        setParams(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  rawPredictionCol="rawPrediction", classifier=None, weightCol=None, parallelism=1):
        Sets params for OneVsRest.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setClassifier(self, value: Classifier[CM]) -> "OneVsRest":
        """
        Sets the value of :py:attr:`classifier`.
        """
        return self._set(classifier=value)

    def setLabelCol(self, value: str) -> "OneVsRest":
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)

    def setFeaturesCol(self, value: str) -> "OneVsRest":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    def setPredictionCol(self, value: str) -> "OneVsRest":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    def setRawPredictionCol(self, value: str) -> "OneVsRest":
        """
        Sets the value of :py:attr:`rawPredictionCol`.
        """
        return self._set(rawPredictionCol=value)

    def setWeightCol(self, value: str) -> "OneVsRest":
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    def setParallelism(self, value: int) -> "OneVsRest":
        """
        Sets the value of :py:attr:`parallelism`.
        """
        return self._set(parallelism=value)

    def _fit(self, dataset: DataFrame) -> "OneVsRestModel":
        labelCol = self.getLabelCol()
        featuresCol = self.getFeaturesCol()
        predictionCol = self.getPredictionCol()
        classifier = self.getClassifier()

        numClasses = (
            int(cast(Row, dataset.agg({labelCol: "max"}).head())["max(" + labelCol + ")"]) + 1
        )

        weightCol = None
        if self.isDefined(self.weightCol) and self.getWeightCol():
            if isinstance(classifier, HasWeightCol):
                weightCol = self.getWeightCol()
            else:
                warnings.warn(
                    "weightCol is ignored, " "as it is not supported by {} now.".format(classifier)
                )

        if weightCol:
            multiclassLabeled = dataset.select(labelCol, featuresCol, weightCol)
        else:
            multiclassLabeled = dataset.select(labelCol, featuresCol)

        # persist if underlying dataset is not persistent.
        handlePersistence = dataset.storageLevel == StorageLevel(False, False, False, False)
        if handlePersistence:
            multiclassLabeled.persist(StorageLevel.MEMORY_AND_DISK)

        def _oneClassFitTasks(numClasses: int) -> List[Callable[[], Tuple[int, CM]]]:
            indices = iter(range(numClasses))

            def trainSingleClass() -> Tuple[int, CM]:
                index = next(indices)

                binaryLabelCol = "mc2b$" + str(index)
                trainingDataset = multiclassLabeled.withColumn(
                    binaryLabelCol,
                    when(multiclassLabeled[labelCol] == float(index), 1.0).otherwise(0.0),
                )
                paramMap = dict(
                    [
                        (classifier.labelCol, binaryLabelCol),
                        (classifier.featuresCol, featuresCol),
                        (classifier.predictionCol, predictionCol),
                    ]
                )
                if weightCol:
                    paramMap[cast(HasWeightCol, classifier).weightCol] = weightCol
                return index, classifier.fit(trainingDataset, paramMap)

            return [trainSingleClass] * numClasses

        tasks = map(
            inheritable_thread_target(dataset.sparkSession),
            _oneClassFitTasks(numClasses),
        )
        pool = ThreadPool(processes=min(self.getParallelism(), numClasses))

        subModels = [None] * numClasses
        for j, subModel in pool.imap_unordered(lambda f: f(), tasks):
            assert subModels is not None
            subModels[j] = subModel

        if handlePersistence:
            multiclassLabeled.unpersist()

        return self._copyValues(OneVsRestModel(models=cast(List[ClassificationModel], subModels)))

    def copy(self, extra: Optional["ParamMap"] = None) -> "OneVsRest":
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This creates a deep copy of the embedded paramMap,
        and copies the embedded and extra parameters over.

        .. versionadded:: 2.0.0

        Examples
        --------
        extra : dict, optional
            Extra parameters to copy to the new instance

        Returns
        -------
        :py:class:`OneVsRest`
            Copy of this instance
        """
        if extra is None:
            extra = dict()
        newOvr = Params.copy(self, extra)
        if self.isSet(self.classifier):
            newOvr.setClassifier(self.getClassifier().copy(extra))
        return newOvr

    @classmethod
    def _from_java(cls, java_stage: "JavaObject") -> "OneVsRest":
        """
        Given a Java OneVsRest, create and return a Python wrapper of it.
        Used for ML persistence.
        """
        featuresCol = java_stage.getFeaturesCol()
        labelCol = java_stage.getLabelCol()
        predictionCol = java_stage.getPredictionCol()
        rawPredictionCol = java_stage.getRawPredictionCol()
        classifier: Classifier = JavaParams._from_java(java_stage.getClassifier())
        parallelism = java_stage.getParallelism()
        py_stage = cls(
            featuresCol=featuresCol,
            labelCol=labelCol,
            predictionCol=predictionCol,
            rawPredictionCol=rawPredictionCol,
            classifier=classifier,
            parallelism=parallelism,
        )
        if java_stage.isDefined(java_stage.getParam("weightCol")):
            py_stage.setWeightCol(java_stage.getWeightCol())
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self) -> "JavaObject":
        """
        Transfer this instance to a Java OneVsRest. Used for ML persistence.

        Returns
        -------
        py4j.java_gateway.JavaObject
            Java object equivalent to this instance.
        """
        _java_obj = JavaParams._new_java_obj(
            "org.apache.spark.ml.classification.OneVsRest", self.uid
        )
        _java_obj.setClassifier(cast(_JavaClassifier, self.getClassifier())._to_java())
        _java_obj.setParallelism(self.getParallelism())
        _java_obj.setFeaturesCol(self.getFeaturesCol())
        _java_obj.setLabelCol(self.getLabelCol())
        _java_obj.setPredictionCol(self.getPredictionCol())
        if self.isDefined(self.weightCol) and self.getWeightCol():
            _java_obj.setWeightCol(self.getWeightCol())
        _java_obj.setRawPredictionCol(self.getRawPredictionCol())
        return _java_obj

    @classmethod
    @try_remote_read
    def read(cls) -> "OneVsRestReader":
        return OneVsRestReader(cls)

    @try_remote_write
    def write(self) -> MLWriter:
        if isinstance(self.getClassifier(), JavaMLWritable):
            return JavaMLWriter(self)  # type: ignore[arg-type]
        else:
            return OneVsRestWriter(self)


class _OneVsRestSharedReadWrite:
    @staticmethod
    def saveImpl(
        instance: Union[OneVsRest, "OneVsRestModel"],
        sc: Union["SparkContext", SparkSession],
        path: str,
        extraMetadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        skipParams = ["classifier"]
        jsonParams = DefaultParamsWriter.extractJsonParams(instance, skipParams)
        DefaultParamsWriter.saveMetadata(
            instance, path, sc, paramMap=jsonParams, extraMetadata=extraMetadata
        )
        classifierPath = os.path.join(path, "classifier")
        cast(MLWritable, instance.getClassifier()).save(classifierPath)

    @staticmethod
    def loadClassifier(
        path: str,
        sc: Union["SparkContext", SparkSession],
    ) -> Union[OneVsRest, "OneVsRestModel"]:
        classifierPath = os.path.join(path, "classifier")
        return DefaultParamsReader.loadParamsInstance(classifierPath, sc)

    @staticmethod
    def validateParams(instance: Union[OneVsRest, "OneVsRestModel"]) -> None:
        elems_to_check: List[Params] = [instance.getClassifier()]
        if isinstance(instance, OneVsRestModel):
            elems_to_check.extend(instance.models)

        for elem in elems_to_check:
            if not isinstance(elem, MLWritable):
                raise ValueError(
                    f"OneVsRest write will fail because it contains {elem.uid} "
                    f"which is not writable."
                )


@inherit_doc
class OneVsRestReader(MLReader[OneVsRest]):
    def __init__(self, cls: Type[OneVsRest]) -> None:
        super(OneVsRestReader, self).__init__()
        self.cls = cls

    def load(self, path: str) -> OneVsRest:
        metadata = DefaultParamsReader.loadMetadata(path, self.sparkSession)
        if not DefaultParamsReader.isPythonParamsInstance(metadata):
            return JavaMLReader(self.cls).load(path)  # type: ignore[arg-type]
        else:
            classifier = cast(
                Classifier, _OneVsRestSharedReadWrite.loadClassifier(path, self.sparkSession)
            )
            ova: OneVsRest = OneVsRest(classifier=classifier)._resetUid(metadata["uid"])
            DefaultParamsReader.getAndSetParams(ova, metadata, skipParams=["classifier"])
            return ova


@inherit_doc
class OneVsRestWriter(MLWriter):
    def __init__(self, instance: OneVsRest):
        super(OneVsRestWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path: str) -> None:
        _OneVsRestSharedReadWrite.validateParams(self.instance)
        _OneVsRestSharedReadWrite.saveImpl(self.instance, self.sparkSession, path)


class OneVsRestModel(
    Model,
    _OneVsRestParams,
    MLReadable["OneVsRestModel"],
    MLWritable,
):
    """
    Model fitted by OneVsRest.
    This stores the models resulting from training k binary classifiers: one for each class.
    Each example is scored against all k models, and the model with the highest score
    is picked to label the example.

    .. versionadded:: 2.0.0
    """

    def setFeaturesCol(self, value: str) -> "OneVsRestModel":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    def setPredictionCol(self, value: str) -> "OneVsRestModel":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    def setRawPredictionCol(self, value: str) -> "OneVsRestModel":
        """
        Sets the value of :py:attr:`rawPredictionCol`.
        """
        return self._set(rawPredictionCol=value)

    def __init__(self, models: List[ClassificationModel]):
        super(OneVsRestModel, self).__init__()
        self.models = models
        if is_remote() or not isinstance(models[0], JavaMLWritable):
            return

        from pyspark.core.context import SparkContext

        # set java instance
        java_models = [cast(_JavaClassificationModel, model)._to_java() for model in self.models]
        sc = SparkContext._active_spark_context
        assert sc is not None and sc._gateway is not None

        java_models_array = JavaWrapper._new_java_array(
            java_models,
            getattr(sc._gateway.jvm, "org.apache.spark.ml.classification.ClassificationModel"),
        )
        # TODO: need to set metadata
        metadata = JavaParams._new_java_obj("org.apache.spark.sql.types.Metadata")
        self._java_obj = JavaParams._new_java_obj(
            "org.apache.spark.ml.classification.OneVsRestModel",
            self.uid,
            metadata.empty(),
            java_models_array,
        )

    def _transform(self, dataset: DataFrame) -> DataFrame:
        # TODO(SPARK-48515): Use Arrow Python UDF
        # determine the input columns: these need to be passed through
        origCols = dataset.columns

        # add an accumulator column to store predictions of all the models
        accColName = "mbc$acc" + str(uuid.uuid4())
        initUDF = udf(lambda _: [], ArrayType(DoubleType()), useArrow=False)
        newDataset = dataset.withColumn(accColName, initUDF(dataset[origCols[0]]))

        # persist if underlying dataset is not persistent.
        handlePersistence = dataset.storageLevel == StorageLevel(False, False, False, False)
        if handlePersistence:
            newDataset.persist(StorageLevel.MEMORY_AND_DISK)

        # update the accumulator column with the result of prediction of models
        aggregatedDataset = newDataset
        for index, model in enumerate(self.models):
            rawPredictionCol = self.getRawPredictionCol()

            columns = origCols + [rawPredictionCol, accColName]

            # add temporary column to store intermediate scores and update
            tmpColName = "mbc$tmp" + str(uuid.uuid4())
            updateUDF = udf(
                lambda predictions, prediction: predictions + [prediction.tolist()[1]],
                ArrayType(DoubleType()),
                useArrow=False,
            )
            transformedDataset = model.transform(aggregatedDataset).select(*columns)
            updatedDataset = transformedDataset.withColumn(
                tmpColName,
                updateUDF(transformedDataset[accColName], transformedDataset[rawPredictionCol]),
            )
            newColumns = origCols + [tmpColName]

            # switch out the intermediate column with the accumulator column
            aggregatedDataset = updatedDataset.select(*newColumns).withColumnRenamed(
                tmpColName, accColName
            )

        if handlePersistence:
            newDataset.unpersist()

        if self.getRawPredictionCol():

            def func(predictions: Iterable[float]) -> Vector:
                predArray: List[float] = []
                for x in predictions:
                    predArray.append(x)
                return Vectors.dense(predArray)

            rawPredictionUDF = udf(func, VectorUDT(), useArrow=False)
            aggregatedDataset = aggregatedDataset.withColumn(
                self.getRawPredictionCol(), rawPredictionUDF(aggregatedDataset[accColName])
            )

        if self.getPredictionCol():
            # output the index of the classifier with highest confidence as prediction
            labelUDF = udf(
                lambda predictions: float(
                    max(enumerate(predictions), key=operator.itemgetter(1))[0]
                ),
                DoubleType(),
                useArrow=False,
            )
            aggregatedDataset = aggregatedDataset.withColumn(
                self.getPredictionCol(), labelUDF(aggregatedDataset[accColName])
            )
        return aggregatedDataset.drop(accColName)

    def copy(self, extra: Optional["ParamMap"] = None) -> "OneVsRestModel":
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This creates a deep copy of the embedded paramMap,
        and copies the embedded and extra parameters over.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        extra : dict, optional
            Extra parameters to copy to the new instance

        Returns
        -------
        :py:class:`OneVsRestModel`
            Copy of this instance
        """
        if extra is None:
            extra = dict()
        newModel = Params.copy(self, extra)
        newModel.models = [model.copy(extra) for model in self.models]
        return newModel

    @classmethod
    def _from_java(cls, java_stage: "JavaObject") -> "OneVsRestModel":
        """
        Given a Java OneVsRestModel, create and return a Python wrapper of it.
        Used for ML persistence.
        """
        featuresCol = java_stage.getFeaturesCol()
        labelCol = java_stage.getLabelCol()
        predictionCol = java_stage.getPredictionCol()
        classifier: Classifier = JavaParams._from_java(java_stage.getClassifier())
        models: List[ClassificationModel] = [
            JavaParams._from_java(model) for model in java_stage.models()
        ]
        py_stage = cls(models=models).setPredictionCol(predictionCol).setFeaturesCol(featuresCol)
        py_stage._set(labelCol=labelCol)
        if java_stage.isDefined(java_stage.getParam("weightCol")):
            py_stage._set(weightCol=java_stage.getWeightCol())
        py_stage._set(classifier=classifier)
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self) -> "JavaObject":
        """
        Transfer this instance to a Java OneVsRestModel. Used for ML persistence.

        Returns
        -------
        py4j.java_gateway.JavaObject
            Java object equivalent to this instance.
        """
        from pyspark.core.context import SparkContext

        sc = SparkContext._active_spark_context
        assert sc is not None and sc._gateway is not None

        java_models = [cast(_JavaClassificationModel, model)._to_java() for model in self.models]
        java_models_array = JavaWrapper._new_java_array(
            java_models,
            getattr(sc._gateway.jvm, "org.apache.spark.ml.classification.ClassificationModel"),
        )
        metadata = JavaParams._new_java_obj("org.apache.spark.sql.types.Metadata")
        _java_obj = JavaParams._new_java_obj(
            "org.apache.spark.ml.classification.OneVsRestModel",
            self.uid,
            metadata.empty(),
            java_models_array,
        )
        _java_obj.set("classifier", cast(_JavaClassifier, self.getClassifier())._to_java())
        _java_obj.set("featuresCol", self.getFeaturesCol())
        _java_obj.set("labelCol", self.getLabelCol())
        _java_obj.set("predictionCol", self.getPredictionCol())
        if self.isDefined(self.weightCol) and self.getWeightCol():
            _java_obj.set("weightCol", self.getWeightCol())
        return _java_obj

    @classmethod
    @try_remote_read
    def read(cls) -> "OneVsRestModelReader":
        return OneVsRestModelReader(cls)

    @try_remote_write
    def write(self) -> MLWriter:
        if all(
            map(
                lambda elem: isinstance(elem, JavaMLWritable),
                [self.getClassifier()] + self.models,  # type: ignore[operator]
            )
        ):
            return JavaMLWriter(self)  # type: ignore[arg-type]
        else:
            return OneVsRestModelWriter(self)


@inherit_doc
class OneVsRestModelReader(MLReader[OneVsRestModel]):
    def __init__(self, cls: Type[OneVsRestModel]):
        super(OneVsRestModelReader, self).__init__()
        self.cls = cls

    def load(self, path: str) -> OneVsRestModel:
        metadata = DefaultParamsReader.loadMetadata(path, self.sparkSession)
        if not DefaultParamsReader.isPythonParamsInstance(metadata):
            return JavaMLReader(self.cls).load(path)  # type: ignore[arg-type]
        else:
            classifier = _OneVsRestSharedReadWrite.loadClassifier(path, self.sparkSession)
            numClasses = metadata["numClasses"]
            subModels = [None] * numClasses
            for idx in range(numClasses):
                subModelPath = os.path.join(path, f"model_{idx}")
                subModels[idx] = DefaultParamsReader.loadParamsInstance(
                    subModelPath, self.sparkSession
                )
            ovaModel = OneVsRestModel(cast(List[ClassificationModel], subModels))._resetUid(
                metadata["uid"]
            )
            ovaModel.set(ovaModel.classifier, classifier)
            DefaultParamsReader.getAndSetParams(ovaModel, metadata, skipParams=["classifier"])
            return ovaModel


@inherit_doc
class OneVsRestModelWriter(MLWriter):
    def __init__(self, instance: OneVsRestModel):
        super(OneVsRestModelWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path: str) -> None:
        _OneVsRestSharedReadWrite.validateParams(self.instance)
        instance = self.instance
        numClasses = len(instance.models)
        extraMetadata = {"numClasses": numClasses}
        _OneVsRestSharedReadWrite.saveImpl(
            instance, self.sparkSession, path, extraMetadata=extraMetadata
        )
        for idx in range(numClasses):
            subModelPath = os.path.join(path, f"model_{idx}")
            cast(MLWritable, instance.models[idx]).save(subModelPath)


@inherit_doc
class FMClassifier(
    _JavaProbabilisticClassifier["FMClassificationModel"],
    _FactorizationMachinesParams,
    JavaMLWritable,
    JavaMLReadable["FMClassifier"],
):
    """
    Factorization Machines learning algorithm for classification.

    Solver supports:

    * gd (normal mini-batch gradient descent)
    * adamW (default)

    .. versionadded:: 3.0.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.classification import FMClassifier
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> fm = FMClassifier(factorSize=2)
    >>> fm.setSeed(11)
    FMClassifier...
    >>> model = fm.fit(df)
    >>> model.getMaxIter()
    100
    >>> test0 = spark.createDataFrame([
    ...     (Vectors.dense(-1.0),),
    ...     (Vectors.dense(0.5),),
    ...     (Vectors.dense(1.0),),
    ...     (Vectors.dense(2.0),)], ["features"])
    >>> model.predictRaw(test0.head().features)
    DenseVector([22.13..., -22.13...])
    >>> model.predictProbability(test0.head().features)
    DenseVector([1.0, 0.0])
    >>> model.transform(test0).select("features", "probability").show(10, False)
    +--------+------------------------------------------+
    |features|probability                               |
    +--------+------------------------------------------+
    |[-1.0]  |[0.9999999997574736,2.425264676902229E-10]|
    |[0.5]   |[0.47627851732981163,0.5237214826701884]  |
    |[1.0]   |[5.491554426243495E-4,0.9994508445573757] |
    |[2.0]   |[2.005766663870645E-10,0.9999999997994233]|
    +--------+------------------------------------------+
    ...
    >>> model.intercept
    -7.316665276826291
    >>> model.linear
    DenseVector([14.8232])
    >>> model.factors
    DenseMatrix(1, 2, [0.0163, -0.0051], 1)
    >>> model_path = temp_path + "/fm_model"
    >>> model.save(model_path)
    >>> model2 = FMClassificationModel.load(model_path)
    >>> model2.intercept
    -7.316665276826291
    >>> model2.linear
    DenseVector([14.8232])
    >>> model2.factors
    DenseMatrix(1, 2, [0.0163, -0.0051], 1)
    >>> model.transform(test0).take(1) == model2.transform(test0).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
        factorSize: int = 8,
        fitIntercept: bool = True,
        fitLinear: bool = True,
        regParam: float = 0.0,
        miniBatchFraction: float = 1.0,
        initStd: float = 0.01,
        maxIter: int = 100,
        stepSize: float = 1.0,
        tol: float = 1e-6,
        solver: str = "adamW",
        thresholds: Optional[List[float]] = None,
        seed: Optional[int] = None,
    ):
        """
        __init__(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", \
                 factorSize=8, fitIntercept=True, fitLinear=True, regParam=0.0, \
                 miniBatchFraction=1.0, initStd=0.01, maxIter=100, stepSize=1.0, \
                 tol=1e-6, solver="adamW", thresholds=None, seed=None)
        """
        super(FMClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.FMClassifier", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("3.0.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        probabilityCol: str = "probability",
        rawPredictionCol: str = "rawPrediction",
        factorSize: int = 8,
        fitIntercept: bool = True,
        fitLinear: bool = True,
        regParam: float = 0.0,
        miniBatchFraction: float = 1.0,
        initStd: float = 0.01,
        maxIter: int = 100,
        stepSize: float = 1.0,
        tol: float = 1e-6,
        solver: str = "adamW",
        thresholds: Optional[List[float]] = None,
        seed: Optional[int] = None,
    ) -> "FMClassifier":
        """
        setParams(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  probabilityCol="probability", rawPredictionCol="rawPrediction", \
                  factorSize=8, fitIntercept=True, fitLinear=True, regParam=0.0, \
                  miniBatchFraction=1.0, initStd=0.01, maxIter=100, stepSize=1.0, \
                  tol=1e-6, solver="adamW", thresholds=None, seed=None)
        Sets Params for FMClassifier.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> "FMClassificationModel":
        return FMClassificationModel(java_model)

    @since("3.0.0")
    def setFactorSize(self, value: int) -> "FMClassifier":
        """
        Sets the value of :py:attr:`factorSize`.
        """
        return self._set(factorSize=value)

    @since("3.0.0")
    def setFitLinear(self, value: bool) -> "FMClassifier":
        """
        Sets the value of :py:attr:`fitLinear`.
        """
        return self._set(fitLinear=value)

    @since("3.0.0")
    def setMiniBatchFraction(self, value: float) -> "FMClassifier":
        """
        Sets the value of :py:attr:`miniBatchFraction`.
        """
        return self._set(miniBatchFraction=value)

    @since("3.0.0")
    def setInitStd(self, value: float) -> "FMClassifier":
        """
        Sets the value of :py:attr:`initStd`.
        """
        return self._set(initStd=value)

    @since("3.0.0")
    def setMaxIter(self, value: int) -> "FMClassifier":
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("3.0.0")
    def setStepSize(self, value: float) -> "FMClassifier":
        """
        Sets the value of :py:attr:`stepSize`.
        """
        return self._set(stepSize=value)

    @since("3.0.0")
    def setTol(self, value: float) -> "FMClassifier":
        """
        Sets the value of :py:attr:`tol`.
        """
        return self._set(tol=value)

    @since("3.0.0")
    def setSolver(self, value: str) -> "FMClassifier":
        """
        Sets the value of :py:attr:`solver`.
        """
        return self._set(solver=value)

    @since("3.0.0")
    def setSeed(self, value: int) -> "FMClassifier":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("3.0.0")
    def setFitIntercept(self, value: bool) -> "FMClassifier":
        """
        Sets the value of :py:attr:`fitIntercept`.
        """
        return self._set(fitIntercept=value)

    @since("3.0.0")
    def setRegParam(self, value: float) -> "FMClassifier":
        """
        Sets the value of :py:attr:`regParam`.
        """
        return self._set(regParam=value)


class FMClassificationModel(
    _JavaProbabilisticClassificationModel[Vector],
    _FactorizationMachinesParams,
    JavaMLWritable,
    JavaMLReadable["FMClassificationModel"],
    HasTrainingSummary,
):
    """
    Model fitted by :class:`FMClassifier`.

    .. versionadded:: 3.0.0
    """

    @property
    @since("3.0.0")
    def intercept(self) -> float:
        """
        Model intercept.
        """
        return self._call_java("intercept")

    @property
    @since("3.0.0")
    def linear(self) -> Vector:
        """
        Model linear term.
        """
        return self._call_java("linear")

    @property
    @since("3.0.0")
    def factors(self) -> Matrix:
        """
        Model factor term.
        """
        return self._call_java("factors")

    @since("3.1.0")
    def summary(self) -> "FMClassificationTrainingSummary":
        """
        Gets summary (accuracy/precision/recall, objective history, total iterations) of model
        trained on the training set. An exception is thrown if `trainingSummary is None`.
        """
        if self.hasSummary:
            return FMClassificationTrainingSummary(super(FMClassificationModel, self).summary)
        else:
            raise RuntimeError(
                "No training summary available for this %s" % self.__class__.__name__
            )

    def evaluate(self, dataset: DataFrame) -> "FMClassificationSummary":
        """
        Evaluates the model on a test dataset.

        .. versionadded:: 3.1.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            Test dataset to evaluate model on.
        """
        if not isinstance(dataset, DataFrame):
            raise TypeError("dataset must be a DataFrame but got %s." % type(dataset))
        java_fm_summary = self._call_java("evaluate", dataset)
        return FMClassificationSummary(java_fm_summary)


class FMClassificationSummary(_BinaryClassificationSummary):
    """
    Abstraction for FMClassifier Results for a given model.

    .. versionadded:: 3.1.0
    """

    pass


@inherit_doc
class FMClassificationTrainingSummary(FMClassificationSummary, _TrainingSummary):
    """
    Abstraction for FMClassifier Training results.

    .. versionadded:: 3.1.0
    """

    pass


if __name__ == "__main__":
    import doctest
    import pyspark.ml.classification
    from pyspark.sql import SparkSession

    globs = pyspark.ml.classification.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder.master("local[2]").appName("ml.classification tests").getOrCreate()
    sc = spark.sparkContext
    globs["sc"] = sc
    globs["spark"] = spark
    import tempfile

    temp_path = tempfile.mkdtemp()
    globs["temp_path"] = temp_path
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
