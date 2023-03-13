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
)

from pyspark.sql import DataFrame
from pyspark.ml.classification import (
    Classifier,
    ProbabilisticClassifier,
    ProbabilisticClassifier,
    _LogisticRegressionParams,
    _LogisticRegressionCommon,
    ClassificationModel,
    ProbabilisticClassificationModel
)
from pyspark.ml.common import inherit_doc
from pyspark.ml.linalg import (Matrix, Vector)
from pyspark import keyword_only, since, SparkContext, inheritable_thread_target
from pyspark.sql.connect.ml.base import (
    ClientEstimator,
    ClientModel,
    HasTrainingSummary,
    ClientModelSummary,
    ClientPredictor,
    ClientPredictionModel,
    ClientMLWritable,
    ClientMLReadable,
)
from abc import ABCMeta, abstractmethod


@inherit_doc
class _ClientClassifier(Classifier, ClientPredictor, metaclass=ABCMeta):
    pass


@inherit_doc
class _ClientProbabilisticClassifier(
    ProbabilisticClassifier, _ClientClassifier, metaclass=ABCMeta
):
    pass


@inherit_doc
class _ClientClassificationModel(ClassificationModel, ClientPredictionModel):
    @property  # type: ignore[misc]
    def numClasses(self) -> int:
        return self._get_model_attr("numClasses")

    def predictRaw(self, value: Vector) -> Vector:
        raise NotImplementedError()


@inherit_doc
class _ClientProbabilisticClassificationModel(
    ProbabilisticClassificationModel, _ClientClassificationModel
):
    def predictProbability(self, value: Vector) -> Vector:
        raise NotImplementedError()


@inherit_doc
class LogisticRegression(
    _ClientProbabilisticClassifier,
    _LogisticRegressionCommon,
    ClientMLWritable,
    ClientMLReadable,
):
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
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self._checkThresholdConsistency()

    @classmethod
    def _algo_name(cls):
        return "LogisticRegression"

    def _create_model(self):
        return LogisticRegressionModel()


class LogisticRegressionModel(
    _ClientProbabilisticClassificationModel,
    _LogisticRegressionParams,
    HasTrainingSummary,
    ClientMLWritable,
    ClientMLReadable,
):
    @classmethod
    def _algo_name(cls):
        return "LogisticRegression"

    @property  # type: ignore[misc]
    def coefficients(self) -> Vector:
        return self._get_model_attr("coefficients")

    @property  # type: ignore[misc]
    def intercept(self) -> float:
        return self._get_model_attr("intercept")

    @property  # type: ignore[misc]
    def coefficientMatrix(self) -> Matrix:
        return self._get_model_attr("coefficientMatrix")

    @property  # type: ignore[misc]
    def interceptVector(self) -> Vector:
        return self._get_model_attr("interceptVector")

    def evaluate(self, dataset):
        if self.numClasses <= 2:
            return BinaryLogisticRegressionSummary(self, dataset)
        else:
            return LogisticRegressionSummary(self, dataset)

    # TODO: Move this method to common interface shared by connect code and legacy code
    @property  # type: ignore[misc]
    def summary(self) -> "LogisticRegressionTrainingSummary":
        if self.hasSummary:
            if self.numClasses <= 2:
                return BinaryLogisticRegressionTrainingSummary(self, None)
            else:
                return LogisticRegressionTrainingSummary(self, None)
        else:
            raise RuntimeError(
                "No training summary available for this %s" % self.__class__.__name__
            )


class _ClassificationSummary(ClientModelSummary):

    @property  # type: ignore[misc]
    def predictions(self) -> DataFrame:
        return self._get_summary_attr_dataframe("predictions")

    @property  # type: ignore[misc]
    def predictionCol(self) -> str:
        return self._get_summary_attr("predictionCol")

    @property  # type: ignore[misc]
    def labelCol(self) -> str:
        return self._get_summary_attr("labelCol")

    @property  # type: ignore[misc]
    def weightCol(self) -> str:
        return self._get_summary_attr("weightCol")

    @property
    def labels(self) -> List[str]:
        return self._get_summary_attr("labels")

    @property  # type: ignore[misc]
    def truePositiveRateByLabel(self) -> List[float]:
        return self._get_summary_attr("truePositiveRateByLabel")

    @property  # type: ignore[misc]
    def falsePositiveRateByLabel(self) -> List[float]:
        return self._get_summary_attr("falsePositiveRateByLabel")

    @property  # type: ignore[misc]
    def precisionByLabel(self) -> List[float]:
        return self._get_summary_attr("precisionByLabel")

    @property  # type: ignore[misc]
    def recallByLabel(self) -> List[float]:
        return self._get_summary_attr("recallByLabel")

    # @property  # type: ignore[misc]
    # def fMeasureByLabel(self, beta: float = 1.0) -> List[float]:
    #     return self._get_summary_attr("fMeasureByLabel", beta)

    @property  # type: ignore[misc]
    def accuracy(self) -> float:
        return self._get_summary_attr("accuracy")

    @property  # type: ignore[misc]
    def weightedTruePositiveRate(self) -> float:
        return self._get_summary_attr("weightedTruePositiveRate")

    @property  # type: ignore[misc]
    def weightedFalsePositiveRate(self) -> float:
        return self._get_summary_attr("weightedFalsePositiveRate")

    @property  # type: ignore[misc]
    def weightedRecall(self) -> float:
        return self._get_summary_attr("weightedRecall")

    @property  # type: ignore[misc]
    def weightedPrecision(self) -> float:
        return self._get_summary_attr("weightedPrecision")

    # def weightedFMeasure(self, beta: float = 1.0) -> float:
    #     return self._get_summary_attr("weightedFMeasure", beta)


@inherit_doc
class _TrainingSummary(ClientModelSummary):

    @property  # type: ignore[misc]
    def objectiveHistory(self) -> List[float]:
        return self._get_summary_attr("objectiveHistory")

    @property  # type: ignore[misc]
    def totalIterations(self) -> int:
        return self._get_summary_attr("totalIterations")


@inherit_doc
class _BinaryClassificationSummary(_ClassificationSummary):

    @property  # type: ignore[misc]
    def scoreCol(self) -> str:
        return self._get_summary_attr("scoreCol")

    @property
    def roc(self) -> DataFrame:
        return self._get_summary_attr_dataframe("roc")

    @property  # type: ignore[misc]
    def areaUnderROC(self) -> float:
        return self._get_summary_attr("areaUnderROC")

    @property  # type: ignore[misc]
    def pr(self) -> DataFrame:
        return self._get_summary_attr_dataframe("pr")

    @property  # type: ignore[misc]
    def fMeasureByThreshold(self) -> DataFrame:
        return self._get_summary_attr_dataframe("fMeasureByThreshold")

    @property  # type: ignore[misc]
    def precisionByThreshold(self) -> DataFrame:
        return self._get_summary_attr_dataframe("precisionByThreshold")

    @property  # type: ignore[misc]
    def recallByThreshold(self) -> DataFrame:
        return self._get_summary_attr_dataframe("recallByThreshold")


class LogisticRegressionSummary(_ClassificationSummary):

    @property  # type: ignore[misc]
    def probabilityCol(self) -> str:
        return self._get_summary_attr("probabilityCol")

    @property  # type: ignore[misc]
    def featuresCol(self) -> str:
        return self._get_summary_attr("featuresCol")


@inherit_doc
class LogisticRegressionTrainingSummary(LogisticRegressionSummary, _TrainingSummary):
    pass


@inherit_doc
class BinaryLogisticRegressionSummary(_BinaryClassificationSummary, LogisticRegressionSummary):
    pass


@inherit_doc
class BinaryLogisticRegressionTrainingSummary(
    BinaryLogisticRegressionSummary, LogisticRegressionTrainingSummary
):
    pass
