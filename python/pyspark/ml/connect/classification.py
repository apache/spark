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
from pyspark.ml.linalg import (Matrix, Vector)
from pyspark import keyword_only, since, SparkContext, inheritable_thread_target
from pyspark.ml.connect.base import (
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
from pyspark.ml.util import inherit_doc
from pyspark.ml.classification import (
    LogisticRegression as PySparkLogisticRegression,
    LogisticRegressionModel as PySparkLogisticRegressionModel,
    _ClassificationSummary as _PySparkClassificationSummary,
    _TrainingSummary as _PySparkTrainingSummary,
    _BinaryClassificationSummary as _PySparkBinaryClassificationSummary,
    LogisticRegressionSummary as PySparkLogisticRegressionSummary,
    LogisticRegressionTrainingSummary as PySparkLogisticRegressionTrainingSummary,
    BinaryLogisticRegressionSummary as PySparkBinaryLogisticRegressionSummary,
    BinaryLogisticRegressionTrainingSummary as PySparkBinaryLogisticRegressionTrainingSummary,
)


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
        # TODO: support this.
        raise NotImplementedError()


@inherit_doc
class _ClientProbabilisticClassificationModel(
    ProbabilisticClassificationModel, _ClientClassificationModel
):
    def predictProbability(self, value: Vector) -> Vector:
        # TODO: support this.
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

    @classmethod
    def _model_class(cls):
        return LogisticRegressionModel


LogisticRegression.__doc__ = PySparkLogisticRegression.__doc__


@inherit_doc
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


LogisticRegressionModel.__doc__ = PySparkLogisticRegressionModel.__doc__


@inherit_doc
class _ClassificationSummary(ClientModelSummary):

    @property  # type: ignore[misc]
    def predictions(self) -> DataFrame:
        return self._get_summary_attr_dataframe("predictions")

    predictions.__doc__ = _PySparkClassificationSummary.predictions.__doc__

    @property  # type: ignore[misc]
    def predictionCol(self) -> str:
        return self._get_summary_attr("predictionCol")

    predictionCol.__doc__ = _PySparkClassificationSummary.predictionCol.__doc__

    @property  # type: ignore[misc]
    def labelCol(self) -> str:
        return self._get_summary_attr("labelCol")

    labelCol.__doc__ = _PySparkClassificationSummary.labelCol.__doc__

    @property  # type: ignore[misc]
    def weightCol(self) -> str:
        return self._get_summary_attr("weightCol")

    weightCol.__doc__ = _PySparkClassificationSummary.weightCol.__doc__

    @property
    def labels(self) -> List[str]:
        return self._get_summary_attr("labels")

    labels.__doc__ = _PySparkClassificationSummary.labels.__doc__

    @property  # type: ignore[misc]
    def truePositiveRateByLabel(self) -> List[float]:
        return self._get_summary_attr("truePositiveRateByLabel")

    truePositiveRateByLabel.__doc__ = _PySparkClassificationSummary.truePositiveRateByLabel.__doc__

    @property  # type: ignore[misc]
    def falsePositiveRateByLabel(self) -> List[float]:
        return self._get_summary_attr("falsePositiveRateByLabel")

    falsePositiveRateByLabel.__doc__ = _PySparkClassificationSummary.falsePositiveRateByLabel.__doc__

    @property  # type: ignore[misc]
    def precisionByLabel(self) -> List[float]:
        return self._get_summary_attr("precisionByLabel")

    precisionByLabel.__doc__ = _PySparkClassificationSummary.precisionByLabel.__doc__

    @property  # type: ignore[misc]
    def recallByLabel(self) -> List[float]:
        return self._get_summary_attr("recallByLabel")

    recallByLabel.__doc__ = _PySparkClassificationSummary.recallByLabel.__doc__

    @property  # type: ignore[misc]
    def fMeasureByLabel(self, beta: float = 1.0) -> List[float]:
        # TODO: support this.
        raise NotImplementedError()

    fMeasureByLabel.__doc__ = _PySparkClassificationSummary.fMeasureByLabel.__doc__

    @property  # type: ignore[misc]
    def accuracy(self) -> float:
        return self._get_summary_attr("accuracy")

    accuracy.__doc__ = _PySparkClassificationSummary.accuracy.__doc__

    @property  # type: ignore[misc]
    def weightedTruePositiveRate(self) -> float:
        return self._get_summary_attr("weightedTruePositiveRate")

    weightedTruePositiveRate.__doc__ = _PySparkClassificationSummary.weightedTruePositiveRate.__doc__

    @property  # type: ignore[misc]
    def weightedFalsePositiveRate(self) -> float:
        return self._get_summary_attr("weightedFalsePositiveRate")

    weightedFalsePositiveRate.__doc__ = _PySparkClassificationSummary.weightedFalsePositiveRate.__doc__

    @property  # type: ignore[misc]
    def weightedRecall(self) -> float:
        return self._get_summary_attr("weightedRecall")

    weightedRecall.__doc__ = _PySparkClassificationSummary.weightedRecall.__doc__

    @property  # type: ignore[misc]
    def weightedPrecision(self) -> float:
        return self._get_summary_attr("weightedPrecision")

    weightedPrecision.__doc__ = _PySparkClassificationSummary.weightedPrecision.__doc__

    def weightedFMeasure(self, beta: float = 1.0) -> float:
        # TODO: support this.
        raise NotImplementedError()

    weightedFMeasure.__doc__ = _PySparkClassificationSummary.weightedFMeasure.__doc__


@inherit_doc
class _TrainingSummary(ClientModelSummary):

    @property  # type: ignore[misc]
    def objectiveHistory(self) -> List[float]:
        return self._get_summary_attr("objectiveHistory")

    objectiveHistory.__doc__ = _PySparkTrainingSummary.objectiveHistory.__doc__

    @property  # type: ignore[misc]
    def totalIterations(self) -> int:
        return self._get_summary_attr("totalIterations")

    totalIterations.__doc__ = _PySparkTrainingSummary.totalIterations.__doc__


@inherit_doc
class _BinaryClassificationSummary(_ClassificationSummary):

    @property  # type: ignore[misc]
    def scoreCol(self) -> str:
        return self._get_summary_attr("scoreCol")

    scoreCol.__doc__ = _PySparkBinaryClassificationSummary.scoreCol.__doc__

    @property
    def roc(self) -> DataFrame:
        return self._get_summary_attr_dataframe("roc")

    roc.__doc__ = _PySparkBinaryClassificationSummary.roc.__doc__

    @property  # type: ignore[misc]
    def areaUnderROC(self) -> float:
        return self._get_summary_attr("areaUnderROC")

    areaUnderROC.__doc__ = _PySparkBinaryClassificationSummary.areaUnderROC.__doc__

    @property  # type: ignore[misc]
    def pr(self) -> DataFrame:
        return self._get_summary_attr_dataframe("pr")

    pr.__doc__ = _PySparkBinaryClassificationSummary.pr.__doc__

    @property  # type: ignore[misc]
    def fMeasureByThreshold(self) -> DataFrame:
        return self._get_summary_attr_dataframe("fMeasureByThreshold")

    fMeasureByThreshold.__doc__ = _PySparkBinaryClassificationSummary.fMeasureByThreshold.__doc__

    @property  # type: ignore[misc]
    def precisionByThreshold(self) -> DataFrame:
        return self._get_summary_attr_dataframe("precisionByThreshold")

    precisionByThreshold.__doc__ = _PySparkBinaryClassificationSummary.precisionByThreshold.__doc__

    @property  # type: ignore[misc]
    def recallByThreshold(self) -> DataFrame:
        return self._get_summary_attr_dataframe("recallByThreshold")

    recallByThreshold.__doc__ = _PySparkBinaryClassificationSummary.recallByThreshold.__doc__


@inherit_doc
class LogisticRegressionSummary(_ClassificationSummary):

    @property  # type: ignore[misc]
    def probabilityCol(self) -> str:
        return self._get_summary_attr("probabilityCol")

    probabilityCol.__doc__ = PySparkLogisticRegressionSummary.probabilityCol.__doc__

    @property  # type: ignore[misc]
    def featuresCol(self) -> str:
        return self._get_summary_attr("featuresCol")

    featuresCol.__doc__ = PySparkLogisticRegressionSummary.featuresCol.__doc__


LogisticRegressionSummary.__doc__ = PySparkLogisticRegressionSummary.__doc__


@inherit_doc
class LogisticRegressionTrainingSummary(LogisticRegressionSummary, _TrainingSummary):
    pass


LogisticRegressionTrainingSummary.__doc__ = PySparkLogisticRegressionTrainingSummary.__doc__


@inherit_doc
class BinaryLogisticRegressionSummary(_BinaryClassificationSummary, LogisticRegressionSummary):
    pass


BinaryLogisticRegressionSummary.__doc__ = PySparkBinaryLogisticRegressionSummary.__doc__


@inherit_doc
class BinaryLogisticRegressionTrainingSummary(
    BinaryLogisticRegressionSummary, LogisticRegressionTrainingSummary
):
    pass


BinaryLogisticRegressionTrainingSummary.__doc__ = PySparkBinaryLogisticRegressionTrainingSummary.__doc__


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.ml.classification

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.connect.dataframe.__dict__.copy()

    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.ml.classification tests")
            .remote("local[4]")
            .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.ml.classification,
        globs=globs,
        optionflags=doctest.ELLIPSIS
                    | doctest.NORMALIZE_WHITESPACE
                    | doctest.IGNORE_EXCEPTION_DETAIL,
    )

    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
