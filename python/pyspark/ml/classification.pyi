#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Any, Generic, List, Optional, Type
from pyspark.ml._typing import JM, M, P, T, ParamMap

import abc
from abc import abstractmethod
from pyspark.ml import Estimator, Model, PredictionModel, Predictor, Transformer
from pyspark.ml.base import _PredictorParams
from pyspark.ml.param.shared import (
    HasAggregationDepth,
    HasBlockSize,
    HasMaxBlockSizeInMB,
    HasElasticNetParam,
    HasFitIntercept,
    HasMaxIter,
    HasParallelism,
    HasProbabilityCol,
    HasRawPredictionCol,
    HasRegParam,
    HasSeed,
    HasSolver,
    HasStandardization,
    HasStepSize,
    HasThreshold,
    HasThresholds,
    HasTol,
    HasWeightCol,
)
from pyspark.ml.regression import _FactorizationMachinesParams
from pyspark.ml.tree import (
    _DecisionTreeModel,
    _DecisionTreeParams,
    _GBTParams,
    _HasVarianceImpurity,
    _RandomForestParams,
    _TreeClassifierParams,
    _TreeEnsembleModel,
)
from pyspark.ml.util import (
    HasTrainingSummary,
    JavaMLReadable,
    JavaMLWritable,
    MLReader,
    MLReadable,
    MLWriter,
    MLWritable,
)
from pyspark.ml.wrapper import JavaPredictionModel, JavaPredictor, JavaWrapper

from pyspark.ml.linalg import Matrix, Vector
from pyspark.ml.param import Param
from pyspark.ml.regression import DecisionTreeRegressionModel
from pyspark.sql.dataframe import DataFrame

from py4j.java_gateway import JavaObject

class _ClassifierParams(HasRawPredictionCol, _PredictorParams): ...

class Classifier(Predictor, _ClassifierParams, metaclass=abc.ABCMeta):
    def setRawPredictionCol(self: P, value: str) -> P: ...

class ClassificationModel(PredictionModel, _ClassifierParams, metaclass=abc.ABCMeta):
    def setRawPredictionCol(self: P, value: str) -> P: ...
    @property
    @abc.abstractmethod
    def numClasses(self) -> int: ...
    @abstractmethod
    def predictRaw(self, value: Vector) -> Vector: ...

class _ProbabilisticClassifierParams(HasProbabilityCol, HasThresholds, _ClassifierParams): ...

class ProbabilisticClassifier(Classifier, _ProbabilisticClassifierParams, metaclass=abc.ABCMeta):
    def setProbabilityCol(self: P, value: str) -> P: ...
    def setThresholds(self: P, value: List[float]) -> P: ...

class ProbabilisticClassificationModel(
    ClassificationModel, _ProbabilisticClassifierParams, metaclass=abc.ABCMeta
):
    def setProbabilityCol(self: M, value: str) -> M: ...
    def setThresholds(self: M, value: List[float]) -> M: ...
    @abstractmethod
    def predictProbability(self, value: Vector) -> Vector: ...

class _JavaClassifier(Classifier, JavaPredictor[JM], Generic[JM], metaclass=abc.ABCMeta):
    def setRawPredictionCol(self: P, value: str) -> P: ...

class _JavaClassificationModel(ClassificationModel, JavaPredictionModel[T]):
    @property
    def numClasses(self) -> int: ...
    def predictRaw(self, value: Vector) -> Vector: ...

class _JavaProbabilisticClassifier(
    ProbabilisticClassifier, _JavaClassifier[JM], Generic[JM], metaclass=abc.ABCMeta
): ...

class _JavaProbabilisticClassificationModel(
    ProbabilisticClassificationModel, _JavaClassificationModel[T]
):
    def predictProbability(self, value: Vector) -> Vector: ...

class _ClassificationSummary(JavaWrapper):
    @property
    def predictions(self) -> DataFrame: ...
    @property
    def predictionCol(self) -> str: ...
    @property
    def labelCol(self) -> str: ...
    @property
    def weightCol(self) -> str: ...
    @property
    def labels(self) -> List[str]: ...
    @property
    def truePositiveRateByLabel(self) -> List[float]: ...
    @property
    def falsePositiveRateByLabel(self) -> List[float]: ...
    @property
    def precisionByLabel(self) -> List[float]: ...
    @property
    def recallByLabel(self) -> List[float]: ...
    def fMeasureByLabel(self, beta: float = ...) -> List[float]: ...
    @property
    def accuracy(self) -> float: ...
    @property
    def weightedTruePositiveRate(self) -> float: ...
    @property
    def weightedFalsePositiveRate(self) -> float: ...
    @property
    def weightedRecall(self) -> float: ...
    @property
    def weightedPrecision(self) -> float: ...
    def weightedFMeasure(self, beta: float = ...) -> float: ...

class _TrainingSummary(JavaWrapper):
    @property
    def objectiveHistory(self) -> List[float]: ...
    @property
    def totalIterations(self) -> int: ...

class _BinaryClassificationSummary(_ClassificationSummary):
    @property
    def scoreCol(self) -> str: ...
    @property
    def roc(self) -> DataFrame: ...
    @property
    def areaUnderROC(self) -> float: ...
    @property
    def pr(self) -> DataFrame: ...
    @property
    def fMeasureByThreshold(self) -> DataFrame: ...
    @property
    def precisionByThreshold(self) -> DataFrame: ...
    @property
    def recallByThreshold(self) -> DataFrame: ...

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
    threshold: Param[float]
    def __init__(self, *args: Any) -> None: ...

class LinearSVC(
    _JavaClassifier[LinearSVCModel],
    _LinearSVCParams,
    JavaMLWritable,
    JavaMLReadable[LinearSVC],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        regParam: float = ...,
        tol: float = ...,
        rawPredictionCol: str = ...,
        fitIntercept: bool = ...,
        standardization: bool = ...,
        threshold: float = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        maxBlockSizeInMB: float = ...,
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        regParam: float = ...,
        tol: float = ...,
        rawPredictionCol: str = ...,
        fitIntercept: bool = ...,
        standardization: bool = ...,
        threshold: float = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        maxBlockSizeInMB: float = ...,
    ) -> LinearSVC: ...
    def setMaxIter(self, value: int) -> LinearSVC: ...
    def setRegParam(self, value: float) -> LinearSVC: ...
    def setTol(self, value: float) -> LinearSVC: ...
    def setFitIntercept(self, value: bool) -> LinearSVC: ...
    def setStandardization(self, value: bool) -> LinearSVC: ...
    def setThreshold(self, value: float) -> LinearSVC: ...
    def setWeightCol(self, value: str) -> LinearSVC: ...
    def setAggregationDepth(self, value: int) -> LinearSVC: ...
    def setMaxBlockSizeInMB(self, value: float) -> LinearSVC: ...
    def _create_model(self, java_model: JavaObject) -> LinearSVCModel: ...

class LinearSVCModel(
    _JavaClassificationModel[Vector],
    _LinearSVCParams,
    JavaMLWritable,
    JavaMLReadable[LinearSVCModel],
    HasTrainingSummary[LinearSVCTrainingSummary],
):
    def setThreshold(self, value: float) -> LinearSVCModel: ...
    @property
    def coefficients(self) -> Vector: ...
    @property
    def intercept(self) -> float: ...
    def summary(self) -> LinearSVCTrainingSummary: ...
    def evaluate(self, dataset: DataFrame) -> LinearSVCSummary: ...

class LinearSVCSummary(_BinaryClassificationSummary): ...
class LinearSVCTrainingSummary(LinearSVCSummary, _TrainingSummary): ...

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
    threshold: Param[float]
    family: Param[str]
    lowerBoundsOnCoefficients: Param[Matrix]
    upperBoundsOnCoefficients: Param[Matrix]
    lowerBoundsOnIntercepts: Param[Vector]
    upperBoundsOnIntercepts: Param[Vector]
    def __init__(self, *args: Any): ...
    def setThreshold(self: P, value: float) -> P: ...
    def getThreshold(self) -> float: ...
    def setThresholds(self: P, value: List[float]) -> P: ...
    def getThresholds(self) -> List[float]: ...
    def getFamily(self) -> str: ...
    def getLowerBoundsOnCoefficients(self) -> Matrix: ...
    def getUpperBoundsOnCoefficients(self) -> Matrix: ...
    def getLowerBoundsOnIntercepts(self) -> Vector: ...
    def getUpperBoundsOnIntercepts(self) -> Vector: ...

class LogisticRegression(
    _JavaProbabilisticClassifier[LogisticRegressionModel],
    _LogisticRegressionParams,
    JavaMLWritable,
    JavaMLReadable[LogisticRegression],
):
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
    ) -> None: ...
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
    ) -> LogisticRegression: ...
    def setFamily(self, value: str) -> LogisticRegression: ...
    def setLowerBoundsOnCoefficients(self, value: Matrix) -> LogisticRegression: ...
    def setUpperBoundsOnCoefficients(self, value: Matrix) -> LogisticRegression: ...
    def setLowerBoundsOnIntercepts(self, value: Vector) -> LogisticRegression: ...
    def setUpperBoundsOnIntercepts(self, value: Vector) -> LogisticRegression: ...
    def setMaxIter(self, value: int) -> LogisticRegression: ...
    def setRegParam(self, value: float) -> LogisticRegression: ...
    def setTol(self, value: float) -> LogisticRegression: ...
    def setElasticNetParam(self, value: float) -> LogisticRegression: ...
    def setFitIntercept(self, value: bool) -> LogisticRegression: ...
    def setStandardization(self, value: bool) -> LogisticRegression: ...
    def setWeightCol(self, value: str) -> LogisticRegression: ...
    def setAggregationDepth(self, value: int) -> LogisticRegression: ...
    def setMaxBlockSizeInMB(self, value: float) -> LogisticRegression: ...
    def _create_model(self, java_model: JavaObject) -> LogisticRegressionModel: ...

class LogisticRegressionModel(
    _JavaProbabilisticClassificationModel[Vector],
    _LogisticRegressionParams,
    JavaMLWritable,
    JavaMLReadable[LogisticRegressionModel],
    HasTrainingSummary[LogisticRegressionTrainingSummary],
):
    @property
    def coefficients(self) -> Vector: ...
    @property
    def intercept(self) -> float: ...
    @property
    def coefficientMatrix(self) -> Matrix: ...
    @property
    def interceptVector(self) -> Vector: ...
    @property
    def summary(self) -> LogisticRegressionTrainingSummary: ...
    def evaluate(self, dataset: DataFrame) -> LogisticRegressionSummary: ...

class LogisticRegressionSummary(_ClassificationSummary):
    @property
    def probabilityCol(self) -> str: ...
    @property
    def featuresCol(self) -> str: ...

class LogisticRegressionTrainingSummary(LogisticRegressionSummary, _TrainingSummary): ...
class BinaryLogisticRegressionSummary(_BinaryClassificationSummary, LogisticRegressionSummary): ...
class BinaryLogisticRegressionTrainingSummary(
    BinaryLogisticRegressionSummary, LogisticRegressionTrainingSummary
): ...

class _DecisionTreeClassifierParams(_DecisionTreeParams, _TreeClassifierParams):
    def __init__(self, *args: Any): ...

class DecisionTreeClassifier(
    _JavaProbabilisticClassifier[DecisionTreeClassificationModel],
    _DecisionTreeClassifierParams,
    JavaMLWritable,
    JavaMLReadable[DecisionTreeClassifier],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        impurity: str = ...,
        seed: Optional[int] = ...,
        weightCol: Optional[str] = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        impurity: str = ...,
        seed: Optional[int] = ...,
        weightCol: Optional[str] = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
    ) -> DecisionTreeClassifier: ...
    def setMaxDepth(self, value: int) -> DecisionTreeClassifier: ...
    def setMaxBins(self, value: int) -> DecisionTreeClassifier: ...
    def setMinInstancesPerNode(self, value: int) -> DecisionTreeClassifier: ...
    def setMinWeightFractionPerNode(self, value: float) -> DecisionTreeClassifier: ...
    def setMinInfoGain(self, value: float) -> DecisionTreeClassifier: ...
    def setMaxMemoryInMB(self, value: int) -> DecisionTreeClassifier: ...
    def setCacheNodeIds(self, value: bool) -> DecisionTreeClassifier: ...
    def setImpurity(self, value: str) -> DecisionTreeClassifier: ...
    def setCheckpointInterval(self, value: int) -> DecisionTreeClassifier: ...
    def setSeed(self, value: int) -> DecisionTreeClassifier: ...
    def setWeightCol(self, value: str) -> DecisionTreeClassifier: ...
    def _create_model(self, java_model: JavaObject) -> DecisionTreeClassificationModel: ...

class DecisionTreeClassificationModel(
    _DecisionTreeModel,
    _JavaProbabilisticClassificationModel[Vector],
    _DecisionTreeClassifierParams,
    JavaMLWritable,
    JavaMLReadable[DecisionTreeClassificationModel],
):
    @property
    def featureImportances(self) -> Vector: ...

class _RandomForestClassifierParams(_RandomForestParams, _TreeClassifierParams):
    def __init__(self, *args: Any): ...

class RandomForestClassifier(
    _JavaProbabilisticClassifier[RandomForestClassificationModel],
    _RandomForestClassifierParams,
    JavaMLWritable,
    JavaMLReadable[RandomForestClassifier],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        impurity: str = ...,
        numTrees: int = ...,
        featureSubsetStrategy: str = ...,
        seed: Optional[int] = ...,
        subsamplingRate: float = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...,
        bootstrap: Optional[bool] = ...,
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        seed: Optional[int] = ...,
        impurity: str = ...,
        numTrees: int = ...,
        featureSubsetStrategy: str = ...,
        subsamplingRate: float = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...,
        bootstrap: Optional[bool] = ...,
    ) -> RandomForestClassifier: ...
    def setMaxDepth(self, value: int) -> RandomForestClassifier: ...
    def setMaxBins(self, value: int) -> RandomForestClassifier: ...
    def setMinInstancesPerNode(self, value: int) -> RandomForestClassifier: ...
    def setMinInfoGain(self, value: float) -> RandomForestClassifier: ...
    def setMaxMemoryInMB(self, value: int) -> RandomForestClassifier: ...
    def setCacheNodeIds(self, value: bool) -> RandomForestClassifier: ...
    def setImpurity(self, value: str) -> RandomForestClassifier: ...
    def setNumTrees(self, value: int) -> RandomForestClassifier: ...
    def setBootstrap(self, value: bool) -> RandomForestClassifier: ...
    def setSubsamplingRate(self, value: float) -> RandomForestClassifier: ...
    def setFeatureSubsetStrategy(self, value: str) -> RandomForestClassifier: ...
    def setSeed(self, value: int) -> RandomForestClassifier: ...
    def setCheckpointInterval(self, value: int) -> RandomForestClassifier: ...
    def setWeightCol(self, value: str) -> RandomForestClassifier: ...
    def setMinWeightFractionPerNode(self, value: float) -> RandomForestClassifier: ...
    def _create_model(self, java_model: JavaObject) -> RandomForestClassificationModel: ...

class RandomForestClassificationModel(
    _TreeEnsembleModel,
    _JavaProbabilisticClassificationModel[Vector],
    _RandomForestClassifierParams,
    JavaMLWritable,
    JavaMLReadable[RandomForestClassificationModel],
    HasTrainingSummary[RandomForestClassificationTrainingSummary],
):
    @property
    def featureImportances(self) -> Vector: ...
    @property
    def trees(self) -> List[DecisionTreeClassificationModel]: ...
    def summary(self) -> RandomForestClassificationTrainingSummary: ...
    def evaluate(self, dataset: DataFrame) -> RandomForestClassificationSummary: ...

class RandomForestClassificationSummary(_ClassificationSummary): ...
class RandomForestClassificationTrainingSummary(
    RandomForestClassificationSummary, _TrainingSummary
): ...
class BinaryRandomForestClassificationSummary(_BinaryClassificationSummary): ...
class BinaryRandomForestClassificationTrainingSummary(
    BinaryRandomForestClassificationSummary, RandomForestClassificationTrainingSummary
): ...

class _GBTClassifierParams(_GBTParams, _HasVarianceImpurity):
    supportedLossTypes: List[str]
    lossType: Param[str]
    def __init__(self, *args: Any): ...
    def getLossType(self) -> str: ...

class GBTClassifier(
    _JavaProbabilisticClassifier[GBTClassificationModel],
    _GBTClassifierParams,
    JavaMLWritable,
    JavaMLReadable[GBTClassifier],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        lossType: str = ...,
        maxIter: int = ...,
        stepSize: float = ...,
        seed: Optional[int] = ...,
        subsamplingRate: float = ...,
        featureSubsetStrategy: str = ...,
        validationTol: float = ...,
        validationIndicatorCol: Optional[str] = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...,
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        lossType: str = ...,
        maxIter: int = ...,
        stepSize: float = ...,
        seed: Optional[int] = ...,
        subsamplingRate: float = ...,
        featureSubsetStrategy: str = ...,
        validationTol: float = ...,
        validationIndicatorCol: Optional[str] = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...,
    ) -> GBTClassifier: ...
    def setMaxDepth(self, value: int) -> GBTClassifier: ...
    def setMaxBins(self, value: int) -> GBTClassifier: ...
    def setMinInstancesPerNode(self, value: int) -> GBTClassifier: ...
    def setMinInfoGain(self, value: float) -> GBTClassifier: ...
    def setMaxMemoryInMB(self, value: int) -> GBTClassifier: ...
    def setCacheNodeIds(self, value: bool) -> GBTClassifier: ...
    def setImpurity(self, value: str) -> GBTClassifier: ...
    def setLossType(self, value: str) -> GBTClassifier: ...
    def setSubsamplingRate(self, value: float) -> GBTClassifier: ...
    def setFeatureSubsetStrategy(self, value: str) -> GBTClassifier: ...
    def setValidationIndicatorCol(self, value: str) -> GBTClassifier: ...
    def setMaxIter(self, value: int) -> GBTClassifier: ...
    def setCheckpointInterval(self, value: int) -> GBTClassifier: ...
    def setSeed(self, value: int) -> GBTClassifier: ...
    def setStepSize(self, value: float) -> GBTClassifier: ...
    def setWeightCol(self, value: str) -> GBTClassifier: ...
    def setMinWeightFractionPerNode(self, value: float) -> GBTClassifier: ...
    def _create_model(self, java_model: JavaObject) -> GBTClassificationModel: ...

class GBTClassificationModel(
    _TreeEnsembleModel,
    _JavaProbabilisticClassificationModel[Vector],
    _GBTClassifierParams,
    JavaMLWritable,
    JavaMLReadable[GBTClassificationModel],
):
    @property
    def featureImportances(self) -> Vector: ...
    @property
    def trees(self) -> List[DecisionTreeRegressionModel]: ...
    def evaluateEachIteration(self, dataset: DataFrame) -> List[float]: ...

class _NaiveBayesParams(_PredictorParams, HasWeightCol):
    smoothing: Param[float]
    modelType: Param[str]
    def __init__(self, *args: Any): ...
    def getSmoothing(self) -> float: ...
    def getModelType(self) -> str: ...

class NaiveBayes(
    _JavaProbabilisticClassifier[NaiveBayesModel],
    _NaiveBayesParams,
    HasThresholds,
    HasWeightCol,
    JavaMLWritable,
    JavaMLReadable[NaiveBayes],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        smoothing: float = ...,
        modelType: str = ...,
        thresholds: Optional[List[float]] = ...,
        weightCol: Optional[str] = ...,
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        smoothing: float = ...,
        modelType: str = ...,
        thresholds: Optional[List[float]] = ...,
        weightCol: Optional[str] = ...,
    ) -> NaiveBayes: ...
    def setSmoothing(self, value: float) -> NaiveBayes: ...
    def setModelType(self, value: str) -> NaiveBayes: ...
    def setWeightCol(self, value: str) -> NaiveBayes: ...
    def _create_model(self, java_model: JavaObject) -> NaiveBayesModel: ...

class NaiveBayesModel(
    _JavaProbabilisticClassificationModel[Vector],
    _NaiveBayesParams,
    JavaMLWritable,
    JavaMLReadable[NaiveBayesModel],
):
    @property
    def pi(self) -> Vector: ...
    @property
    def theta(self) -> Matrix: ...
    @property
    def sigma(self) -> Matrix: ...

class _MultilayerPerceptronParams(
    _ProbabilisticClassifierParams,
    HasSeed,
    HasMaxIter,
    HasTol,
    HasStepSize,
    HasSolver,
    HasBlockSize,
):
    layers: Param[List[int]]
    solver: Param[str]
    initialWeights: Param[Vector]
    def __init__(self, *args: Any): ...
    def getLayers(self) -> List[int]: ...
    def getInitialWeights(self) -> Vector: ...

class MultilayerPerceptronClassifier(
    _JavaProbabilisticClassifier[MultilayerPerceptronClassificationModel],
    _MultilayerPerceptronParams,
    JavaMLWritable,
    JavaMLReadable[MultilayerPerceptronClassifier],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        tol: float = ...,
        seed: Optional[int] = ...,
        layers: Optional[List[int]] = ...,
        blockSize: int = ...,
        stepSize: float = ...,
        solver: str = ...,
        initialWeights: Optional[Vector] = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        tol: float = ...,
        seed: Optional[int] = ...,
        layers: Optional[List[int]] = ...,
        blockSize: int = ...,
        stepSize: float = ...,
        solver: str = ...,
        initialWeights: Optional[Vector] = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
    ) -> MultilayerPerceptronClassifier: ...
    def setLayers(self, value: List[int]) -> MultilayerPerceptronClassifier: ...
    def setBlockSize(self, value: int) -> MultilayerPerceptronClassifier: ...
    def setInitialWeights(self, value: Vector) -> MultilayerPerceptronClassifier: ...
    def setMaxIter(self, value: int) -> MultilayerPerceptronClassifier: ...
    def setSeed(self, value: int) -> MultilayerPerceptronClassifier: ...
    def setTol(self, value: float) -> MultilayerPerceptronClassifier: ...
    def setStepSize(self, value: float) -> MultilayerPerceptronClassifier: ...
    def setSolver(self, value: str) -> MultilayerPerceptronClassifier: ...
    def _create_model(self, java_model: JavaObject) -> MultilayerPerceptronClassificationModel: ...

class MultilayerPerceptronClassificationModel(
    _JavaProbabilisticClassificationModel[Vector],
    _MultilayerPerceptronParams,
    JavaMLWritable,
    JavaMLReadable[MultilayerPerceptronClassificationModel],
    HasTrainingSummary[MultilayerPerceptronClassificationTrainingSummary],
):
    @property
    def weights(self) -> Vector: ...
    def summary(self) -> MultilayerPerceptronClassificationTrainingSummary: ...
    def evaluate(self, dataset: DataFrame) -> MultilayerPerceptronClassificationSummary: ...

class MultilayerPerceptronClassificationSummary(_ClassificationSummary): ...
class MultilayerPerceptronClassificationTrainingSummary(
    MultilayerPerceptronClassificationSummary, _TrainingSummary
): ...

class _OneVsRestParams(_ClassifierParams, HasWeightCol):
    classifier: Param[Estimator]
    def getClassifier(self) -> Estimator[M]: ...

class OneVsRest(
    Estimator[OneVsRestModel],
    _OneVsRestParams,
    HasParallelism,
    MLReadable[OneVsRest],
    MLWritable,
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        rawPredictionCol: str = ...,
        classifier: Optional[Estimator[M]] = ...,
        weightCol: Optional[str] = ...,
        parallelism: int = ...,
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: Optional[str] = ...,
        labelCol: Optional[str] = ...,
        predictionCol: Optional[str] = ...,
        rawPredictionCol: str = ...,
        classifier: Optional[Estimator[M]] = ...,
        weightCol: Optional[str] = ...,
        parallelism: int = ...,
    ) -> OneVsRest: ...
    def _fit(self, dataset: DataFrame) -> OneVsRestModel: ...
    def setClassifier(self, value: Estimator[M]) -> OneVsRest: ...
    def setLabelCol(self, value: str) -> OneVsRest: ...
    def setFeaturesCol(self, value: str) -> OneVsRest: ...
    def setPredictionCol(self, value: str) -> OneVsRest: ...
    def setRawPredictionCol(self, value: str) -> OneVsRest: ...
    def setWeightCol(self, value: str) -> OneVsRest: ...
    def setParallelism(self, value: int) -> OneVsRest: ...
    def copy(self, extra: Optional[ParamMap] = ...) -> OneVsRest: ...

class OneVsRestModel(Model, _OneVsRestParams, MLReadable[OneVsRestModel], MLWritable):
    models: List[Transformer]
    def __init__(self, models: List[Transformer]) -> None: ...
    def _transform(self, dataset: DataFrame) -> DataFrame: ...
    def setFeaturesCol(self, value: str) -> OneVsRestModel: ...
    def setPredictionCol(self, value: str) -> OneVsRestModel: ...
    def setRawPredictionCol(self, value: str) -> OneVsRestModel: ...
    def copy(self, extra: Optional[ParamMap] = ...) -> OneVsRestModel: ...

class OneVsRestWriter(MLWriter):
    instance: OneVsRest
    def __init__(self, instance: OneVsRest) -> None: ...
    def saveImpl(self, path: str) -> None: ...

class OneVsRestReader(MLReader[OneVsRest]):
    cls: Type[OneVsRest]
    def __init__(self, cls: Type[OneVsRest]) -> None: ...
    def load(self, path: str) -> OneVsRest: ...

class OneVsRestModelWriter(MLWriter):
    instance: OneVsRestModel
    def __init__(self, instance: OneVsRestModel) -> None: ...
    def saveImpl(self, path: str) -> None: ...

class OneVsRestModelReader(MLReader[OneVsRestModel]):
    cls: Type[OneVsRestModel]
    def __init__(self, cls: Type[OneVsRestModel]) -> None: ...
    def load(self, path: str) -> OneVsRestModel: ...

class FMClassifier(
    _JavaProbabilisticClassifier[FMClassificationModel],
    _FactorizationMachinesParams,
    JavaMLWritable,
    JavaMLReadable[FMClassifier],
):
    factorSize: Param[int]
    fitLinear: Param[bool]
    miniBatchFraction: Param[float]
    initStd: Param[float]
    solver: Param[str]
    def __init__(
        self,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        factorSize: int = ...,
        fitIntercept: bool = ...,
        fitLinear: bool = ...,
        regParam: float = ...,
        miniBatchFraction: float = ...,
        initStd: float = ...,
        maxIter: int = ...,
        stepSize: float = ...,
        tol: float = ...,
        solver: str = ...,
        thresholds: Optional[Any] = ...,
        seed: Optional[Any] = ...,
    ) -> None: ...
    def setParams(
        self,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        factorSize: int = ...,
        fitIntercept: bool = ...,
        fitLinear: bool = ...,
        regParam: float = ...,
        miniBatchFraction: float = ...,
        initStd: float = ...,
        maxIter: int = ...,
        stepSize: float = ...,
        tol: float = ...,
        solver: str = ...,
        thresholds: Optional[Any] = ...,
        seed: Optional[Any] = ...,
    ) -> FMClassifier: ...
    def setFactorSize(self, value: int) -> FMClassifier: ...
    def setFitLinear(self, value: bool) -> FMClassifier: ...
    def setMiniBatchFraction(self, value: float) -> FMClassifier: ...
    def setInitStd(self, value: float) -> FMClassifier: ...
    def setMaxIter(self, value: int) -> FMClassifier: ...
    def setStepSize(self, value: float) -> FMClassifier: ...
    def setTol(self, value: float) -> FMClassifier: ...
    def setSolver(self, value: str) -> FMClassifier: ...
    def setSeed(self, value: int) -> FMClassifier: ...
    def setFitIntercept(self, value: bool) -> FMClassifier: ...
    def setRegParam(self, value: float) -> FMClassifier: ...
    def _create_model(self, java_model: JavaObject) -> FMClassificationModel: ...

class FMClassificationModel(
    _JavaProbabilisticClassificationModel[Vector],
    _FactorizationMachinesParams,
    JavaMLWritable,
    JavaMLReadable[FMClassificationModel],
):
    @property
    def intercept(self) -> float: ...
    @property
    def linear(self) -> Vector: ...
    @property
    def factors(self) -> Matrix: ...
    def summary(self) -> FMClassificationTrainingSummary: ...
    def evaluate(self, dataset: DataFrame) -> FMClassificationSummary: ...

class FMClassificationSummary(_BinaryClassificationSummary): ...
class FMClassificationTrainingSummary(FMClassificationSummary, _TrainingSummary): ...
