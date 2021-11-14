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

from typing import Any, List, Optional
from pyspark.ml._typing import JM, M, T

import abc
from pyspark.ml import PredictionModel, Predictor
from pyspark.ml.base import _PredictorParams
from pyspark.ml.param.shared import (
    HasAggregationDepth,
    HasMaxBlockSizeInMB,
    HasElasticNetParam,
    HasFeaturesCol,
    HasFitIntercept,
    HasLabelCol,
    HasLoss,
    HasMaxIter,
    HasPredictionCol,
    HasRegParam,
    HasSeed,
    HasSolver,
    HasStandardization,
    HasStepSize,
    HasTol,
    HasVarianceCol,
    HasWeightCol,
)
from pyspark.ml.tree import (
    _DecisionTreeModel,
    _DecisionTreeParams,
    _GBTParams,
    _RandomForestParams,
    _TreeEnsembleModel,
    _TreeRegressorParams,
)
from pyspark.ml.util import (
    GeneralJavaMLWritable,
    HasTrainingSummary,
    JavaMLReadable,
    JavaMLWritable,
)
from pyspark.ml.wrapper import (
    JavaEstimator,
    JavaModel,
    JavaPredictionModel,
    JavaPredictor,
    JavaWrapper,
)

from pyspark.ml.linalg import Matrix, Vector
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame

class Regressor(Predictor[M], _PredictorParams, metaclass=abc.ABCMeta): ...
class RegressionModel(PredictionModel[T], _PredictorParams, metaclass=abc.ABCMeta): ...
class _JavaRegressor(Regressor, JavaPredictor[JM], metaclass=abc.ABCMeta): ...
class _JavaRegressionModel(RegressionModel, JavaPredictionModel[T], metaclass=abc.ABCMeta): ...

class _LinearRegressionParams(
    _PredictorParams,
    HasRegParam,
    HasElasticNetParam,
    HasMaxIter,
    HasTol,
    HasFitIntercept,
    HasStandardization,
    HasWeightCol,
    HasSolver,
    HasAggregationDepth,
    HasLoss,
    HasMaxBlockSizeInMB,
):
    solver: Param[str]
    loss: Param[str]
    epsilon: Param[float]
    def __init__(self, *args: Any): ...
    def getEpsilon(self) -> float: ...

class LinearRegression(
    _JavaRegressor[LinearRegressionModel],
    _LinearRegressionParams,
    JavaMLWritable,
    JavaMLReadable[LinearRegression],
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
        standardization: bool = ...,
        solver: str = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        epsilon: float = ...,
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
        standardization: bool = ...,
        solver: str = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        epsilon: float = ...,
        maxBlockSizeInMB: float = ...,
    ) -> LinearRegression: ...
    def setEpsilon(self, value: float) -> LinearRegression: ...
    def setMaxIter(self, value: int) -> LinearRegression: ...
    def setRegParam(self, value: float) -> LinearRegression: ...
    def setTol(self, value: float) -> LinearRegression: ...
    def setElasticNetParam(self, value: float) -> LinearRegression: ...
    def setFitIntercept(self, value: bool) -> LinearRegression: ...
    def setStandardization(self, value: bool) -> LinearRegression: ...
    def setWeightCol(self, value: str) -> LinearRegression: ...
    def setSolver(self, value: str) -> LinearRegression: ...
    def setAggregationDepth(self, value: int) -> LinearRegression: ...
    def setLoss(self, value: str) -> LinearRegression: ...
    def setMaxBlockSizeInMB(self, value: float) -> LinearRegression: ...

class LinearRegressionModel(
    _JavaRegressionModel[Vector],
    _LinearRegressionParams,
    GeneralJavaMLWritable,
    JavaMLReadable[LinearRegressionModel],
    HasTrainingSummary[LinearRegressionSummary],
):
    @property
    def coefficients(self) -> Vector: ...
    @property
    def intercept(self) -> float: ...
    @property
    def summary(self) -> LinearRegressionTrainingSummary: ...
    def evaluate(self, dataset: DataFrame) -> LinearRegressionSummary: ...

class LinearRegressionSummary(JavaWrapper):
    @property
    def predictions(self) -> DataFrame: ...
    @property
    def predictionCol(self) -> str: ...
    @property
    def labelCol(self) -> str: ...
    @property
    def featuresCol(self) -> str: ...
    @property
    def explainedVariance(self) -> float: ...
    @property
    def meanAbsoluteError(self) -> float: ...
    @property
    def meanSquaredError(self) -> float: ...
    @property
    def rootMeanSquaredError(self) -> float: ...
    @property
    def r2(self) -> float: ...
    @property
    def r2adj(self) -> float: ...
    @property
    def residuals(self) -> DataFrame: ...
    @property
    def numInstances(self) -> int: ...
    @property
    def devianceResiduals(self) -> List[float]: ...
    @property
    def coefficientStandardErrors(self) -> List[float]: ...
    @property
    def tValues(self) -> List[float]: ...
    @property
    def pValues(self) -> List[float]: ...

class LinearRegressionTrainingSummary(LinearRegressionSummary):
    @property
    def objectiveHistory(self) -> List[float]: ...
    @property
    def totalIterations(self) -> int: ...

class _IsotonicRegressionParams(HasFeaturesCol, HasLabelCol, HasPredictionCol, HasWeightCol):
    isotonic: Param[bool]
    featureIndex: Param[int]
    def getIsotonic(self) -> bool: ...
    def getFeatureIndex(self) -> int: ...

class IsotonicRegression(
    JavaEstimator[IsotonicRegressionModel],
    _IsotonicRegressionParams,
    HasWeightCol,
    JavaMLWritable,
    JavaMLReadable[IsotonicRegression],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        weightCol: Optional[str] = ...,
        isotonic: bool = ...,
        featureIndex: int = ...,
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        weightCol: Optional[str] = ...,
        isotonic: bool = ...,
        featureIndex: int = ...,
    ) -> IsotonicRegression: ...
    def setIsotonic(self, value: bool) -> IsotonicRegression: ...
    def setFeatureIndex(self, value: int) -> IsotonicRegression: ...
    def setFeaturesCol(self, value: str) -> IsotonicRegression: ...
    def setPredictionCol(self, value: str) -> IsotonicRegression: ...
    def setLabelCol(self, value: str) -> IsotonicRegression: ...
    def setWeightCol(self, value: str) -> IsotonicRegression: ...

class IsotonicRegressionModel(
    JavaModel,
    _IsotonicRegressionParams,
    JavaMLWritable,
    JavaMLReadable[IsotonicRegressionModel],
):
    def setFeaturesCol(self, value: str) -> IsotonicRegressionModel: ...
    def setPredictionCol(self, value: str) -> IsotonicRegressionModel: ...
    def setFeatureIndex(self, value: int) -> IsotonicRegressionModel: ...
    @property
    def boundaries(self) -> Vector: ...
    @property
    def predictions(self) -> Vector: ...
    @property
    def numFeatures(self) -> int: ...
    def predict(self, value: float) -> float: ...

class _DecisionTreeRegressorParams(_DecisionTreeParams, _TreeRegressorParams, HasVarianceCol):
    def __init__(self, *args: Any): ...

class DecisionTreeRegressor(
    _JavaRegressor[DecisionTreeRegressionModel],
    _DecisionTreeRegressorParams,
    JavaMLWritable,
    JavaMLReadable[DecisionTreeRegressor],
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
        impurity: str = ...,
        seed: Optional[int] = ...,
        varianceCol: Optional[str] = ...,
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
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        impurity: str = ...,
        seed: Optional[int] = ...,
        varianceCol: Optional[str] = ...,
        weightCol: Optional[str] = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
    ) -> DecisionTreeRegressor: ...
    def setMaxDepth(self, value: int) -> DecisionTreeRegressor: ...
    def setMaxBins(self, value: int) -> DecisionTreeRegressor: ...
    def setMinInstancesPerNode(self, value: int) -> DecisionTreeRegressor: ...
    def setMinWeightFractionPerNode(self, value: float) -> DecisionTreeRegressor: ...
    def setMinInfoGain(self, value: float) -> DecisionTreeRegressor: ...
    def setMaxMemoryInMB(self, value: int) -> DecisionTreeRegressor: ...
    def setCacheNodeIds(self, value: bool) -> DecisionTreeRegressor: ...
    def setImpurity(self, value: str) -> DecisionTreeRegressor: ...
    def setCheckpointInterval(self, value: int) -> DecisionTreeRegressor: ...
    def setSeed(self, value: int) -> DecisionTreeRegressor: ...
    def setWeightCol(self, value: str) -> DecisionTreeRegressor: ...
    def setVarianceCol(self, value: str) -> DecisionTreeRegressor: ...

class DecisionTreeRegressionModel(
    _JavaRegressionModel[Vector],
    _DecisionTreeModel,
    _DecisionTreeRegressorParams,
    JavaMLWritable,
    JavaMLReadable[DecisionTreeRegressionModel],
):
    def setVarianceCol(self, value: str) -> DecisionTreeRegressionModel: ...
    @property
    def featureImportances(self) -> Vector: ...

class _RandomForestRegressorParams(_RandomForestParams, _TreeRegressorParams):
    def __init__(self, *args: Any): ...

class RandomForestRegressor(
    _JavaRegressor[RandomForestRegressionModel],
    _RandomForestRegressorParams,
    JavaMLWritable,
    JavaMLReadable[RandomForestRegressor],
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
        impurity: str = ...,
        subsamplingRate: float = ...,
        seed: Optional[int] = ...,
        numTrees: int = ...,
        featureSubsetStrategy: str = ...,
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
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        impurity: str = ...,
        subsamplingRate: float = ...,
        seed: Optional[int] = ...,
        numTrees: int = ...,
        featureSubsetStrategy: str = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...,
        bootstrap: Optional[bool] = ...,
    ) -> RandomForestRegressor: ...
    def setMaxDepth(self, value: int) -> RandomForestRegressor: ...
    def setMaxBins(self, value: int) -> RandomForestRegressor: ...
    def setMinInstancesPerNode(self, value: int) -> RandomForestRegressor: ...
    def setMinInfoGain(self, value: float) -> RandomForestRegressor: ...
    def setMaxMemoryInMB(self, value: int) -> RandomForestRegressor: ...
    def setCacheNodeIds(self, value: bool) -> RandomForestRegressor: ...
    def setImpurity(self, value: str) -> RandomForestRegressor: ...
    def setNumTrees(self, value: int) -> RandomForestRegressor: ...
    def setBootstrap(self, value: bool) -> RandomForestRegressor: ...
    def setSubsamplingRate(self, value: float) -> RandomForestRegressor: ...
    def setFeatureSubsetStrategy(self, value: str) -> RandomForestRegressor: ...
    def setCheckpointInterval(self, value: int) -> RandomForestRegressor: ...
    def setSeed(self, value: int) -> RandomForestRegressor: ...
    def setWeightCol(self, value: str) -> RandomForestRegressor: ...
    def setMinWeightFractionPerNode(self, value: float) -> RandomForestRegressor: ...

class RandomForestRegressionModel(
    _JavaRegressionModel[Vector],
    _TreeEnsembleModel,
    _RandomForestRegressorParams,
    JavaMLWritable,
    JavaMLReadable[RandomForestRegressionModel],
):
    @property
    def trees(self) -> List[DecisionTreeRegressionModel]: ...
    @property
    def featureImportances(self) -> Vector: ...

class _GBTRegressorParams(_GBTParams, _TreeRegressorParams):
    supportedLossTypes: List[str]
    lossType: Param[str]
    def __init__(self, *args: Any): ...
    def getLossType(self) -> str: ...

class GBTRegressor(
    _JavaRegressor[GBTRegressionModel],
    _GBTRegressorParams,
    JavaMLWritable,
    JavaMLReadable[GBTRegressor],
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
        subsamplingRate: float = ...,
        checkpointInterval: int = ...,
        lossType: str = ...,
        maxIter: int = ...,
        stepSize: float = ...,
        seed: Optional[int] = ...,
        impurity: str = ...,
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
        subsamplingRate: float = ...,
        checkpointInterval: int = ...,
        lossType: str = ...,
        maxIter: int = ...,
        stepSize: float = ...,
        seed: Optional[int] = ...,
        impurity: str = ...,
        featureSubsetStrategy: str = ...,
        validationTol: float = ...,
        validationIndicatorCol: Optional[str] = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...,
    ) -> GBTRegressor: ...
    def setMaxDepth(self, value: int) -> GBTRegressor: ...
    def setMaxBins(self, value: int) -> GBTRegressor: ...
    def setMinInstancesPerNode(self, value: int) -> GBTRegressor: ...
    def setMinInfoGain(self, value: float) -> GBTRegressor: ...
    def setMaxMemoryInMB(self, value: int) -> GBTRegressor: ...
    def setCacheNodeIds(self, value: bool) -> GBTRegressor: ...
    def setImpurity(self, value: str) -> GBTRegressor: ...
    def setLossType(self, value: str) -> GBTRegressor: ...
    def setSubsamplingRate(self, value: float) -> GBTRegressor: ...
    def setFeatureSubsetStrategy(self, value: str) -> GBTRegressor: ...
    def setValidationIndicatorCol(self, value: str) -> GBTRegressor: ...
    def setMaxIter(self, value: int) -> GBTRegressor: ...
    def setCheckpointInterval(self, value: int) -> GBTRegressor: ...
    def setSeed(self, value: int) -> GBTRegressor: ...
    def setStepSize(self, value: float) -> GBTRegressor: ...
    def setWeightCol(self, value: str) -> GBTRegressor: ...
    def setMinWeightFractionPerNode(self, value: float) -> GBTRegressor: ...

class GBTRegressionModel(
    _JavaRegressionModel[Vector],
    _TreeEnsembleModel,
    _GBTRegressorParams,
    JavaMLWritable,
    JavaMLReadable[GBTRegressionModel],
):
    @property
    def featureImportances(self) -> Vector: ...
    @property
    def trees(self) -> List[DecisionTreeRegressionModel]: ...
    def evaluateEachIteration(self, dataset: DataFrame, loss: str) -> List[float]: ...

class _AFTSurvivalRegressionParams(
    _PredictorParams,
    HasMaxIter,
    HasTol,
    HasFitIntercept,
    HasAggregationDepth,
    HasMaxBlockSizeInMB,
):
    censorCol: Param[str]
    quantileProbabilities: Param[List[float]]
    quantilesCol: Param[str]
    def __init__(self, *args: Any): ...
    def getCensorCol(self) -> str: ...
    def getQuantileProbabilities(self) -> List[float]: ...
    def getQuantilesCol(self) -> str: ...

class AFTSurvivalRegression(
    _JavaRegressor[AFTSurvivalRegressionModel],
    _AFTSurvivalRegressionParams,
    JavaMLWritable,
    JavaMLReadable[AFTSurvivalRegression],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        fitIntercept: bool = ...,
        maxIter: int = ...,
        tol: float = ...,
        censorCol: str = ...,
        quantileProbabilities: List[float] = ...,
        quantilesCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        maxBlockSizeInMB: float = ...,
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        fitIntercept: bool = ...,
        maxIter: int = ...,
        tol: float = ...,
        censorCol: str = ...,
        quantileProbabilities: List[float] = ...,
        quantilesCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        maxBlockSizeInMB: float = ...,
    ) -> AFTSurvivalRegression: ...
    def setCensorCol(self, value: str) -> AFTSurvivalRegression: ...
    def setQuantileProbabilities(self, value: List[float]) -> AFTSurvivalRegression: ...
    def setQuantilesCol(self, value: str) -> AFTSurvivalRegression: ...
    def setMaxIter(self, value: int) -> AFTSurvivalRegression: ...
    def setTol(self, value: float) -> AFTSurvivalRegression: ...
    def setFitIntercept(self, value: bool) -> AFTSurvivalRegression: ...
    def setAggregationDepth(self, value: int) -> AFTSurvivalRegression: ...
    def setMaxBlockSizeInMB(self, value: float) -> AFTSurvivalRegression: ...

class AFTSurvivalRegressionModel(
    _JavaRegressionModel[Vector],
    _AFTSurvivalRegressionParams,
    JavaMLWritable,
    JavaMLReadable[AFTSurvivalRegressionModel],
):
    def setQuantileProbabilities(self, value: List[float]) -> AFTSurvivalRegressionModel: ...
    def setQuantilesCol(self, value: str) -> AFTSurvivalRegressionModel: ...
    @property
    def coefficients(self) -> Vector: ...
    @property
    def intercept(self) -> float: ...
    @property
    def scale(self) -> float: ...
    def predictQuantiles(self, features: Vector) -> Vector: ...
    def predict(self, features: Vector) -> float: ...

class _GeneralizedLinearRegressionParams(
    _PredictorParams,
    HasFitIntercept,
    HasMaxIter,
    HasTol,
    HasRegParam,
    HasWeightCol,
    HasSolver,
    HasAggregationDepth,
):
    family: Param[str]
    link: Param[str]
    linkPredictionCol: Param[str]
    variancePower: Param[float]
    linkPower: Param[float]
    solver: Param[str]
    offsetCol: Param[str]
    def __init__(self, *args: Any): ...
    def getFamily(self) -> str: ...
    def getLinkPredictionCol(self) -> str: ...
    def getLink(self) -> str: ...
    def getVariancePower(self) -> float: ...
    def getLinkPower(self) -> float: ...
    def getOffsetCol(self) -> str: ...

class GeneralizedLinearRegression(
    _JavaRegressor[GeneralizedLinearRegressionModel],
    _GeneralizedLinearRegressionParams,
    JavaMLWritable,
    JavaMLReadable[GeneralizedLinearRegression],
):
    def __init__(
        self,
        *,
        labelCol: str = ...,
        featuresCol: str = ...,
        predictionCol: str = ...,
        family: str = ...,
        link: Optional[str] = ...,
        fitIntercept: bool = ...,
        maxIter: int = ...,
        tol: float = ...,
        regParam: float = ...,
        weightCol: Optional[str] = ...,
        solver: str = ...,
        linkPredictionCol: Optional[str] = ...,
        variancePower: float = ...,
        linkPower: Optional[float] = ...,
        offsetCol: Optional[str] = ...,
        aggregationDepth: int = ...,
    ) -> None: ...
    def setParams(
        self,
        *,
        labelCol: str = ...,
        featuresCol: str = ...,
        predictionCol: str = ...,
        family: str = ...,
        link: Optional[str] = ...,
        fitIntercept: bool = ...,
        maxIter: int = ...,
        tol: float = ...,
        regParam: float = ...,
        weightCol: Optional[str] = ...,
        solver: str = ...,
        linkPredictionCol: Optional[str] = ...,
        variancePower: float = ...,
        linkPower: Optional[float] = ...,
        offsetCol: Optional[str] = ...,
        aggregationDepth: int = ...,
    ) -> GeneralizedLinearRegression: ...
    def setFamily(self, value: str) -> GeneralizedLinearRegression: ...
    def setLinkPredictionCol(self, value: str) -> GeneralizedLinearRegression: ...
    def setLink(self, value: str) -> GeneralizedLinearRegression: ...
    def setVariancePower(self, value: float) -> GeneralizedLinearRegression: ...
    def setLinkPower(self, value: float) -> GeneralizedLinearRegression: ...
    def setOffsetCol(self, value: str) -> GeneralizedLinearRegression: ...
    def setMaxIter(self, value: int) -> GeneralizedLinearRegression: ...
    def setRegParam(self, value: float) -> GeneralizedLinearRegression: ...
    def setTol(self, value: float) -> GeneralizedLinearRegression: ...
    def setFitIntercept(self, value: bool) -> GeneralizedLinearRegression: ...
    def setWeightCol(self, value: str) -> GeneralizedLinearRegression: ...
    def setSolver(self, value: str) -> GeneralizedLinearRegression: ...
    def setAggregationDepth(self, value: int) -> GeneralizedLinearRegression: ...

class GeneralizedLinearRegressionModel(
    _JavaRegressionModel[Vector],
    _GeneralizedLinearRegressionParams,
    JavaMLWritable,
    JavaMLReadable[GeneralizedLinearRegressionModel],
    HasTrainingSummary[GeneralizedLinearRegressionTrainingSummary],
):
    def setLinkPredictionCol(self, value: str) -> GeneralizedLinearRegressionModel: ...
    @property
    def coefficients(self) -> Vector: ...
    @property
    def intercept(self) -> float: ...
    @property
    def summary(self) -> GeneralizedLinearRegressionTrainingSummary: ...
    def evaluate(self, dataset: DataFrame) -> GeneralizedLinearRegressionSummary: ...

class GeneralizedLinearRegressionSummary(JavaWrapper):
    @property
    def predictions(self) -> DataFrame: ...
    @property
    def predictionCol(self) -> str: ...
    @property
    def rank(self) -> int: ...
    @property
    def degreesOfFreedom(self) -> int: ...
    @property
    def residualDegreeOfFreedom(self) -> int: ...
    @property
    def residualDegreeOfFreedomNull(self) -> int: ...
    def residuals(self, residualsType: str = ...) -> DataFrame: ...
    @property
    def nullDeviance(self) -> float: ...
    @property
    def deviance(self) -> float: ...
    @property
    def dispersion(self) -> float: ...
    @property
    def aic(self) -> float: ...

class GeneralizedLinearRegressionTrainingSummary(GeneralizedLinearRegressionSummary):
    @property
    def numIterations(self) -> int: ...
    @property
    def solver(self) -> str: ...
    @property
    def coefficientStandardErrors(self) -> List[float]: ...
    @property
    def tValues(self) -> List[float]: ...
    @property
    def pValues(self) -> List[float]: ...

class _FactorizationMachinesParams(
    _PredictorParams,
    HasMaxIter,
    HasStepSize,
    HasTol,
    HasSolver,
    HasSeed,
    HasFitIntercept,
    HasRegParam,
    HasWeightCol,
):
    factorSize: Param[int]
    fitLinear: Param[bool]
    miniBatchFraction: Param[float]
    initStd: Param[float]
    solver: Param[str]
    def __init__(self, *args: Any): ...
    def getFactorSize(self) -> int: ...
    def getFitLinear(self) -> bool: ...
    def getMiniBatchFraction(self) -> float: ...
    def getInitStd(self) -> float: ...

class FMRegressor(
    _JavaRegressor[FMRegressionModel],
    _FactorizationMachinesParams,
    JavaMLWritable,
    JavaMLReadable[FMRegressor],
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
        seed: Optional[int] = ...,
    ) -> None: ...
    def setParams(
        self,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
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
        seed: Optional[int] = ...,
    ) -> FMRegressor: ...
    def setFactorSize(self, value: int) -> FMRegressor: ...
    def setFitLinear(self, value: bool) -> FMRegressor: ...
    def setMiniBatchFraction(self, value: float) -> FMRegressor: ...
    def setInitStd(self, value: float) -> FMRegressor: ...
    def setMaxIter(self, value: int) -> FMRegressor: ...
    def setStepSize(self, value: float) -> FMRegressor: ...
    def setTol(self, value: float) -> FMRegressor: ...
    def setSolver(self, value: str) -> FMRegressor: ...
    def setSeed(self, value: int) -> FMRegressor: ...
    def setFitIntercept(self, value: bool) -> FMRegressor: ...
    def setRegParam(self, value: float) -> FMRegressor: ...

class FMRegressionModel(
    _JavaRegressionModel,
    _FactorizationMachinesParams,
    JavaMLWritable,
    JavaMLReadable[FMRegressionModel],
):
    @property
    def intercept(self) -> float: ...
    @property
    def linear(self) -> Vector: ...
    @property
    def factors(self) -> Matrix: ...
