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

# Stubs for pyspark.ml.regression (Python 3)

from typing import Any, List, Optional, Sequence
from pyspark.ml._typing import JM, P, T

from pyspark.ml.param.shared import *
from pyspark.ml.linalg import Matrix, Vector
from pyspark.ml.util import *
from pyspark.ml.tree import (
    _DecisionTreeModel,
    _DecisionTreeParams,
    _TreeEnsembleModel,
    _TreeEnsembleParams,
    _RandomForestParams,
    _GBTParams,
    _HasVarianceImpurity,
    _TreeRegressorParams,
)
from pyspark.ml.wrapper import (
    JavaEstimator,
    JavaModel,
    JavaPredictionModel,
    JavaPredictor,
    _JavaPredictorParams,
    JavaWrapper,
)
from pyspark.sql.dataframe import DataFrame

class JavaRegressor(JavaPredictor[JM], _JavaPredictorParams): ...
class JavaRegressionModel(JavaPredictionModel[T], _JavaPredictorParams): ...

class _LinearRegressionParams(
    _JavaPredictorParams,
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
    HasBlockSize,
):
    solver: Param[str]
    loss: Param[str]
    epsilon: Param[float]
    def getEpsilon(self) -> float: ...

class LinearRegression(
    JavaPredictor[LinearRegressionModel],
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
        blockSize: int = ...
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
        blockSize: int = ...
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
    def setBlockSize(self, value: int) -> LinearRegression: ...

class LinearRegressionModel(
    JavaPredictionModel[Vector],
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
    @property
    def hasSummary(self) -> bool: ...
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

class _IsotonicRegressionParams(
    HasFeaturesCol, HasLabelCol, HasPredictionCol, HasWeightCol
):
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
        featureIndex: int = ...
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        weightCol: Optional[str] = ...,
        isotonic: bool = ...,
        featureIndex: int = ...
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

class _DecisionTreeRegressorParams(
    _DecisionTreeParams, _TreeRegressorParams, HasVarianceCol
): ...

class DecisionTreeRegressor(
    JavaPredictor[DecisionTreeRegressionModel],
    _DecisionTreeRegressorParams,
    JavaMLWritable,
    JavaMLReadable[DecisionTreeRegressor],
    HasVarianceCol,
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
        minWeightFractionPerNode: float = ...
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
        minWeightFractionPerNode: float = ...
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
    _DecisionTreeModel[T], JavaMLWritable, JavaMLReadable[DecisionTreeRegressionModel]
):
    def setVarianceCol(self, value: str) -> DecisionTreeRegressionModel: ...
    @property
    def featureImportances(self) -> Vector: ...

class _RandomForestRegressorParams(_RandomForestParams, _TreeRegressorParams): ...

class RandomForestRegressor(
    JavaPredictor[RandomForestRegressionModel],
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
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...,
        bootstrap: Optional[bool] = ...
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
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...,
        bootstrap: Optional[bool] = ...
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
    _TreeEnsembleModel[Vector],
    _RandomForestRegressorParams,
    JavaMLWritable,
    JavaMLReadable[RandomForestRegressionModel],
):
    @property
    def trees(self) -> Sequence[DecisionTreeRegressionModel]: ...
    @property
    def featureImportances(self) -> Vector: ...

class _GBTRegressorParams(_GBTParams, _TreeRegressorParams):
    supportedLossTypes: List[str]
    lossType: Param[str]
    def getLossType(self) -> str: ...

class GBTRegressor(
    JavaPredictor[GBTRegressionModel],
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
        weightCol: Optional[str] = ...
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
        impuriy: str = ...,
        featureSubsetStrategy: str = ...,
        validationTol: float = ...,
        validationIndicatorCol: Optional[str] = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...
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
    _TreeEnsembleModel[Vector],
    _GBTRegressorParams,
    JavaMLWritable,
    JavaMLReadable[GBTRegressionModel],
):
    @property
    def featureImportances(self) -> Vector: ...
    @property
    def trees(self) -> Sequence[DecisionTreeRegressionModel]: ...
    def evaluateEachIteration(self, dataset: DataFrame, loss: str) -> List[float]: ...

class _AFTSurvivalRegressionParams(
    _JavaPredictorParams, HasMaxIter, HasTol, HasFitIntercept, HasAggregationDepth
):
    censorCol: Param[str]
    quantileProbabilities: Param[List[float]]
    quantilesCol: Param[str]
    def getCensorCol(self) -> str: ...
    def getQuantileProbabilities(self) -> List[float]: ...
    def getQuantilesCol(self) -> str: ...

class AFTSurvivalRegression(
    JavaPredictor[AFTSurvivalRegressionModel],
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
        aggregationDepth: int = ...
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
        aggregationDepth: int = ...
    ) -> AFTSurvivalRegression: ...
    def setCensorCol(self, value: str) -> AFTSurvivalRegression: ...
    def setQuantileProbabilities(self, value: List[float]) -> AFTSurvivalRegression: ...
    def setQuantilesCol(self, value: str) -> AFTSurvivalRegression: ...
    def setMaxIter(self, value: int) -> AFTSurvivalRegression: ...
    def setTol(self, value: float) -> AFTSurvivalRegression: ...
    def setFitIntercept(self, value: bool) -> AFTSurvivalRegression: ...
    def setAggregationDepth(self, value: int) -> AFTSurvivalRegression: ...

class AFTSurvivalRegressionModel(
    JavaPredictionModel[Vector],
    _AFTSurvivalRegressionParams,
    JavaMLWritable,
    JavaMLReadable[AFTSurvivalRegressionModel],
):
    def setQuantileProbabilities(
        self, value: List[float]
    ) -> AFTSurvivalRegressionModel: ...
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
    _JavaPredictorParams,
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
    def getFamily(self) -> str: ...
    def getLinkPredictionCol(self) -> str: ...
    def getLink(self) -> str: ...
    def getVariancePower(self) -> float: ...
    def getLinkPower(self) -> float: ...
    def getOffsetCol(self) -> str: ...

class GeneralizedLinearRegression(
    JavaPredictor[GeneralizedLinearRegressionModel],
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
        aggregationDepth: int = ...
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
        aggregationDepth: int = ...
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
    JavaPredictionModel[Vector],
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
    @property
    def hasSummary(self) -> bool: ...
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
    _JavaPredictorParams,
    HasMaxIter,
    HasStepSize,
    HasTol,
    HasSolver,
    HasSeed,
    HasFitIntercept,
    HasRegParam,
):
    factorSize: Param[int]
    fitLinear: Param[bool]
    miniBatchFraction: Param[float]
    initStd: Param[float]
    solver: Param[str]
    def getFactorSize(self): ...
    def getFitLinear(self): ...
    def getMiniBatchFraction(self): ...
    def getInitStd(self): ...

class FMRegressor(
    JavaPredictor[FMRegressionModel],
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
    JavaPredictionModel,
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
