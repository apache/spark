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

from typing import List, Sequence
from pyspark.ml._typing import P, T

from pyspark.ml.linalg import Vector
from pyspark import since as since  # noqa: F401
from pyspark.ml.common import inherit_doc as inherit_doc  # noqa: F401
from pyspark.ml.param import Param, Params as Params
from pyspark.ml.param.shared import (  # noqa: F401
    HasCheckpointInterval as HasCheckpointInterval,
    HasMaxIter as HasMaxIter,
    HasSeed as HasSeed,
    HasStepSize as HasStepSize,
    HasValidationIndicatorCol as HasValidationIndicatorCol,
    HasWeightCol as HasWeightCol,
    Param as Param,
    TypeConverters as TypeConverters,
)
from pyspark.ml.wrapper import JavaPredictionModel as JavaPredictionModel

class _DecisionTreeModel(JavaPredictionModel[T]):
    @property
    def numNodes(self) -> int: ...
    @property
    def depth(self) -> int: ...
    @property
    def toDebugString(self) -> str: ...
    def predictLeaf(self, value: Vector) -> float: ...

class _DecisionTreeParams(HasCheckpointInterval, HasSeed, HasWeightCol):
    leafCol: Param[str]
    maxDepth: Param[int]
    maxBins: Param[int]
    minInstancesPerNode: Param[int]
    minWeightFractionPerNode: Param[float]
    minInfoGain: Param[float]
    maxMemoryInMB: Param[int]
    cacheNodeIds: Param[bool]
    def __init__(self) -> None: ...
    def setLeafCol(self: P, value: str) -> P: ...
    def getLeafCol(self) -> str: ...
    def getMaxDepth(self) -> int: ...
    def getMaxBins(self) -> int: ...
    def getMinInstancesPerNode(self) -> int: ...
    def getMinInfoGain(self) -> float: ...
    def getMaxMemoryInMB(self) -> int: ...
    def getCacheNodeIds(self) -> bool: ...

class _TreeEnsembleModel(JavaPredictionModel[T]):
    @property
    def trees(self) -> Sequence[_DecisionTreeModel]: ...
    @property
    def getNumTrees(self) -> int: ...
    @property
    def treeWeights(self) -> List[float]: ...
    @property
    def totalNumNodes(self) -> int: ...
    @property
    def toDebugString(self) -> str: ...

class _TreeEnsembleParams(_DecisionTreeParams):
    subsamplingRate: Param[float]
    supportedFeatureSubsetStrategies: List[str]
    featureSubsetStrategy: Param[str]
    def __init__(self) -> None: ...
    def getSubsamplingRate(self) -> float: ...
    def getFeatureSubsetStrategy(self) -> str: ...

class _RandomForestParams(_TreeEnsembleParams):
    numTrees: Param[int]
    bootstrap: Param[bool]
    def __init__(self) -> None: ...
    def getNumTrees(self) -> int: ...
    def getBootstrap(self) -> bool: ...

class _GBTParams(_TreeEnsembleParams, HasMaxIter, HasStepSize, HasValidationIndicatorCol):
    stepSize: Param[float]
    validationTol: Param[float]
    def getValidationTol(self) -> float: ...

class _HasVarianceImpurity(Params):
    supportedImpurities: List[str]
    impurity: Param[str]
    def __init__(self) -> None: ...
    def getImpurity(self) -> str: ...

class _TreeClassifierParams(Params):
    supportedImpurities: List[str]
    impurity: Param[str]
    def __init__(self) -> None: ...
    def getImpurity(self) -> str: ...

class _TreeRegressorParams(_HasVarianceImpurity): ...
