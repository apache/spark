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

from typing import overload
from typing import Dict, Optional, Tuple
from pyspark.mllib._typing import VectorLike
from pyspark.rdd import RDD
from pyspark.mllib.common import JavaModelWrapper
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import JavaLoader, JavaSaveable

class TreeEnsembleModel(JavaModelWrapper, JavaSaveable):
    @overload
    def predict(self, x: VectorLike) -> float: ...
    @overload
    def predict(self, x: RDD[VectorLike]) -> RDD[VectorLike]: ...
    def numTrees(self) -> int: ...
    def totalNumNodes(self) -> int: ...
    def toDebugString(self) -> str: ...

class DecisionTreeModel(JavaModelWrapper, JavaSaveable, JavaLoader[DecisionTreeModel]):
    @overload
    def predict(self, x: VectorLike) -> float: ...
    @overload
    def predict(self, x: RDD[VectorLike]) -> RDD[VectorLike]: ...
    def numNodes(self) -> int: ...
    def depth(self) -> int: ...
    def toDebugString(self) -> str: ...

class DecisionTree:
    @classmethod
    def trainClassifier(
        cls,
        data: RDD[LabeledPoint],
        numClasses: int,
        categoricalFeaturesInfo: Dict[int, int],
        impurity: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
    ) -> DecisionTreeModel: ...
    @classmethod
    def trainRegressor(
        cls,
        data: RDD[LabeledPoint],
        categoricalFeaturesInfo: Dict[int, int],
        impurity: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
    ) -> DecisionTreeModel: ...

class RandomForestModel(TreeEnsembleModel, JavaLoader[RandomForestModel]): ...

class RandomForest:
    supportedFeatureSubsetStrategies: Tuple[str, ...]
    @classmethod
    def trainClassifier(
        cls,
        data: RDD[LabeledPoint],
        numClasses: int,
        categoricalFeaturesInfo: Dict[int, int],
        numTrees: int,
        featureSubsetStrategy: str = ...,
        impurity: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        seed: Optional[int] = ...,
    ) -> RandomForestModel: ...
    @classmethod
    def trainRegressor(
        cls,
        data: RDD[LabeledPoint],
        categoricalFeaturesInfo: Dict[int, int],
        numTrees: int,
        featureSubsetStrategy: str = ...,
        impurity: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        seed: Optional[int] = ...,
    ) -> RandomForestModel: ...

class GradientBoostedTreesModel(TreeEnsembleModel, JavaLoader[GradientBoostedTreesModel]): ...

class GradientBoostedTrees:
    @classmethod
    def trainClassifier(
        cls,
        data: RDD[LabeledPoint],
        categoricalFeaturesInfo: Dict[int, int],
        loss: str = ...,
        numIterations: int = ...,
        learningRate: float = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
    ) -> GradientBoostedTreesModel: ...
    @classmethod
    def trainRegressor(
        cls,
        data: RDD[LabeledPoint],
        categoricalFeaturesInfo: Dict[int, int],
        loss: str = ...,
        numIterations: int = ...,
        learningRate: float = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
    ) -> GradientBoostedTreesModel: ...
