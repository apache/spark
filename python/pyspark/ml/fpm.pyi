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

from typing import Any, Optional

from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaParams, JavaModel
from pyspark.ml.param.shared import HasPredictionCol
from pyspark.sql.dataframe import DataFrame

from pyspark.ml.param import Param

class _FPGrowthParams(HasPredictionCol):
    itemsCol: Param[str]
    minSupport: Param[float]
    numPartitions: Param[int]
    minConfidence: Param[float]
    def __init__(self, *args: Any): ...
    def getItemsCol(self) -> str: ...
    def getMinSupport(self) -> float: ...
    def getNumPartitions(self) -> int: ...
    def getMinConfidence(self) -> float: ...

class FPGrowthModel(
    JavaModel, _FPGrowthParams, JavaMLWritable, JavaMLReadable[FPGrowthModel]
):
    def setItemsCol(self, value: str) -> FPGrowthModel: ...
    def setMinConfidence(self, value: float) -> FPGrowthModel: ...
    def setPredictionCol(self, value: str) -> FPGrowthModel: ...
    @property
    def freqItemsets(self) -> DataFrame: ...
    @property
    def associationRules(self) -> DataFrame: ...

class FPGrowth(
    JavaEstimator[FPGrowthModel],
    _FPGrowthParams,
    JavaMLWritable,
    JavaMLReadable[FPGrowth],
):
    def __init__(
        self,
        *,
        minSupport: float = ...,
        minConfidence: float = ...,
        itemsCol: str = ...,
        predictionCol: str = ...,
        numPartitions: Optional[int] = ...
    ) -> None: ...
    def setParams(
        self,
        *,
        minSupport: float = ...,
        minConfidence: float = ...,
        itemsCol: str = ...,
        predictionCol: str = ...,
        numPartitions: Optional[int] = ...
    ) -> FPGrowth: ...
    def setItemsCol(self, value: str) -> FPGrowth: ...
    def setMinSupport(self, value: float) -> FPGrowth: ...
    def setNumPartitions(self, value: int) -> FPGrowth: ...
    def setMinConfidence(self, value: float) -> FPGrowth: ...
    def setPredictionCol(self, value: str) -> FPGrowth: ...

class PrefixSpan(JavaParams):
    minSupport: Param[float]
    maxPatternLength: Param[int]
    maxLocalProjDBSize: Param[int]
    sequenceCol: Param[str]
    def __init__(
        self,
        *,
        minSupport: float = ...,
        maxPatternLength: int = ...,
        maxLocalProjDBSize: int = ...,
        sequenceCol: str = ...
    ) -> None: ...
    def setParams(
        self,
        *,
        minSupport: float = ...,
        maxPatternLength: int = ...,
        maxLocalProjDBSize: int = ...,
        sequenceCol: str = ...
    ) -> PrefixSpan: ...
    def setMinSupport(self, value: float) -> PrefixSpan: ...
    def getMinSupport(self) -> float: ...
    def setMaxPatternLength(self, value: int) -> PrefixSpan: ...
    def getMaxPatternLength(self) -> int: ...
    def setMaxLocalProjDBSize(self, value: int) -> PrefixSpan: ...
    def getMaxLocalProjDBSize(self) -> int: ...
    def setSequenceCol(self, value: str) -> PrefixSpan: ...
    def getSequenceCol(self) -> str: ...
    def findFrequentSequentialPatterns(self, dataset: DataFrame) -> DataFrame: ...
