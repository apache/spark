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

from typing import Any, Dict, List, Optional, Tuple, Type, Union

from pyspark.ml._typing import PipelineStage
from pyspark.context import SparkContext
from pyspark.ml.base import Estimator, Model, Transformer
from pyspark.ml.param import Param
from pyspark.ml.util import (  # noqa: F401
    DefaultParamsReader as DefaultParamsReader,
    DefaultParamsWriter as DefaultParamsWriter,
    JavaMLReader as JavaMLReader,
    JavaMLWritable as JavaMLWritable,
    JavaMLWriter as JavaMLWriter,
    MLReadable as MLReadable,
    MLReader as MLReader,
    MLWritable as MLWritable,
    MLWriter as MLWriter,
)
from pyspark.sql.dataframe import DataFrame

class Pipeline(Estimator[PipelineModel], MLReadable[Pipeline], MLWritable):
    stages: List[PipelineStage]
    def __init__(self, *, stages: Optional[List[PipelineStage]] = ...) -> None: ...
    def _fit(self, dataset: DataFrame) -> PipelineModel: ...
    def setStages(self, stages: List[PipelineStage]) -> Pipeline: ...
    def getStages(self) -> List[PipelineStage]: ...
    def setParams(self, *, stages: Optional[List[PipelineStage]] = ...) -> Pipeline: ...
    def copy(self, extra: Optional[Dict[Param, str]] = ...) -> Pipeline: ...
    def write(self) -> JavaMLWriter: ...
    def save(self, path: str) -> None: ...
    @classmethod
    def read(cls) -> PipelineReader: ...

class PipelineWriter(MLWriter):
    instance: Pipeline
    def __init__(self, instance: Pipeline) -> None: ...
    def saveImpl(self, path: str) -> None: ...

class PipelineReader(MLReader[Pipeline]):
    cls: Type[Pipeline]
    def __init__(self, cls: Type[Pipeline]) -> None: ...
    def load(self, path: str) -> Pipeline: ...

class PipelineModelWriter(MLWriter):
    instance: PipelineModel
    def __init__(self, instance: PipelineModel) -> None: ...
    def saveImpl(self, path: str) -> None: ...

class PipelineModelReader(MLReader[PipelineModel]):
    cls: Type[PipelineModel]
    def __init__(self, cls: Type[PipelineModel]) -> None: ...
    def load(self, path: str) -> PipelineModel: ...

class PipelineModel(Model, MLReadable[PipelineModel], MLWritable):
    stages: List[PipelineStage]
    def __init__(self, stages: List[Transformer]) -> None: ...
    def _transform(self, dataset: DataFrame) -> DataFrame: ...
    def copy(self, extra: Optional[Dict[Param, Any]] = ...) -> PipelineModel: ...
    def write(self) -> JavaMLWriter: ...
    def save(self, path: str) -> None: ...
    @classmethod
    def read(cls) -> PipelineModelReader: ...

class PipelineSharedReadWrite:
    @staticmethod
    def checkStagesForJava(stages: List[PipelineStage]) -> bool: ...
    @staticmethod
    def validateStages(stages: List[PipelineStage]) -> None: ...
    @staticmethod
    def saveImpl(
        instance: Union[Pipeline, PipelineModel],
        stages: List[PipelineStage],
        sc: SparkContext,
        path: str,
    ) -> None: ...
    @staticmethod
    def load(
        metadata: Dict[str, Any], sc: SparkContext, path: str
    ) -> Tuple[str, List[PipelineStage]]: ...
    @staticmethod
    def getStagePath(stageUid: str, stageIdx: int, numStages: int, stagesDir: str) -> str: ...
