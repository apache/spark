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

# Stubs for pyspark.ml.wrapper (Python 3)

import abc
from typing import Any, Generic, Optional, Type, TypeVar
from pyspark.ml._typing import P, T, JM, ParamMap

from pyspark.ml import Estimator, Predictor, PredictionModel, Transformer, Model
from pyspark.ml.base import _PredictorParams
from pyspark.ml.param import Param, Params
from pyspark.ml.param.shared import HasFeaturesCol, HasLabelCol, HasPredictionCol

class JavaWrapper:
    def __init__(self, java_obj: Optional[Any] = ...) -> None: ...
    def __del__(self) -> None: ...

class JavaParams(JavaWrapper, Params):
    __metaclass__: Type[abc.ABCMeta]
    def copy(self: P, extra: Optional[ParamMap] = ...) -> P: ...
    def clear(self, param: Param) -> None: ...

class JavaEstimator(JavaParams, Estimator[JM], metaclass=abc.ABCMeta):
    __metaclass__: Type[abc.ABCMeta]

class JavaTransformer(JavaParams, Transformer):
    __metaclass__: Type[abc.ABCMeta]

class JavaModel(JavaTransformer, Model):
    __metaclass__: Type[abc.ABCMeta]
    def __init__(self, java_model: Optional[Any] = ...) -> None: ...

class _JavaPredictorParams(HasLabelCol, HasFeaturesCol, HasPredictionCol): ...

class JavaPredictor(
    Predictor[JM], JavaEstimator, _JavaPredictorParams, metaclass=abc.ABCMeta
):
    __metaclass__: Type[abc.ABCMeta]

class JavaPredictionModel(PredictionModel[T], JavaModel, _PredictorParams):
    @property
    def numFeatures(self) -> int: ...
    def predict(self, value: T) -> float: ...
