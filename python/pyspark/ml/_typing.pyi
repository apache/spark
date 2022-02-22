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

from typing import Any, Dict, List, TypeVar, Tuple, Union
from typing_extensions import Literal

from numpy import ndarray

import pyspark.ml.base
import pyspark.ml.param
import pyspark.ml.util
from pyspark.ml.linalg import Vector
import pyspark.ml.wrapper
from py4j.java_gateway import JavaObject

ParamMap = Dict[pyspark.ml.param.Param, Any]
PipelineStage = Union[pyspark.ml.base.Estimator, pyspark.ml.base.Transformer]

T = TypeVar("T")
P = TypeVar("P", bound=pyspark.ml.param.Params)
M = TypeVar("M", bound=pyspark.ml.base.Transformer)
JM = TypeVar("JM", bound=pyspark.ml.wrapper.JavaTransformer)
C = TypeVar("C", bound=type)

JavaObjectOrPickleDump = Union[JavaObject, bytearray, bytes]
BinaryClassificationEvaluatorMetricType = Union[Literal["areaUnderROC"], Literal["areaUnderPR"]]
RegressionEvaluatorMetricType = Union[
    Literal["rmse"], Literal["mse"], Literal["r2"], Literal["mae"], Literal["var"]
]
MulticlassClassificationEvaluatorMetricType = Union[
    Literal["f1"],
    Literal["accuracy"],
    Literal["weightedPrecision"],
    Literal["weightedRecall"],
    Literal["weightedTruePositiveRate"],
    Literal["weightedFalsePositiveRate"],
    Literal["weightedFMeasure"],
    Literal["truePositiveRateByLabel"],
    Literal["falsePositiveRateByLabel"],
    Literal["precisionByLabel"],
    Literal["recallByLabel"],
    Literal["fMeasureByLabel"],
]
MultilabelClassificationEvaluatorMetricType = Union[
    Literal["subsetAccuracy"],
    Literal["accuracy"],
    Literal["hammingLoss"],
    Literal["precision"],
    Literal["recall"],
    Literal["f1Measure"],
    Literal["precisionByLabel"],
    Literal["recallByLabel"],
    Literal["f1MeasureByLabel"],
    Literal["microPrecision"],
    Literal["microRecall"],
    Literal["microF1Measure"],
]
ClusteringEvaluatorMetricType = Literal["silhouette"]
ClusteringEvaluatorDistanceMeasureType = Union[Literal["squaredEuclidean"], Literal["cosine"]]

RankingEvaluatorMetricType = Union[
    Literal["meanAveragePrecision"],
    Literal["meanAveragePrecisionAtK"],
    Literal["precisionAtK"],
    Literal["ndcgAtK"],
    Literal["recallAtK"],
]

VectorLike = Union[ndarray, Vector, List[float], Tuple[float, ...]]
