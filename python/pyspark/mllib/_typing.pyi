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

from typing import List, Tuple, TypeVar, Union

from typing_extensions import Literal

from pyspark.mllib.linalg import Vector
from numpy import ndarray  # noqa: F401
from py4j.java_gateway import JavaObject

VectorLike = Union[ndarray, Vector, List[float], Tuple[float, ...]]
C = TypeVar("C", bound=type)
JavaObjectOrPickleDump = Union[JavaObject, bytearray, bytes]

CorrelationMethod = Union[Literal["spearman"], Literal["pearson"]]
DistName = Literal["norm"]
NormType = Union[None, float, Literal["fro"], Literal["nuc"]]
