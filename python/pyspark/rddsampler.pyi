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

from typing import Any, Dict, Iterator, Optional, Tuple, TypeVar

T = TypeVar("T")
U = TypeVar("U")
K = TypeVar("K")
V = TypeVar("V")

class RDDSamplerBase:
    def __init__(self, withReplacement: bool, seed: Optional[int] = ...) -> None: ...
    def initRandomGenerator(self, split: int) -> None: ...
    def getUniformSample(self) -> float: ...
    def getPoissonSample(self, mean: float) -> int: ...
    def func(self, split: int, iterator: Iterator[Any]) -> Iterator[Any]: ...

class RDDSampler(RDDSamplerBase):
    def __init__(
        self, withReplacement: bool, fraction: float, seed: Optional[int] = ...
    ) -> None: ...
    def func(self, split: int, iterator: Iterator[T]) -> Iterator[T]: ...

class RDDRangeSampler(RDDSamplerBase):
    def __init__(
        self, lowerBound: T, upperBound: T, seed: Optional[Any] = ...
    ) -> None: ...
    def func(self, split: int, iterator: Iterator[T]) -> Iterator[T]: ...

class RDDStratifiedSampler(RDDSamplerBase):
    def __init__(
        self,
        withReplacement: bool,
        fractions: Dict[K, float],
        seed: Optional[int] = ...,
    ) -> None: ...
    def func(
        self, split: int, iterator: Iterator[Tuple[K, V]]
    ) -> Iterator[Tuple[K, V]]: ...
