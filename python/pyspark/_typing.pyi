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

from typing import Callable, Iterable, Sized, TypeVar, Union
from typing_extensions import Literal, Protocol

from numpy import int32, int64, float32, float64, ndarray

F = TypeVar("F", bound=Callable)
T_co = TypeVar("T_co", covariant=True)

PrimitiveType = Union[bool, float, int, str]

NonUDFType = Literal[0]

class SupportsIAdd(Protocol):
    def __iadd__(self, other: SupportsIAdd) -> SupportsIAdd: ...

class SupportsOrdering(Protocol):
    def __lt__(self, other: SupportsOrdering) -> bool: ...

class SizedIterable(Protocol, Sized, Iterable[T_co]): ...

S = TypeVar("S", bound=SupportsOrdering)

NumberOrArray = TypeVar(
    "NumberOrArray", float, int, complex, int32, int64, float32, float64, ndarray
)
