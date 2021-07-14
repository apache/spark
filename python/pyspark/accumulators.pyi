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

from typing import Callable, Dict, Generic, Tuple, Type, TypeVar

import socketserver.BaseRequestHandler  # type: ignore

from pyspark._typing import SupportsIAdd

T = TypeVar("T")
U = TypeVar("U", bound=SupportsIAdd)

import socketserver as SocketServer

_accumulatorRegistry: Dict[int, Accumulator]

class Accumulator(Generic[T]):
    aid: int
    accum_param: AccumulatorParam[T]
    def __init__(
        self, aid: int, value: T, accum_param: AccumulatorParam[T]
    ) -> None: ...
    def __reduce__(
        self,
    ) -> Tuple[
        Callable[[int, int, AccumulatorParam[T]], Accumulator[T]],
        Tuple[int, int, AccumulatorParam[T]],
    ]: ...
    @property
    def value(self) -> T: ...
    @value.setter
    def value(self, value: T) -> None: ...
    def add(self, term: T) -> None: ...
    def __iadd__(self, term: T) -> Accumulator[T]: ...

class AccumulatorParam(Generic[T]):
    def zero(self, value: T) -> T: ...
    def addInPlace(self, value1: T, value2: T) -> T: ...

class AddingAccumulatorParam(AccumulatorParam[U]):
    zero_value: U
    def __init__(self, zero_value: U) -> None: ...
    def zero(self, value: U) -> U: ...
    def addInPlace(self, value1: U, value2: U) -> U: ...

class _UpdateRequestHandler(SocketServer.StreamRequestHandler):
    def handle(self) -> None: ...

class AccumulatorServer(SocketServer.TCPServer):
    auth_token: str
    def __init__(
        self,
        server_address: Tuple[str, int],
        RequestHandlerClass: Type[socketserver.BaseRequestHandler],
        auth_token: str,
    ) -> None: ...
    server_shutdown: bool
    def shutdown(self) -> None: ...
