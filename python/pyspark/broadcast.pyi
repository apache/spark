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

import threading
from typing import Any, Callable, Dict, Generic, Optional, Tuple, TypeVar

T = TypeVar("T")

_broadcastRegistry: Dict[int, Broadcast]

class Broadcast(Generic[T]):
    def __init__(
        self,
        sc: Optional[Any] = ...,
        value: Optional[T] = ...,
        pickle_registry: Optional[Any] = ...,
        path: Optional[Any] = ...,
        sock_file: Optional[Any] = ...,
    ) -> None: ...
    def dump(self, value: T, f: Any) -> None: ...
    def load_from_path(self, path: Any) -> T: ...
    def load(self, file: Any) -> T: ...
    @property
    def value(self) -> T: ...
    def unpersist(self, blocking: bool = ...) -> None: ...
    def destroy(self, blocking: bool = ...) -> None: ...
    def __reduce__(self) -> Tuple[Callable[[int], T], Tuple[int]]: ...

class BroadcastPickleRegistry(threading.local):
    def __init__(self) -> None: ...
    def __iter__(self) -> None: ...
    def add(self, bcast: Any) -> None: ...
    def clear(self) -> None: ...
