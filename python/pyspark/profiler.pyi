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

from typing import Any, Callable, List, Optional, Tuple, Type

import pstats

from pyspark.accumulators import AccumulatorParam
from pyspark.context import SparkContext

class ProfilerCollector:
    profiler_cls: Type[Profiler]
    profile_dump_path: Optional[str]
    profilers: List[Tuple[int, Profiler, bool]]
    def __init__(
        self, profiler_cls: Type[Profiler], dump_path: Optional[str] = ...
    ) -> None: ...
    def new_profiler(self, ctx: SparkContext) -> Profiler: ...
    def add_profiler(self, id: int, profiler: Profiler) -> None: ...
    def dump_profiles(self, path: str) -> None: ...
    def show_profiles(self) -> None: ...

class Profiler:
    def __init__(self, ctx: SparkContext) -> None: ...
    def profile(self, func: Callable[[], Any]) -> None: ...
    def stats(self) -> pstats.Stats: ...
    def show(self, id: int) -> None: ...
    def dump(self, id: int, path: str) -> None: ...

class PStatsParam(AccumulatorParam):
    @staticmethod
    def zero(value: pstats.Stats) -> None: ...
    @staticmethod
    def addInPlace(
        value1: Optional[pstats.Stats], value2: Optional[pstats.Stats]
    ) -> Optional[pstats.Stats]: ...

class BasicProfiler(Profiler):
    def __init__(self, ctx: SparkContext) -> None: ...
    def profile(self, func: Callable[[], Any]) -> None: ...
    def stats(self) -> pstats.Stats: ...
