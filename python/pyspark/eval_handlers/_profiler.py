#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Wrappers that profile a UDF's calls into the SQL UDF profiler accumulators."""

from typing import Any, Callable

from pyspark.accumulators import SpecialAccumulatorIds, _deserialize_accumulator


def wrap_perf_profiler(f: Callable, iter_based: bool, result_id: int) -> Callable:
    from pyspark.sql.profiler import ProfileResultsParam, ProfileResultsParamV2, WorkerPerfProfiler

    accumulator = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
    )
    accumulator_v2 = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER_V2, {}, ProfileResultsParamV2
    )

    if iter_based:

        def profiling_func(*args: Any, **kwargs: Any) -> Any:
            iterator = iter(f(*args, **kwargs))
            while True:
                try:
                    with WorkerPerfProfiler(accumulator, accumulator_v2, result_id):
                        item = next(iterator)
                    yield item
                except StopIteration:
                    break

    else:

        def profiling_func(*args: Any, **kwargs: Any) -> Any:
            with WorkerPerfProfiler(accumulator, accumulator_v2, result_id):
                ret = f(*args, **kwargs)
            return ret

    return profiling_func


def wrap_memory_profiler(f: Callable, iter_based: bool, result_id: int) -> Callable:
    import pyspark.memory_profiler_ext
    from pyspark.sql.profiler import (
        ProfileResultsParam,
        ProfileResultsParamV2,
        WorkerMemoryProfiler,
    )

    if not pyspark.memory_profiler_ext.has_memory_profiler:
        return f

    accumulator = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
    )

    accumulator_v2 = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER_V2, {}, ProfileResultsParamV2
    )

    if iter_based:

        def profiling_func(*args: Any, **kwargs: Any) -> Any:
            g = f(*args, **kwargs)
            iterator = iter(g)

            while True:
                try:
                    with WorkerMemoryProfiler(accumulator, accumulator_v2, result_id, g.gi_code):
                        item = next(iterator)
                    yield item
                except StopIteration:
                    break

    else:

        def profiling_func(*args: Any, **kwargs: Any) -> Any:
            with WorkerMemoryProfiler(accumulator, accumulator_v2, result_id, f):
                ret = f(*args, **kwargs)
            return ret

    return profiling_func
