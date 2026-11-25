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

"""
What the Python workers run for the RDD API on Spark Connect, besides the functions of the RDDs.

The commands of the PythonRDDs that the client sends reference this module, so the Python workers
import it. It must not import the Spark Connect client, whose dependencies the Python workers of a
cluster may lack.
"""

import os
from typing import Any, Callable

__all__ = ["RDDProfiler"]

# The profiler that the server enables for the Python workers of an RDD, from the
# `spark.sql.pyspark.udf.profiler` configuration of the session.
PROFILER_ENV = "PYSPARK_CONNECT_RDD_PROFILER"
SAMPLING_INTERVAL_ENV = "PYSPARK_CONNECT_RDD_PROFILER_SAMPLING_INTERVAL"
SAMPLING_AGGREGATION_ENV = "PYSPARK_CONNECT_RDD_PROFILER_SAMPLING_AGGREGATION"


class RDDProfiler:
    """
    The profiler of the command of a PythonRDD (see ``pyspark.worker``). When the server enables
    profiling for the session, it profiles the computation of each partition as the profiler of
    Python UDFs does, and reports the results under ``key`` to ``SparkSession.profile`` of the
    client, which ``SparkContext.show_profiles`` shows.

    Spark Classic profiles RDDs with ``spark.python.profile`` and the profilers of its
    SparkContext, which the client of Spark Connect does not have.
    """

    def __init__(self, key: str) -> None:
        self.key = key

    def profile(self, func: Callable[[], Any]) -> None:
        kind = os.environ.get(PROFILER_ENV)
        if kind == "perf":
            from pyspark.accumulators import SpecialAccumulatorIds, _deserialize_accumulator
            from pyspark.sql.profiler import (
                ProfileResultsParam,
                ProfileResultsParamV2,
                WorkerPerfProfiler,
            )

            accumulator = _deserialize_accumulator(
                SpecialAccumulatorIds.SQL_UDF_PROFIER, {}, ProfileResultsParam
            )
            accumulator_v2 = _deserialize_accumulator(
                SpecialAccumulatorIds.SQL_UDF_PROFIER_V2, {}, ProfileResultsParamV2
            )
            with WorkerPerfProfiler(accumulator, accumulator_v2, self.key):
                func()
        elif kind == "sampling":
            from pyspark.accumulators import SpecialAccumulatorIds, _deserialize_accumulator
            from pyspark.sql.profiler import ProfileResultsParamV2, WorkerSamplingProfiler
            from pyspark.taskcontext import TaskContext

            accumulator_v2 = _deserialize_accumulator(
                SpecialAccumulatorIds.SQL_UDF_PROFIER_V2, {}, ProfileResultsParamV2
            )
            task_context = TaskContext.get()
            assert task_context is not None
            with WorkerSamplingProfiler(
                task_context=task_context,
                sampling_interval=float(os.environ.get(SAMPLING_INTERVAL_ENV, "100")),
                aggregation=os.environ.get(SAMPLING_AGGREGATION_ENV, "stage"),
            ) as profiler:
                func()
            profiler.save(accumulator_v2)
        else:
            func()

    def __repr__(self) -> str:
        return "RDDProfiler(%r)" % self.key
