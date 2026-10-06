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

"""Handlers for the pandas UDF eval types (the UDF exchanges ``pd.Series`` /
``pd.DataFrame`` values, converted to and from Arrow via the conversion utilities
in ``pyspark.sql.conversion``, the pandas counterpart of ``ArrowBatchTransformer``).

pandas and pyarrow are imported lazily (inside ``run`` and the type-checking block)
so the module stays importable and its handlers register without either installed.
Each handler calls ``require_minimum_pandas_version`` and
``require_minimum_pyarrow_version`` in ``__init__`` so a missing or too-old
dependency surfaces a clear error when the handler runs.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.eval_handlers._base import BatchEvalTypeHandler
from pyspark.eval_handlers.verification import verify_result_row_count
from pyspark.sql.conversion import ArrowToPandasConversion, PandasToArrowConversion
from pyspark.sql.pandas.utils import (
    require_minimum_pandas_version,
    require_minimum_pyarrow_version,
)
from pyspark.sql.types import StructField, StructType
from pyspark.util import PythonEvalType

if TYPE_CHECKING:
    import pyarrow as pa

    from pyspark.worker_util import EvalConf, RunnerConf


class PandasScalarUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_SCALAR_PANDAS_UDF: convert each input RecordBatch to pandas Series
    (struct columns become DataFrames), invoke each UDF once per batch, check the
    row count, and convert the pandas results back to one RecordBatch. The pandas
    counterpart of ArrowScalarUDFHandler, calling the Arrow<->pandas conversions in
    ``pyspark.sql.conversion`` directly with the runner_conf-derived parameters."""

    eval_type = PythonEvalType.SQL_SCALAR_PANDAS_UDF
    supports_phase_timing = True

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        self._return_schema = StructType(
            [StructField("_%d" % i, rt) for i, (_, _, _, rt) in enumerate(udfs)]
        )

    def run(self, split_index: int, data: "Iterator[pa.RecordBatch]") -> "Iterator[pa.RecordBatch]":
        import pandas as pd

        runner_conf = self._runner_conf
        input_timer = self._input_preparation_timer
        udf_timer = self._udf_execution_timer
        output_timer = self._output_preparation_timer
        assert input_timer is not None and udf_timer is not None and output_timer is not None
        for input_batch in data:
            # Input: Arrow -> pandas Series (struct columns become DataFrames).
            with input_timer:
                num_rows = input_batch.num_rows
                results = []
                pandas_columns = ArrowToPandasConversion.to_pandas(
                    input_batch,
                    timezone=runner_conf.timezone,
                    struct_in_pandas="dict",
                    ndarray_as_list=False,
                    prefer_int_ext_dtype=runner_conf.prefer_int_ext_dtype,
                    df_for_struct=True,
                )

            # Process: evaluate each UDF column-wise on pandas Series.
            for udf_func, args_offsets, kwargs_offsets, return_type in self._udfs:
                with input_timer:
                    args = [pandas_columns[o] for o in args_offsets]
                    kwargs = {k: pandas_columns[v] for k, v in kwargs_offsets.items()}
                # Include profiler wrapper work when profiling is enabled.
                with udf_timer:
                    result = udf_func(*args, **kwargs)
                with output_timer:
                    if not hasattr(result, "__len__"):
                        pd_type = (
                            "pandas.DataFrame"
                            if isinstance(return_type, StructType)
                            else "pandas.Series"
                        )
                        raise PySparkTypeError(
                            errorClass="UDF_RETURN_TYPE",
                            messageParameters={
                                "expected": pd_type,
                                "actual": type(result).__name__,
                            },
                        )
                    verify_result_row_count(len(result), num_rows)
                    # struct_in_pandas="dict": UDF must return a DataFrame for struct types.
                    if isinstance(return_type, StructType) and not isinstance(result, pd.DataFrame):
                        raise PySparkValueError(
                            "Invalid return type. Please make sure that the UDF returns a "
                            "pandas.DataFrame when the specified return type is StructType."
                        )
                    results.append(result)

            # Output: pandas -> Arrow.
            with output_timer:
                output_batch = PandasToArrowConversion.from_pandas(
                    results,
                    self._return_schema,
                    timezone=runner_conf.timezone,
                    safecheck=runner_conf.safecheck,
                    arrow_cast=True,
                    prefers_large_types=runner_conf.use_large_var_types,
                    assign_cols_by_name=runner_conf.assign_cols_by_name,
                    int_to_decimal_coercion_enabled=runner_conf.int_to_decimal_coercion_enabled,
                )
                self.metrics.increment("pythonNumTimedBatches")
            # End timing before yielding, since the consumer may pause between batches.
            yield output_batch
