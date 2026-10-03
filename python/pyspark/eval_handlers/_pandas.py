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

pandas and pyarrow are imported lazily (inside ``__init__``/``run`` and the type-checking
block) so the module stays importable and its handlers register without either installed.
Each handler calls ``require_minimum_pandas_version`` and
``require_minimum_pyarrow_version`` in ``__init__`` so a missing or too-old
dependency surfaces a clear error when the handler runs.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.eval_handlers._base import BatchEvalTypeHandler
from pyspark.eval_handlers.verification import (
    verify_iter_result_row_count,
    verify_iterator_exhausted,
    verify_output_row_limit,
    verify_pandas_result,
    verify_result_row_count,
    verify_return_type,
)
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
        for input_batch in data:
            num_rows = input_batch.num_rows

            # Input: Arrow -> pandas Series (struct columns become DataFrames).
            pandas_columns = ArrowToPandasConversion.to_pandas(
                input_batch,
                timezone=runner_conf.timezone,
                struct_in_pandas="dict",
                ndarray_as_list=False,
                prefer_int_ext_dtype=runner_conf.prefer_int_ext_dtype,
                df_for_struct=True,
            )

            # Process: evaluate each UDF column-wise on pandas Series.
            results = []
            for udf_func, args_offsets, kwargs_offsets, return_type in self._udfs:
                result = udf_func(
                    *[pandas_columns[o] for o in args_offsets],
                    **{k: pandas_columns[v] for k, v in kwargs_offsets.items()},
                )
                if not hasattr(result, "__len__"):
                    pd_type = (
                        "pandas.DataFrame"
                        if isinstance(return_type, StructType)
                        else "pandas.Series"
                    )
                    raise PySparkTypeError(
                        errorClass="UDF_RETURN_TYPE",
                        messageParameters={"expected": pd_type, "actual": type(result).__name__},
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
            yield PandasToArrowConversion.from_pandas(
                results,
                self._return_schema,
                timezone=runner_conf.timezone,
                safecheck=runner_conf.safecheck,
                arrow_cast=True,
                prefers_large_types=runner_conf.use_large_var_types,
                assign_cols_by_name=runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=runner_conf.int_to_decimal_coercion_enabled,
            )


class PandasScalarIterUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_SCALAR_PANDAS_ITER_UDF: the single UDF receives an iterator of its
    argument columns (each a pandas Series, struct columns as DataFrames) and yields
    an iterator of pandas Series/DataFrames; enforce the declared element type, verify
    the total row count matches the input, and convert each result back to one
    RecordBatch. The pandas counterpart of ArrowScalarIterUDFHandler, calling the
    Arrow<->pandas conversions in ``pyspark.sql.conversion`` directly."""

    eval_type = PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        import pandas as pd

        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        assert len(udfs) == 1, "One SCALAR_PANDAS_ITER UDF expected here."
        self._udf_func, self._args_offsets, _, self._return_type = udfs[0]
        self._return_schema = StructType([StructField("_0", self._return_type)])
        self._expected_iter_type = (
            Iterator[pd.DataFrame]
            if isinstance(self._return_type, StructType)
            else Iterator[pd.Series]
        )

    def run(self, split_index: int, data: "Iterator[pa.RecordBatch]") -> "Iterator[pa.RecordBatch]":
        runner_conf = self._runner_conf
        args_offsets = self._args_offsets
        num_input_rows = 0

        def extract_args(batch: "pa.RecordBatch") -> Any:
            nonlocal num_input_rows
            # Input: Arrow -> pandas Series (struct columns become DataFrames).
            pandas_columns = ArrowToPandasConversion.to_pandas(
                batch,
                timezone=runner_conf.timezone,
                struct_in_pandas="dict",
                ndarray_as_list=False,
                prefer_int_ext_dtype=runner_conf.prefer_int_ext_dtype,
                df_for_struct=True,
            )
            args = tuple(pandas_columns[o] for o in args_offsets)
            num_input_rows += batch.num_rows
            return args[0] if len(args) == 1 else args

        # Extract args from input batches (streaming), then call the UDF and verify the
        # result type (iterator of pd.Series / pd.DataFrame).
        args_iter = map(extract_args, data)
        verified_iter = verify_return_type(self._udf_func(args_iter), self._expected_iter_type)

        def process_results() -> "Iterator[pa.RecordBatch]":
            for result in verified_iter:
                verify_pandas_result(
                    result,
                    self._return_type,
                    assign_cols_by_name=True,
                    truncate_return_schema=True,
                )
                yield PandasToArrowConversion.from_pandas(
                    [result],
                    self._return_schema,
                    timezone=runner_conf.timezone,
                    safecheck=runner_conf.safecheck,
                    arrow_cast=True,
                    prefers_large_types=runner_conf.use_large_var_types,
                    assign_cols_by_name=runner_conf.assign_cols_by_name,
                    int_to_decimal_coercion_enabled=runner_conf.int_to_decimal_coercion_enabled,
                )

        # Row-limit check (fail-fast) then exact row-count match (final).
        limited = verify_output_row_limit(process_results(), lambda: num_input_rows)
        yield from verify_iter_result_row_count(limited, lambda: num_input_rows)

        # Verify the input iterator was fully consumed.
        verify_iterator_exhausted(args_iter)


class PandasMapUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_MAP_PANDAS_ITER_UDF (mapInPandas): the single UDF receives the input
    RecordBatch stream as an iterator of pandas DataFrames (one per batch, the single
    struct column expanded) and yields pandas DataFrames/Series, converted back to one
    RecordBatch each. The pandas counterpart of ArrowMapUDFHandler."""

    eval_type = PythonEvalType.SQL_MAP_PANDAS_ITER_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        import pandas as pd

        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        assert len(udfs) == 1, "One MAP_PANDAS_ITER UDF expected here."
        self._udf_func, _, _, self._return_type = udfs[0]
        self._return_schema = StructType([StructField("_0", self._return_type)])
        self._expected_iter_type = (
            Iterator[pd.DataFrame]
            if isinstance(self._return_type, StructType)
            else Iterator[pd.Series]
        )

    def run(self, split_index: int, data: "Iterator[pa.RecordBatch]") -> "Iterator[pa.RecordBatch]":
        runner_conf = self._runner_conf

        def dataframe_iter() -> "Iterator[Any]":
            # Input batches have a single struct column (see MapInBatchEvaluatorFactory);
            # convert lazily so peakmem stays bounded by one batch.
            for batch in data:
                yield ArrowToPandasConversion.to_pandas(
                    batch,
                    timezone=runner_conf.timezone,
                    prefer_int_ext_dtype=runner_conf.prefer_int_ext_dtype,
                    df_for_struct=True,
                )[0]

        result = self._udf_func(dataframe_iter())
        # The declared signature is Iterator[...], so a strict iterator is required by
        # default. With the legacy flag, accept any object Python can iterate over -- via
        # iter(...), which honors both __iter__ and the sequence protocol (__getitem__) --
        # by adapting it into an iterator before the shared element-type verification.
        if runner_conf.map_in_batch_legacy_accept_any_iterable and not isinstance(result, Iterator):
            try:
                result = iter(result)
            except TypeError:
                # Not iterable at all; leave it so verify_return_type below raises the
                # standard UDF_RETURN_TYPE error.
                pass

        for df in verify_return_type(result, self._expected_iter_type):
            verify_pandas_result(
                df, self._return_type, assign_cols_by_name=True, truncate_return_schema=True
            )
            yield PandasToArrowConversion.from_pandas(
                [df],
                self._return_schema,
                timezone=runner_conf.timezone,
                safecheck=runner_conf.safecheck,
                arrow_cast=True,
                prefers_large_types=runner_conf.use_large_var_types,
                assign_cols_by_name=runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=runner_conf.int_to_decimal_coercion_enabled,
            )
