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

import inspect
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from pyspark import _NoValue
from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.eval_handlers._base import (
    BatchEvalTypeHandler,
    CoGroupedEvalTypeHandler,
    GroupedEvalTypeHandler,
)
from pyspark.eval_handlers.utils import extract_key_value_indexes, wrap_kwargs_support
from pyspark.eval_handlers.verification import (
    verify_iter_result_row_count,
    verify_iterator_exhausted,
    verify_output_row_limit,
    verify_pandas_result,
    verify_result_row_count,
    verify_return_type,
)
from pyspark.sql.conversion import (
    ArrowBatchTransformer,
    ArrowToPandasConversion,
    PandasToArrowConversion,
)
from pyspark.sql.pandas.utils import (
    require_minimum_pandas_version,
    require_minimum_pyarrow_version,
)
from pyspark.sql.types import BinaryType, StringType, StructField, StructType
from pyspark.util import PythonEvalType, fail_on_stopiteration
from pyspark.worker_metrics import WorkerMetrics

if TYPE_CHECKING:
    import pyarrow as pa

    from pyspark.eval_handlers._typing import UDF, CoGroupedBatch, GroupedBatch
    from pyspark.worker_util import EvalConf, RunnerConf


class PandasScalarUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_SCALAR_PANDAS_UDF: convert each input RecordBatch to pandas Series
    (struct columns become DataFrames), invoke each UDF once per batch, check the
    row count, and convert the pandas results back to one RecordBatch. The pandas
    counterpart of ArrowScalarUDFHandler, calling the Arrow<->pandas conversions in
    ``pyspark.sql.conversion`` directly with the runner_conf-derived parameters."""

    eval_type = PythonEvalType.SQL_SCALAR_PANDAS_UDF
    _multi_udf = True

    def __init__(
        self, udf_descs: list[UDF], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udf_descs, runner_conf, eval_conf)
        self._return_schema = StructType(
            [StructField("_%d" % i, udf.return_type) for i, udf in enumerate(udf_descs)]
        )

    def run(self, split_index: int, data: "Iterator[pa.RecordBatch]") -> "Iterator[pa.RecordBatch]":
        import pandas as pd

        for input_batch in data:
            num_rows = input_batch.num_rows

            # Input: Arrow -> pandas Series (struct columns become DataFrames).
            pandas_columns = ArrowToPandasConversion.to_pandas(
                input_batch,
                timezone=self._runner_conf.timezone,
                struct_in_pandas="dict",
                ndarray_as_list=False,
                prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
                df_for_struct=True,
            )

            # Process: evaluate each UDF column-wise on pandas Series.
            results = []
            for udf, func in zip(self._udf_descs, self._funcs):
                return_type = udf.return_type
                result = func(
                    *[pandas_columns[o] for o in udf.args_offsets],
                    **{k: pandas_columns[v] for k, v in udf.kwargs_offsets.items()},
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
                timezone=self._runner_conf.timezone,
                safecheck=self._runner_conf.safecheck,
                arrow_cast=True,
                prefers_large_types=self._runner_conf.use_large_var_types,
                assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=self._runner_conf.int_to_decimal_coercion_enabled,
            )


class PandasScalarIterUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_SCALAR_PANDAS_ITER_UDF: the single UDF receives an iterator of its
    argument columns (each a pandas Series, struct columns as DataFrames) and yields
    an iterator of pandas Series/DataFrames; enforce the declared element type, verify
    the total row count matches the input, and convert each result back to one
    RecordBatch. The pandas counterpart of ArrowScalarIterUDFHandler, calling the
    Arrow<->pandas conversions in ``pyspark.sql.conversion`` directly."""

    eval_type = PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
    _iter_based = True

    def __init__(
        self, udf_descs: list[UDF], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        import pandas as pd

        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udf_descs, runner_conf, eval_conf)
        self._return_schema = StructType([StructField("_0", self._udf_desc.return_type)])
        self._expected_iter_type = (
            Iterator[pd.DataFrame]
            if isinstance(self._udf_desc.return_type, StructType)
            else Iterator[pd.Series]
        )

    def run(self, split_index: int, data: "Iterator[pa.RecordBatch]") -> "Iterator[pa.RecordBatch]":
        num_input_rows = 0

        def extract_args(batch: "pa.RecordBatch") -> Any:
            nonlocal num_input_rows
            # Input: Arrow -> pandas Series (struct columns become DataFrames).
            pandas_columns = ArrowToPandasConversion.to_pandas(
                batch,
                timezone=self._runner_conf.timezone,
                struct_in_pandas="dict",
                ndarray_as_list=False,
                prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
                df_for_struct=True,
            )
            args = tuple(pandas_columns[o] for o in self._udf_desc.args_offsets)
            num_input_rows += batch.num_rows
            return args[0] if len(args) == 1 else args

        # Extract args from input batches (streaming), then call the UDF and verify the
        # result type (iterator of pd.Series / pd.DataFrame).
        args_iter = map(extract_args, data)
        verified_iter = verify_return_type(self._func(args_iter), self._expected_iter_type)

        def convert(result: Any) -> "pa.RecordBatch":
            verify_pandas_result(
                result,
                self._udf_desc.return_type,
                assign_cols_by_name=True,
                truncate_return_schema=True,
            )
            return PandasToArrowConversion.from_pandas(
                [result],
                self._return_schema,
                timezone=self._runner_conf.timezone,
                safecheck=self._runner_conf.safecheck,
                arrow_cast=True,
                prefers_large_types=self._runner_conf.use_large_var_types,
                assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=self._runner_conf.int_to_decimal_coercion_enabled,
            )

        # Row-limit check (fail-fast) then exact row-count match (final).
        batches = verify_output_row_limit(map(convert, verified_iter), lambda: num_input_rows)
        yield from verify_iter_result_row_count(batches, lambda: num_input_rows)

        # Verify the input iterator was fully consumed.
        verify_iterator_exhausted(args_iter)


class PandasMapUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_MAP_PANDAS_ITER_UDF (mapInPandas): the single UDF receives the input
    RecordBatch stream as an iterator of pandas DataFrames (one per batch, the single
    struct column expanded) and yields pandas DataFrames/Series, converted back to one
    RecordBatch each. The pandas counterpart of ArrowMapUDFHandler."""

    eval_type = PythonEvalType.SQL_MAP_PANDAS_ITER_UDF
    _iter_based = True

    def __init__(
        self, udf_descs: list[UDF], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        import pandas as pd

        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udf_descs, runner_conf, eval_conf)
        self._return_schema = StructType([StructField("_0", self._udf_desc.return_type)])
        self._expected_iter_type = (
            Iterator[pd.DataFrame]
            if isinstance(self._udf_desc.return_type, StructType)
            else Iterator[pd.Series]
        )

    def run(self, split_index: int, data: "Iterator[pa.RecordBatch]") -> "Iterator[pa.RecordBatch]":
        metrics = WorkerMetrics()
        phase_timers = self._create_phase_timers()
        if phase_timers is not None:
            # If the UDF never pulls from its lazy input iterator, the serializer's load_stream()
            # may never start, leaving the read timer unregistered. Register a zero for that case.
            metrics.measure("pythonDataReadTime")

            # Sequential input intervals include reads. Subtract them at report time because
            # the serializer may publish its read total after the handler finishes.
            metrics.subtract_duration_at_report(
                total_metric="pythonInputPreparationTime", nested_metric="pythonDataReadTime"
            )

        clock = time.perf_counter_ns
        input_duration_ns = 0
        udf_duration_ns = 0
        output_duration_ns = 0
        num_timed_batches = 0

        def dataframe_iter() -> "Iterator[Any]":
            nonlocal input_duration_ns, num_timed_batches
            # Input batches have a single struct column (see MapInBatchEvaluatorFactory);
            # convert lazily so peakmem stays bounded by one batch.
            while True:
                if phase_timers is not None:
                    input_start_ns = clock()
                try:
                    batch = next(data, None)
                    if batch is None:
                        return
                    if phase_timers is not None:
                        num_timed_batches += 1
                    df = ArrowToPandasConversion.to_pandas(
                        batch,
                        timezone=self._runner_conf.timezone,
                        prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
                        df_for_struct=True,
                    )[0]
                finally:
                    if phase_timers is not None:
                        input_duration_ns += clock() - input_start_ns
                yield df
                del df

        if phase_timers is not None:
            udf_start_ns = clock()
        try:
            result = self._func(dataframe_iter())
            # The declared signature is Iterator[...], so a strict iterator is required by
            # default. With the legacy flag, accept any object Python can iterate over -- via
            # iter(...), which honors both __iter__ and the sequence protocol (__getitem__) --
            # by adapting it into an iterator before the shared element-type verification.
            if self._runner_conf.map_in_batch_legacy_accept_any_iterable and not isinstance(
                result, Iterator
            ):
                try:
                    result = iter(result)
                except TypeError:
                    # Not iterable at all; leave it so verify_return_type below raises the
                    # standard UDF_RETURN_TYPE error.
                    pass
            result = verify_return_type(result, self._expected_iter_type)
            if phase_timers is not None:
                udf_duration_ns += clock() - udf_start_ns

            while True:
                if phase_timers is not None:
                    udf_start_ns = clock()
                df = next(result, None)
                if phase_timers is not None:
                    udf_duration_ns += clock() - udf_start_ns
                if df is None:
                    break

                if phase_timers is not None:
                    output_start_ns = clock()
                verify_pandas_result(
                    df,
                    self._udf_desc.return_type,
                    assign_cols_by_name=True,
                    truncate_return_schema=True,
                )
                batch = PandasToArrowConversion.from_pandas(
                    [df],
                    self._return_schema,
                    timezone=self._runner_conf.timezone,
                    safecheck=self._runner_conf.safecheck,
                    arrow_cast=True,
                    prefers_large_types=self._runner_conf.use_large_var_types,
                    assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                    int_to_decimal_coercion_enabled=(
                        self._runner_conf.int_to_decimal_coercion_enabled
                    ),
                )
                if phase_timers is not None:
                    output_duration_ns += clock() - output_start_ns
                yield batch
                del batch
        finally:
            if phase_timers is not None:
                metrics.set("pythonNumTimedBatches", num_timed_batches)
                input_timer, udf_timer, output_timer = phase_timers
                input_timer.add_duration_ns(input_duration_ns)
                # Input requests run inside UDF execution; subtract their full duration.
                udf_timer.add_duration_ns(udf_duration_ns - input_duration_ns)
                output_timer.add_duration_ns(output_duration_ns)


class PandasCoGroupedMapUDFHandler(CoGroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_COGROUPED_MAP_PANDAS_UDF (applyInPandas on a cogroup): the single UDF
    receives the two sides' value columns as pandas DataFrames (plus the grouping
    key when it takes three arguments) and returns one pandas DataFrame, converted
    back to one RecordBatch. The pandas counterpart of ArrowCoGroupedMapUDFHandler."""

    eval_type = PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF

    def __init__(
        self, udf_descs: list[UDF], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udf_descs, runner_conf, eval_conf)
        # signature was lost when wrapping it
        self._num_udf_args = len(inspect.getfullargspec(self._raw_func).args)
        parsed_offsets = extract_key_value_indexes(self._udf_desc.args_offsets)
        self._left_key_offsets, self._left_value_offsets = parsed_offsets[0]
        self._right_key_offsets, self._right_value_offsets = parsed_offsets[1]
        self._return_schema = StructType([StructField("_0", self._udf_desc.return_type)])

    def run(self, split_index: int, data: "Iterator[CoGroupedBatch]") -> "Iterator[pa.RecordBatch]":
        """Apply cogroupBy Pandas UDF.

        The explicit ``del`` calls keep peakmem bounded across groups: without them the
        previous group's tables and DataFrames stay bound on the generator frame while
        the next group is read.
        """
        import pandas as pd
        import pyarrow as pa

        for left_batches, right_batches in data:
            left_table = pa.Table.from_batches(left_batches)
            right_table = pa.Table.from_batches(right_batches)
            left_series = ArrowToPandasConversion.to_pandas(
                left_table,
                timezone=self._runner_conf.timezone,
                prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
            )
            right_series = ArrowToPandasConversion.to_pandas(
                right_table,
                timezone=self._runner_conf.timezone,
                prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
            )
            left_df = pd.concat([left_series[o] for o in self._left_value_offsets], axis=1)
            right_df = pd.concat([right_series[o] for o in self._right_value_offsets], axis=1)

            if self._num_udf_args == 2:
                result = self._func(left_df, right_df)
            else:
                key_series = (
                    [left_series[o] for o in self._left_key_offsets]
                    if not left_df.empty
                    else [right_series[o] for o in self._right_key_offsets]
                )
                key = tuple(s.iloc[0] for s in key_series)
                result = self._func(key, left_df, right_df)

            del left_batches, right_batches, left_table, right_table
            del left_series, right_series, left_df, right_df

            verify_pandas_result(
                result,
                self._udf_desc.return_type,
                self._runner_conf.assign_cols_by_name,
                truncate_return_schema=False,
            )

            yield PandasToArrowConversion.from_pandas(
                [result],
                self._return_schema,
                timezone=self._runner_conf.timezone,
                safecheck=self._runner_conf.safecheck,
                arrow_cast=True,
                prefers_large_types=self._runner_conf.use_large_var_types,
                assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=self._runner_conf.int_to_decimal_coercion_enabled,
            )
            del result


class PandasGroupedMapIterUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_MAP_PANDAS_ITER_UDF: the single UDF receives each group as an
    iterator of pandas DataFrames (one per input batch, converted lazily) and returns
    an iterator of pandas DataFrames, each converted back to one RecordBatch. The
    pandas counterpart of ArrowGroupedMapIterUDFHandler."""

    eval_type = PythonEvalType.SQL_GROUPED_MAP_PANDAS_ITER_UDF
    _iter_based = True

    def __init__(
        self, udf_descs: list[UDF], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udf_descs, runner_conf, eval_conf)
        # signature was lost when wrapping it
        self._num_udf_args = len(inspect.getfullargspec(self._raw_func).args)
        parsed_offsets = extract_key_value_indexes(self._udf_desc.args_offsets)
        assert len(parsed_offsets) == 1, (
            "Expected one pair of offsets for GROUPED_MAP_PANDAS_ITER UDF."
        )
        self._key_offsets, self._value_offsets = parsed_offsets[0]
        self._return_schema = StructType([StructField("_0", self._udf_desc.return_type)])

    def run(self, split_index: int, data: "Iterator[GroupedBatch]") -> "Iterator[pa.RecordBatch]":
        """Apply groupBy Pandas UDF (iterator variant)."""
        import pandas as pd

        value_offsets = self._value_offsets
        for group in data:
            group_iter = iter(group)
            # Read the first batch to extract grouping keys.
            first_series = ArrowToPandasConversion.to_pandas(
                next(group_iter),
                timezone=self._runner_conf.timezone,
                prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
            )

            def dataframe_iter() -> "Iterator[pd.DataFrame]":
                # Convert lazily so peakmem stays bounded by one batch, not the whole group.
                yield pd.concat([first_series[o] for o in value_offsets], axis=1)
                for batch in group_iter:
                    series = ArrowToPandasConversion.to_pandas(
                        batch,
                        timezone=self._runner_conf.timezone,
                        prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
                    )
                    yield pd.concat([series[o] for o in value_offsets], axis=1)

            if self._num_udf_args == 1:
                result = self._func(dataframe_iter())
            else:
                key = tuple(first_series[o].iloc[0] for o in self._key_offsets)
                result = self._func(key, dataframe_iter())

            for df in result:
                verify_pandas_result(
                    df,
                    self._udf_desc.return_type,
                    self._runner_conf.assign_cols_by_name,
                    truncate_return_schema=False,
                )
                yield PandasToArrowConversion.from_pandas(
                    [df],
                    self._return_schema,
                    timezone=self._runner_conf.timezone,
                    safecheck=self._runner_conf.safecheck,
                    arrow_cast=True,
                    prefers_large_types=self._runner_conf.use_large_var_types,
                    assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                    int_to_decimal_coercion_enabled=(
                        self._runner_conf.int_to_decimal_coercion_enabled
                    ),
                )

            # Drain remaining input batches to maintain stream position.
            for _ in group_iter:
                pass


class PandasGroupedMapUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_MAP_PANDAS_UDF (applyInPandas): the single UDF receives each group
    as one pandas DataFrame of its value columns (plus the grouping key when it takes
    two arguments) and returns one pandas DataFrame, converted back to Arrow and
    resized toward the output batch byte cap. The pandas counterpart of
    ArrowGroupedMapUDFHandler."""

    eval_type = PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF

    def __init__(
        self, udf_descs: list[UDF], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udf_descs, runner_conf, eval_conf)
        # signature was lost when wrapping it
        self._num_udf_args = len(inspect.getfullargspec(self._raw_func).args)
        parsed_offsets = extract_key_value_indexes(self._udf_desc.args_offsets)
        assert len(parsed_offsets) == 1, "Expected one pair of offsets for GROUPED_MAP_PANDAS UDF."
        self._key_offsets, self._value_offsets = parsed_offsets[0]
        self._return_schema = StructType([StructField("_0", self._udf_desc.return_type)])

    def run(self, split_index: int, data: "Iterator[GroupedBatch]") -> "Iterator[pa.RecordBatch]":
        # Bound each output RecordBatch toward the byte cap (no limit when unset).
        max_output_bytes = self._runner_conf.python_udf_arrow_worker_output_batch_max_bytes
        if max_output_bytes > 0:
            return ArrowBatchTransformer.resize_batches(self._run(data), max_output_bytes)
        return self._run(data)

    def _run(self, data: "Iterator[GroupedBatch]") -> "Iterator[pa.RecordBatch]":
        """Apply groupBy Pandas UDF (non-iterator variant).

        The explicit ``del`` calls below keep peakmem bounded across groups. Without
        them, generator locals from the previous iteration stay bound on the frame until
        each statement in the next iteration rebinds its slot, so the input-side
        DataFrames overlap with the next group's allocations and the working set grows
        unbounded on wide-column, large-group inputs. ``del result`` runs on resume from
        yield, before ``data.__next__()`` is asked for the next group.
        """
        import pandas as pd
        import pyarrow as pa

        for group in data:
            all_batches = list(group)
            if all_batches:
                table = pa.Table.from_batches(all_batches).combine_chunks()
            else:
                table = pa.table({})
            all_series = ArrowToPandasConversion.to_pandas(
                table,
                timezone=self._runner_conf.timezone,
                prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
            )
            value_df = pd.concat([all_series[o] for o in self._value_offsets], axis=1)

            if self._num_udf_args == 1:
                result = self._func(value_df)
            else:
                key = tuple(all_series[o].iloc[0] for o in self._key_offsets)
                result = self._func(key, value_df)

            del all_batches, table, all_series, value_df

            verify_pandas_result(
                result,
                self._udf_desc.return_type,
                self._runner_conf.assign_cols_by_name,
                truncate_return_schema=False,
            )

            yield PandasToArrowConversion.from_pandas(
                [result],
                self._return_schema,
                timezone=self._runner_conf.timezone,
                safecheck=self._runner_conf.safecheck,
                arrow_cast=True,
                prefers_large_types=self._runner_conf.use_large_var_types,
                assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=self._runner_conf.int_to_decimal_coercion_enabled,
            )
            del result


class PandasGroupedAggUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_AGG_PANDAS_UDF: each UDF reduces its input columns (pandas Series)
    over the whole group to a single scalar; emit one row per group with one column
    per UDF, converted back to Arrow. The pandas counterpart of
    ArrowGroupedAggUDFHandler."""

    eval_type = PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF
    _multi_udf = True

    def __init__(
        self, udf_descs: list[UDF], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udf_descs, runner_conf, eval_conf)
        self._return_schema = StructType(
            [StructField("_%d" % i, udf.return_type) for i, udf in enumerate(udf_descs)]
        )

    def run(self, split_index: int, data: "Iterator[GroupedBatch]") -> "Iterator[pa.RecordBatch]":
        import pandas as pd
        import pyarrow as pa

        for group in data:
            batch_list = list(group)
            if not batch_list:
                continue
            table = pa.Table.from_batches(batch_list).combine_chunks()
            all_series = ArrowToPandasConversion.to_pandas(
                table,
                timezone=self._runner_conf.timezone,
                prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
            )
            results = [
                func(
                    *[all_series[o] for o in udf.args_offsets],
                    **{k: all_series[v] for k, v in udf.kwargs_offsets.items()},
                )
                for udf, func in zip(self._udf_descs, self._funcs)
            ]
            yield PandasToArrowConversion.from_pandas(
                [pd.Series([r]) for r in results],
                self._return_schema,
                timezone=self._runner_conf.timezone,
                safecheck=self._runner_conf.safecheck,
                arrow_cast=True,
                prefers_large_types=self._runner_conf.use_large_var_types,
                assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=self._runner_conf.int_to_decimal_coercion_enabled,
            )


class PandasGroupedAggIterUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_AGG_PANDAS_ITER_UDF: the single UDF receives each group as an
    iterator of its input columns (one pandas Series, or a tuple of Series, per input
    batch) and returns one scalar; emit one row per group. The pandas counterpart of
    ArrowGroupedAggIterUDFHandler."""

    eval_type = PythonEvalType.SQL_GROUPED_AGG_PANDAS_ITER_UDF

    def __init__(
        self, udf_descs: list[UDF], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udf_descs, runner_conf, eval_conf)
        self._return_schema = StructType([StructField("_0", self._udf_desc.return_type)])

    def run(self, split_index: int, data: "Iterator[GroupedBatch]") -> "Iterator[pa.RecordBatch]":
        import pandas as pd

        args_offsets = self._udf_desc.args_offsets

        def extract_series(batch: "pa.RecordBatch") -> Any:
            # Convert one RecordBatch to a pandas Series per column, then select args:
            # - pd.Series for a single column
            # - tuple[pd.Series, ...] for multiple columns
            all_series = ArrowToPandasConversion.to_pandas(
                batch,
                timezone=self._runner_conf.timezone,
                prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
            )
            series = tuple(all_series[o] for o in args_offsets)
            return series[0] if len(series) == 1 else series

        for group in data:
            series_iter = map(extract_series, group)
            result = self._func(series_iter)
            # Drain remaining batches to maintain stream position
            for _ in series_iter:
                pass
            yield PandasToArrowConversion.from_pandas(
                [pd.Series([result])],
                self._return_schema,
                timezone=self._runner_conf.timezone,
                safecheck=self._runner_conf.safecheck,
                arrow_cast=True,
                prefers_large_types=False,
                assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=self._runner_conf.int_to_decimal_coercion_enabled,
            )


class PandasWindowAggUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_WINDOW_AGG_PANDAS_UDF: each UDF produces one value per input row over its
    window frame -- an unbounded frame is computed once and repeated for every row,
    a bounded frame slices the input Series per row -- emitted as one column per UDF,
    converted back to Arrow. The pandas counterpart of ArrowWindowAggUDFHandler."""

    eval_type = PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF
    _multi_udf = True

    def __init__(
        self, udf_descs: list[UDF], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udf_descs, runner_conf, eval_conf)
        self._window_bound_types = runner_conf.window_bound_types
        self._return_schema = StructType(
            [StructField("_%d" % i, udf.return_type) for i, udf in enumerate(udf_descs)]
        )

    def run(self, split_index: int, data: "Iterator[GroupedBatch]") -> "Iterator[pa.RecordBatch]":
        import pandas as pd
        import pyarrow as pa

        for group in data:
            batch_list = list(group)
            if not batch_list:
                continue
            table = pa.Table.from_batches(batch_list).combine_chunks()
            all_series = ArrowToPandasConversion.to_pandas(
                table,
                timezone=self._runner_conf.timezone,
                prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
            )
            num_rows = table.num_rows

            result_series = []
            for udf_index, (udf, udf_func) in enumerate(zip(self._udf_descs, self._funcs)):
                args_offsets, kwargs_offsets = udf.args_offsets, udf.kwargs_offsets
                bound_type = self._window_bound_types[udf_index]
                if bound_type == "unbounded":
                    result = udf_func(
                        *[all_series[o] for o in args_offsets],
                        **{k: all_series[v] for k, v in kwargs_offsets.items()},
                    )
                    # Repeat the scalar result to match the window (group) length.
                    result_series.append(pd.Series([result]).repeat(num_rows))
                elif bound_type == "bounded":
                    # args_offsets[0] and args_offsets[1] are begin_index and end_index.
                    assert len(args_offsets) >= 2, len(args_offsets)
                    # Index operation is faster on np.ndarray, so we turn the
                    # index series into np arrays here for performance.
                    begin_array = all_series[args_offsets[0]].values
                    end_array = all_series[args_offsets[1]].values
                    series = [all_series[o] for o in args_offsets[2:]]
                    kw_series = {k: all_series[v] for k, v in kwargs_offsets.items()}
                    results = []
                    for i in range(num_rows):
                        # Note: Creating a slice from a series for each window is
                        #       actually pretty expensive. However, there
                        #       is no easy way to reduce cost here.
                        # Note: s.iloc[i : j] is about 30% faster than s[i: j], with
                        #       the caveat that the created slices shares the same
                        #       memory with s. Therefore, user are not allowed to
                        #       change the value of input series inside the window
                        #       function. It is rare that user needs to modify the
                        #       input series in the window function, and therefore,
                        #       it is a reasonable restriction.
                        # Note: Calling reset_index on the slices will increase the
                        #       cost of creating slices by about 100%. Therefore, for
                        #       performance reasons we don't do it here.
                        slices = [s.iloc[begin_array[i] : end_array[i]] for s in series]
                        kw_slices = {
                            k: s.iloc[begin_array[i] : end_array[i]] for k, s in kw_series.items()
                        }
                        results.append(udf_func(*slices, **kw_slices))
                    result_series.append(pd.Series(results))
                else:
                    raise PySparkRuntimeError(
                        errorClass="INVALID_WINDOW_BOUND_TYPE",
                        messageParameters={"window_bound_type": bound_type},
                    )

            yield PandasToArrowConversion.from_pandas(
                result_series,
                self._return_schema,
                timezone=self._runner_conf.timezone,
                safecheck=self._runner_conf.safecheck,
                arrow_cast=True,
                prefers_large_types=self._runner_conf.use_large_var_types,
                assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=self._runner_conf.int_to_decimal_coercion_enabled,
            )


class ArrowBatchedUDFLegacyPandasHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_ARROW_BATCHED_UDF (Arrow-optimized ``@udf``) with the legacy pandas conversion:
    convert each input RecordBatch to pandas Series, invoke each UDF row by row on their
    values, and convert the results back to Arrow through pandas."""

    eval_type = PythonEvalType.SQL_ARROW_BATCHED_UDF
    variant_conf = "use_legacy_pandas_udf_conversion"
    variant_value = True
    _multi_udf = True
    # run wraps the row-by-row evaluation in fail_on_stopiteration itself.
    _fail_on_stopiteration = False

    def __init__(
        self, udf_descs: list[UDF], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udf_descs, runner_conf, eval_conf)

        # --- UDF preparation ---
        self._udf_infos = []
        for udf, func in zip(udf_descs, self._funcs):
            udf_return_type = udf.return_type
            wrapped_func, args_kwargs_offsets = wrap_kwargs_support(
                func, udf.args_offsets, udf.kwargs_offsets
            )
            zero_arg = len(args_kwargs_offsets) == 0
            # Legacy coerces String/Binary for Arrow compatibility
            coerce = (
                str
                if isinstance(udf_return_type, StringType)
                else bytes
                if isinstance(udf_return_type, BinaryType)
                else None
            )
            self._udf_infos.append(
                (
                    wrapped_func,
                    args_kwargs_offsets or (0,),
                    zero_arg,
                    udf_return_type,
                    coerce,
                )
            )
        col_names = [f"_{i}" for i in range(len(udf_descs))]
        self._return_schema = StructType(
            [StructField(name, info[3]) for name, info in zip(col_names, self._udf_infos)]
        )

    def run(self, split_index: int, data: "Iterator[pa.RecordBatch]") -> "Iterator[pa.RecordBatch]":
        import pandas as pd

        runner_conf = self._runner_conf

        @fail_on_stopiteration
        def _evaluate_batch_udf_legacy(udf_func: Any, rows: list[tuple]) -> list[Any]:
            if runner_conf.arrow_concurrency_level <= 0:
                return [udf_func(*row) for row in rows]
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=runner_conf.arrow_concurrency_level) as pool:
                return list(pool.map(lambda row: udf_func(*row), rows))

        for input_batch in data:
            # --- Input: Arrow -> pandas columns ---
            pandas_columns = ArrowToPandasConversion.to_pandas(
                input_batch,
                timezone=runner_conf.timezone,
                schema=self._eval_conf.input_type,  # type: ignore[arg-type]
                struct_in_pandas="row",
                ndarray_as_list=True,
                prefer_int_ext_dtype=runner_conf.prefer_int_ext_dtype,
                df_for_struct=False,
            )
            num_rows = len(pandas_columns[0]) if pandas_columns else input_batch.num_rows
            if not pandas_columns:
                pandas_columns = [pd.Series([_NoValue] * num_rows)]

            # --- Process: evaluate each UDF row-by-row ---
            result_series = []
            for udf_func, offsets, zero_arg, _, coerce in self._udf_infos:
                rows = (
                    [() for _ in range(num_rows)]
                    if zero_arg
                    else list(
                        zip(*[pandas_columns[o].tolist() for o in offsets])  # type: ignore[operator]
                    )
                )
                results = _evaluate_batch_udf_legacy(udf_func, rows)
                verify_result_row_count(len(results), num_rows)
                if coerce:
                    results = [coerce(v) if v is not None else v for v in results]
                result_series.append(pd.Series(results))

            # --- Output: pandas -> Arrow ---
            yield PandasToArrowConversion.from_pandas(
                result_series,
                self._return_schema,
                timezone=runner_conf.timezone,
                safecheck=runner_conf.safecheck,
                arrow_cast=True,
                prefers_large_types=runner_conf.use_large_var_types,
                assign_cols_by_name=runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=runner_conf.int_to_decimal_coercion_enabled,
            )
