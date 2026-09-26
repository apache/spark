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

"""Handlers for the Arrow-native UDF eval types (the UDF exchanges ``pa.Array`` /
``pa.RecordBatch`` values directly, without a pandas conversion).

pyarrow is imported lazily (inside ``run`` and the type-checking block) so the module
stays importable and its handlers register without pyarrow installed. Each handler
calls ``require_minimum_pyarrow_version`` in ``__init__``, so a missing or too-old
pyarrow surfaces a clear error when the handler runs rather than leaving the eval type
unregistered.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from pyspark.errors import PySparkRuntimeError
from pyspark.eval_handlers._base import (
    BatchEvalTypeHandler,
    CoGroupedEvalTypeHandler,
    GroupedEvalTypeHandler,
)
from pyspark.eval_handlers.utils import extract_key_value_indexes
from pyspark.eval_handlers.verification import (
    verify_iter_result_row_count,
    verify_iterator_exhausted,
    verify_output_row_limit,
    verify_return_type,
    verify_scalar_result,
)
from pyspark.sql.conversion import ArrowBatchTransformer
from pyspark.sql.pandas.types import to_arrow_schema, to_arrow_type
from pyspark.sql.pandas.utils import require_minimum_pyarrow_version
from pyspark.sql.types import StructField, StructType
from pyspark.util import PythonEvalType

if TYPE_CHECKING:
    import pyarrow as pa

    from pyspark.eval_handlers._typing import CoGroupedBatch, GroupedBatch
    from pyspark.worker_util import EvalConf, RunnerConf


class ArrowCoGroupedMapUDFHandler(CoGroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_COGROUPED_MAP_ARROW_UDF (applyInArrow on a cogroup): the single UDF
    receives the two sides' value tables and returns one pa.Table, coerced to the
    declared schema."""

    eval_type = PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        assert len(udfs) == 1, "One COGROUPED_MAP_ARROW UDF expected here."
        self._cogrouped_udf, arg_offsets, return_type, self._num_udf_args = udfs[0]
        parsed_offsets = extract_key_value_indexes(arg_offsets)
        self._left_key_cols, self._left_val_cols = parsed_offsets[0]
        self._right_key_cols, self._right_val_cols = parsed_offsets[1]
        self._arrow_return_schema = to_arrow_schema(
            return_type, timezone="UTC", prefers_large_types=runner_conf.use_large_var_types
        )

    def run(self, split_index: int, data: Iterator[CoGroupedBatch]) -> Iterator[pa.RecordBatch]:
        """Apply cogroupBy Arrow UDF."""
        import pyarrow as pa

        select_columns = ArrowBatchTransformer.select_columns

        def table_from_batches(batches: list[pa.RecordBatch], cols: list[int]) -> pa.Table:
            return pa.Table.from_batches([select_columns(b, cols) for b in batches])

        for left_batches, right_batches in data:
            left_keys = table_from_batches(left_batches, self._left_key_cols)
            left_values = table_from_batches(left_batches, self._left_val_cols)
            right_keys = table_from_batches(right_batches, self._right_key_cols)
            right_values = table_from_batches(right_batches, self._right_val_cols)

            if self._num_udf_args == 2:
                result = self._cogrouped_udf(left_values, right_values)
            else:
                key_table = left_keys if left_keys.num_rows > 0 else right_keys
                key = tuple(c[0] for c in key_table.columns)
                result = self._cogrouped_udf(key, left_values, right_values)

            verify_return_type(result, pa.Table)
            # Verify types (and reorder by name when configured).
            result = ArrowBatchTransformer.enforce_schema(
                result,
                self._arrow_return_schema,
                arrow_cast=False,
                reorder_by_name=self._runner_conf.assign_cols_by_name,
            )

            for batch in result.to_batches():
                yield ArrowBatchTransformer.wrap_struct(batch)


class ArrowGroupedMapIterUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_MAP_ARROW_ITER_UDF: the single UDF receives each group as an
    iterator of RecordBatches and returns an iterator of RecordBatches, coerced
    to the declared schema."""

    eval_type = PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        assert len(udfs) == 1, "One GROUPED_MAP_ARROW_ITER UDF expected here."
        self._grouped_udf, arg_offsets, return_type, self._num_udf_args = udfs[0]
        parsed_offsets = extract_key_value_indexes(arg_offsets)
        assert len(parsed_offsets) == 1, (
            "Expected one pair of offsets for GROUPED_MAP_ARROW_ITER UDF."
        )
        self._key_offsets = parsed_offsets[0][0]
        self._value_offsets = parsed_offsets[0][1]
        self._arrow_return_schema = to_arrow_schema(
            return_type, timezone="UTC", prefers_large_types=runner_conf.use_large_var_types
        )

    def run(self, split_index: int, data: Iterator[GroupedBatch]) -> Iterator[pa.RecordBatch]:
        """Apply groupBy Arrow UDF (iterator variant)."""
        import pyarrow as pa

        key_offsets = self._key_offsets
        value_offsets = self._value_offsets
        for group in data:
            # Flatten struct column into separate columns
            flattened_iter = map(ArrowBatchTransformer.flatten_struct, group)

            # Materialize first batch to get keys
            first_batch = next(flattened_iter)
            keys = pa.RecordBatch.from_arrays(
                [first_batch.columns[o] for o in key_offsets],
                [first_batch.schema.names[o] for o in key_offsets],
            )
            value_batches = (
                pa.RecordBatch.from_arrays(
                    [b.columns[o] for o in value_offsets],
                    [b.schema.names[o] for o in value_offsets],
                )
                for b in itertools.chain((first_batch,), flattened_iter)
            )

            # Call UDF with iterator of batches
            if self._num_udf_args == 1:
                result = self._grouped_udf(value_batches)
            else:
                key = tuple(c[0] for c in keys.columns)
                result = self._grouped_udf(key, value_batches)

            # Verify (and reorder by name when configured) each output batch
            for batch in verify_return_type(result, Iterator[pa.RecordBatch]):
                batch = ArrowBatchTransformer.enforce_schema(
                    batch,
                    self._arrow_return_schema,
                    arrow_cast=False,
                    reorder_by_name=self._runner_conf.assign_cols_by_name,
                )
                yield ArrowBatchTransformer.wrap_struct(batch)

            # Drain remaining input batches to maintain stream position
            for _ in value_batches:
                pass


class ArrowGroupedMapUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_MAP_ARROW_UDF (applyInArrow): the single UDF receives each
    group as one pa.Table and returns one pa.Table, coerced to the declared
    schema."""

    eval_type = PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        assert len(udfs) == 1, "One GROUPED_MAP_ARROW UDF expected here."
        self._grouped_udf, arg_offsets, return_type, self._num_udf_args = udfs[0]
        parsed_offsets = extract_key_value_indexes(arg_offsets)
        assert len(parsed_offsets) == 1, "Expected one pair of offsets for GROUPED_MAP_ARROW UDF."
        self._key_offsets = parsed_offsets[0][0]
        self._value_offsets = parsed_offsets[0][1]
        self._arrow_return_schema = to_arrow_schema(
            return_type, timezone="UTC", prefers_large_types=runner_conf.use_large_var_types
        )

    def run(self, split_index: int, data: Iterator[GroupedBatch]) -> Iterator[pa.RecordBatch]:
        """Apply groupBy Arrow UDF (non-iterator variant)."""
        import pyarrow as pa

        key_offsets = self._key_offsets
        value_offsets = self._value_offsets
        for group in data:
            # Flatten struct column into separate columns
            flattened = map(ArrowBatchTransformer.flatten_struct, group)

            # Materialize first batch to get keys
            first_batch = next(flattened)
            keys = pa.RecordBatch.from_arrays(
                [first_batch.columns[o] for o in key_offsets],
                [first_batch.schema.names[o] for o in key_offsets],
            )
            value_batches = (
                pa.RecordBatch.from_arrays(
                    [b.columns[o] for o in value_offsets],
                    [b.schema.names[o] for o in value_offsets],
                )
                for b in itertools.chain((first_batch,), flattened)
            )

            # Call UDF
            value_table = pa.Table.from_batches(value_batches)
            if self._num_udf_args == 1:
                result = self._grouped_udf(value_table)
            else:
                key = tuple(c[0] for c in keys.columns)
                result = self._grouped_udf(key, value_table)

            verify_return_type(result, pa.Table)
            # Verify types (and reorder by name when configured).
            result = ArrowBatchTransformer.enforce_schema(
                result,
                self._arrow_return_schema,
                arrow_cast=False,
                reorder_by_name=self._runner_conf.assign_cols_by_name,
            )

            for batch in result.to_batches():
                yield ArrowBatchTransformer.wrap_struct(batch)


class ArrowGroupedAggUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_AGG_ARROW_UDF: each UDF reduces its input columns over the whole
    group to a single scalar; emit one row per group with one column per UDF,
    coerced to the declared schema."""

    eval_type = PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        self._col_names = ["_%d" % i for i in range(len(udfs))]
        self._return_schema = to_arrow_schema(
            StructType([StructField(n, rt) for n, (_, _, _, rt) in zip(self._col_names, udfs)]),
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )

    def run(self, split_index: int, data: Iterator[GroupedBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        for group in data:
            batch_list = list(group)
            if not batch_list:
                continue
            concatenated = ArrowBatchTransformer.concat_batches(batch_list)
            results = [
                udf_func(
                    *[concatenated.column(o) for o in args_offsets],
                    **{k: concatenated.column(v) for k, v in kwargs_offsets.items()},
                )
                for udf_func, args_offsets, kwargs_offsets, _ in self._udfs
            ]
            result_arrays = [pa.array([r]) for r in results]
            batch = pa.RecordBatch.from_arrays(result_arrays, self._col_names)
            yield ArrowBatchTransformer.enforce_schema(batch, self._return_schema)


class ArrowGroupedAggIterUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_AGG_ARROW_ITER_UDF: the single UDF receives each group as an
    iterator of its input columns and returns one scalar; emit one row per group."""

    eval_type = PythonEvalType.SQL_GROUPED_AGG_ARROW_ITER_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        assert len(udfs) == 1, "One GROUPED_AGG_ARROW_ITER UDF expected here."
        self._udf_func, self._args_offsets, _, return_type = udfs[0]
        self._return_schema = to_arrow_schema(
            StructType([StructField("_0", return_type)]),
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )

    def run(self, split_index: int, data: Iterator[GroupedBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        args_offsets = self._args_offsets

        def extract_args(batch: pa.RecordBatch) -> Any:
            args = tuple(batch.column(o) for o in args_offsets)
            return args[0] if len(args) == 1 else args

        for group in data:
            batch_iter = map(extract_args, group)
            result = self._udf_func(batch_iter)
            # Drain remaining batches to maintain stream position
            for _ in batch_iter:
                pass
            batch = pa.RecordBatch.from_arrays([pa.array([result])], ["_0"])
            yield ArrowBatchTransformer.enforce_schema(batch, self._return_schema)


class ArrowWindowAggUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_WINDOW_AGG_ARROW_UDF: each UDF produces one value per input row over its
    window frame -- an unbounded frame is computed once and repeated for every row,
    a bounded frame slices the input columns per row -- emitted as one column per
    UDF, coerced to the declared schema."""

    eval_type = PythonEvalType.SQL_WINDOW_AGG_ARROW_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        self._window_bound_types = runner_conf.window_bound_types
        self._col_names = ["_%d" % i for i in range(len(udfs))]
        self._return_schema = to_arrow_schema(
            StructType([StructField(n, rt) for n, (_, _, _, rt) in zip(self._col_names, udfs)]),
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )

    def run(self, split_index: int, data: Iterator[GroupedBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        for group in data:
            batch_list = list(group)
            if not batch_list:
                continue
            concatenated = ArrowBatchTransformer.concat_batches(batch_list)
            num_rows = concatenated.num_rows

            result_arrays = []
            for udf_index, (udf_func, args_offsets, kwargs_offsets, _) in enumerate(self._udfs):
                bound_type = self._window_bound_types[udf_index]
                if bound_type == "unbounded":
                    result = udf_func(
                        *[concatenated.column(o) for o in args_offsets],
                        **{k: concatenated.column(v) for k, v in kwargs_offsets.items()},
                    )
                    result_arrays.append(pa.repeat(result, num_rows))
                elif bound_type == "bounded":
                    begin_col = concatenated.column(args_offsets[0])
                    end_col = concatenated.column(args_offsets[1])
                    results = []
                    for i in range(num_rows):
                        offset = begin_col[i].as_py()
                        length = end_col[i].as_py() - offset
                        slices = [
                            concatenated.column(o).slice(offset=offset, length=length)
                            for o in args_offsets[2:]
                        ]
                        kw_slices = {
                            k: concatenated.column(v).slice(offset=offset, length=length)
                            for k, v in kwargs_offsets.items()
                        }
                        results.append(udf_func(*slices, **kw_slices))
                    result_arrays.append(pa.array(results))
                else:
                    raise PySparkRuntimeError(
                        errorClass="INVALID_WINDOW_BOUND_TYPE",
                        messageParameters={"window_bound_type": bound_type},
                    )

            batch = pa.RecordBatch.from_arrays(result_arrays, self._col_names)
            yield ArrowBatchTransformer.enforce_schema(batch, self._return_schema)


class ArrowMapUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_MAP_ARROW_ITER_UDF (mapInArrow): the single UDF receives the input
    RecordBatch stream and yields a RecordBatch stream, exchanged as flattened
    columns on the wire and wrapped back into a single struct column."""

    eval_type = PythonEvalType.SQL_MAP_ARROW_ITER_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        assert len(udfs) == 1, "One MAP_ARROW_ITER UDF expected here."
        self._udf_func = udfs[0][0]

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        # Pre-processing
        input_batches = map(ArrowBatchTransformer.flatten_struct, data)

        # invoke the UDF
        output_batches = self._udf_func(input_batches)

        # The declared signature is Iterator[...], so a strict iterator is required by
        # default. With the legacy flag, accept any object Python can iterate over -- via
        # iter(...), which honors both __iter__ and the sequence protocol (__getitem__) --
        # by adapting it into an iterator before the shared element-type verification.
        if self._runner_conf.map_in_batch_legacy_accept_any_iterable and not isinstance(
            output_batches, Iterator
        ):
            try:
                output_batches = iter(output_batches)
            except TypeError:
                # Not iterable at all; leave it so verify_return_type below raises the
                # standard UDF_RETURN_TYPE error.
                pass

        # Post-processing
        verified_iter = verify_return_type(output_batches, Iterator[pa.RecordBatch])
        yield from map(ArrowBatchTransformer.wrap_struct, verified_iter)


class ArrowScalarIterUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_SCALAR_ARROW_ITER_UDF: the UDF receives an iterator of the argument
    columns and yields an iterator of pa.Array; enforce the declared type on each
    result and verify the total row count matches the input."""

    eval_type = PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        assert len(udfs) == 1, "One SCALAR_ARROW_ITER UDF expected here."
        self._udf_func, self._args_offsets, _, return_type = udfs[0]
        self._arrow_return_type = to_arrow_type(
            return_type, timezone="UTC", prefers_large_types=runner_conf.use_large_var_types
        )

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        args_offsets = self._args_offsets
        num_input_rows = 0

        def extract_args(batch: pa.RecordBatch) -> Any:
            nonlocal num_input_rows
            args = tuple(batch.column(o) for o in args_offsets)
            num_input_rows += batch.num_rows
            return args[0] if len(args) == 1 else args

        # Extract args from input batches (streaming)
        args_iter = map(extract_args, data)

        # Call UDF and verify result type (iterator of pa.Array)
        verified_iter = verify_return_type(self._udf_func(args_iter), Iterator[pa.Array])

        # Process results: enforce schema and assemble into RecordBatch
        target_schema = pa.schema([pa.field("_0", self._arrow_return_type)])

        def process_results() -> Iterator[pa.RecordBatch]:
            for result in verified_iter:
                batch = pa.RecordBatch.from_arrays([result], ["_0"])
                yield ArrowBatchTransformer.enforce_schema(batch, target_schema, safecheck=True)

        # Apply row limit check (fail-fast)
        limited = verify_output_row_limit(process_results(), lambda: num_input_rows)

        # Apply row count match check (final)
        matched = verify_iter_result_row_count(limited, lambda: num_input_rows)

        # Yield batches
        yield from matched

        # Verify iterator consumed
        verify_iterator_exhausted(args_iter)


class ArrowScalarUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_SCALAR_ARROW_UDF: invoke each UDF once per input RecordBatch, coerce
    the result to the declared schema, and check the row count."""

    eval_type = PythonEvalType.SQL_SCALAR_ARROW_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        self._col_names = ["_%d" % i for i in range(len(udfs))]
        self._combined_arrow_schema = to_arrow_schema(
            StructType([StructField(n, rt) for n, (_, _, _, rt) in zip(self._col_names, udfs)]),
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        for batch in data:
            output_batch = pa.RecordBatch.from_arrays(
                [
                    udf_func(
                        *[batch.column(o) for o in args_offsets],
                        **{k: batch.column(v) for k, v in kwargs_offsets.items()},
                    )
                    for udf_func, args_offsets, kwargs_offsets, _ in self._udfs
                ],
                self._col_names,
            )
            output_batch = ArrowBatchTransformer.enforce_schema(
                output_batch, self._combined_arrow_schema
            )
            verify_scalar_result(output_batch, batch.num_rows)
            yield output_batch
