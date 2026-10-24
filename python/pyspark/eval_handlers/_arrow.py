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

import inspect
import itertools
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from pyspark import _NoValue
from pyspark.errors import PySparkRuntimeError
from pyspark.eval_handlers._base import (
    BatchEvalTypeHandler,
    CoGroupedEvalTypeHandler,
    GroupedEvalTypeHandler,
)
from pyspark.eval_handlers.utils import (
    extract_key_value_indexes,
    hashable_grouping_key,
    wrap_kwargs_support,
)
from pyspark.eval_handlers.verification import (
    verify_iter_result_row_count,
    verify_iterator_exhausted,
    verify_output_row_limit,
    verify_result_row_count,
    verify_return_type,
    verify_scalar_result,
)
from pyspark.sql.conversion import (
    ArrowBatchTransformer,
    ArrowTableToRowsConversion,
    LocalDataToArrowConversion,
)
from pyspark.sql.pandas.serializers import ArrowStreamSerializer
from pyspark.sql.pandas.types import to_arrow_schema, to_arrow_type
from pyspark.sql.pandas.utils import require_minimum_pyarrow_version
from pyspark.sql.types import StructField, StructType
from pyspark.util import PythonEvalType, fail_on_stopiteration
from pyspark.worker_metrics import WorkerMetrics

if TYPE_CHECKING:
    import pyarrow as pa

    from pyspark.eval_handlers._typing import UDF, CoGroupedBatch, GroupedBatch
    from pyspark.worker_util import EvalConf, RunnerConf


class ArrowCoGroupedMapUDFHandler(CoGroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_COGROUPED_MAP_ARROW_UDF (applyInArrow on a cogroup): the single UDF
    receives the two sides' value tables and returns one pa.Table, coerced to the
    declared schema."""

    eval_type = PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF
    _single_udf = True

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        # signature was lost when wrapping it
        self._num_udf_args = len(inspect.getfullargspec(self._raw_func).args)
        parsed_offsets = extract_key_value_indexes(self._udf.args_offsets)
        self._left_key_cols, self._left_val_cols = parsed_offsets[0]
        self._right_key_cols, self._right_val_cols = parsed_offsets[1]
        self._arrow_return_schema = to_arrow_schema(
            self._udf.return_type,
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
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
                result = self._func(left_values, right_values)
            else:
                key_table = left_keys if left_keys.num_rows > 0 else right_keys
                key = tuple(c[0] for c in key_table.columns)
                result = self._func(key, left_values, right_values)

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
    _single_udf = True
    _iter_based = True

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        # signature was lost when wrapping it
        self._num_udf_args = len(inspect.getfullargspec(self._raw_func).args)
        parsed_offsets = extract_key_value_indexes(self._udf.args_offsets)
        assert len(parsed_offsets) == 1, (
            "Expected one pair of offsets for GROUPED_MAP_ARROW_ITER UDF."
        )
        self._key_offsets = parsed_offsets[0][0]
        self._value_offsets = parsed_offsets[0][1]
        self._arrow_return_schema = to_arrow_schema(
            self._udf.return_type,
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
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
                result = self._func(value_batches)
            else:
                key = tuple(c[0] for c in keys.columns)
                result = self._func(key, value_batches)

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
    _single_udf = True

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        # signature was lost when wrapping it
        self._num_udf_args = len(inspect.getfullargspec(self._raw_func).args)
        parsed_offsets = extract_key_value_indexes(self._udf.args_offsets)
        assert len(parsed_offsets) == 1, "Expected one pair of offsets for GROUPED_MAP_ARROW UDF."
        self._key_offsets = parsed_offsets[0][0]
        self._value_offsets = parsed_offsets[0][1]
        self._arrow_return_schema = to_arrow_schema(
            self._udf.return_type,
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
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
                result = self._func(value_table)
            else:
                key = tuple(c[0] for c in keys.columns)
                result = self._func(key, value_table)

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

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        self._col_names = ["_%d" % i for i in range(len(udfs))]
        self._return_schema = to_arrow_schema(
            StructType([StructField(n, udf.return_type) for n, udf in zip(self._col_names, udfs)]),
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )

    def run(self, split_index: int, data: Iterator[GroupedBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        for group in data:
            concatenated = ArrowBatchTransformer.concat_batches(group)
            results = [
                func(
                    *[concatenated.column(o) for o in udf.args_offsets],
                    **{k: concatenated.column(v) for k, v in udf.kwargs_offsets.items()},
                )
                for udf, func in zip(self._udfs, self._funcs)
            ]
            result_arrays = [pa.array([r]) for r in results]
            batch = pa.RecordBatch.from_arrays(result_arrays, self._col_names)
            yield ArrowBatchTransformer.enforce_schema(batch, self._return_schema)


class ArrowGroupedAggIterUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_AGG_ARROW_ITER_UDF: the single UDF receives each group as an
    iterator of its input columns and returns one scalar; emit one row per group."""

    eval_type = PythonEvalType.SQL_GROUPED_AGG_ARROW_ITER_UDF
    _single_udf = True

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        self._return_schema = to_arrow_schema(
            StructType([StructField("_0", self._udf.return_type)]),
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )

    def run(self, split_index: int, data: Iterator[GroupedBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        args_offsets = self._udf.args_offsets

        def extract_args(batch: pa.RecordBatch) -> Any:
            args = tuple(batch.column(o) for o in args_offsets)
            return args[0] if len(args) == 1 else args

        for group in data:
            batch_iter = map(extract_args, group)
            result = self._func(batch_iter)
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

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        self._window_bound_types = runner_conf.window_bound_types
        self._col_names = ["_%d" % i for i in range(len(udfs))]
        self._return_schema = to_arrow_schema(
            StructType([StructField(n, udf.return_type) for n, udf in zip(self._col_names, udfs)]),
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )

    def run(self, split_index: int, data: Iterator[GroupedBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        for group in data:
            concatenated = ArrowBatchTransformer.concat_batches(group)
            num_rows = concatenated.num_rows

            result_arrays = []
            for udf_index, (udf, udf_func) in enumerate(zip(self._udfs, self._funcs)):
                args_offsets, kwargs_offsets = udf.args_offsets, udf.kwargs_offsets
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


class ArrowGroupedAggIncrementalPartialUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_AGG_ARROW_INCREMENTAL_PARTIAL_UDF: map-side PARTIAL stage of an
    incremental ``Aggregator``. Hash-combine input rows into a per-group buffer via
    the aggregator's ``reduce`` and emit one row per key: the grouping key columns
    followed by one buffer struct column per aggregator.

    Ordinary (multi-group) batches are streamed in; the worker keeps one running
    buffer per distinct grouping key -- never whole groups of rows. Because the FINAL
    stage re-groups these authoritatively after the shuffle, the grouping here only
    needs to be a best-effort combine: any keys it fails to collapse (e.g. NaN, which
    compares unequal to itself) are merged downstream."""

    eval_type = PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_PARTIAL_UDF

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        # The leading `num_grouping_keys` input columns are the grouping keys (see the operator);
        # `grouping_key_schema` carries their names/types so the emitted key columns round-trip.
        grouping_key_schema = eval_conf.grouping_key_schema
        self._num_grouping_keys = (
            len(grouping_key_schema.fields) if grouping_key_schema is not None else 0
        )

        # The pickled function is the Aggregator object itself, whose zero/reduce/merge/finish
        # methods are called directly, so take it unwrapped rather than as a plain callable.
        self._aggs = self._raw_funcs
        self._buffer_col_names = ["_%d" % i for i in range(len(udfs))]
        self._buffer_arrow_types = [
            to_arrow_type(
                agg.bufferSchema,
                timezone="UTC",
                prefers_large_types=runner_conf.use_large_var_types,
            )
            for agg in self._aggs
        ]
        # Buffer field names are invariant across groups; compute them once per aggregator.
        self._field_names_by_udf = [[f.name for f in agg.bufferSchema.fields] for agg in self._aggs]

        # The aggregator's `reduce` receives a single positional tuple, so any named arguments at
        # the call site are appended after the positional ones, in call order (kwargs_offsets
        # preserves that order). This mirrors how a Python call `f(*args, **kwargs)` would order
        # them into one value tuple.
        self._input_offsets_by_udf = [
            list(udf.args_offsets) + list(udf.kwargs_offsets.values()) for udf in udfs
        ]
        # Input columns actually consumed per batch: the leading grouping keys plus every
        # aggregator input, deduplicated. A UDF input may reuse a grouping-key column (the operator
        # dedups its projection), so converting by distinct offset avoids repeated `to_pylist()`.
        self._needed_offsets = sorted(
            set(range(self._num_grouping_keys))
            | {o for offsets in self._input_offsets_by_udf for o in offsets}
        )

        # Cap the map-side buffer so a high-cardinality partition -- exactly where partial
        # aggregation degenerates -- cannot grow the per-key dict without bound and OOM the worker.
        # When the distinct-key count reaches the cap we flush the whole map as one batch and start
        # fresh; end-of-partition buffers are emitted in equally bounded chunks. This is safe
        # because the FINAL stage re-groups the emitted partial buffers authoritatively after the
        # shuffle and merges any duplicate keys that early flushes produce. A non-positive
        # maxRecordsPerBatch means "unbounded" (mirroring the reader side).
        max_records = runner_conf.arrow_max_records_per_batch
        self._cap = max_records if max_records > 0 else None

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        num_grouping_keys = self._num_grouping_keys
        cap = self._cap

        # hashable key -> (representative key value tuple, list of per-aggregator buffers)
        groups: dict[Any, tuple] = {}
        key_field_types: list | None = None

        def make_batch(entries: list) -> pa.RecordBatch:
            arrays = []
            names = []
            for j in range(num_grouping_keys):
                arrays.append(
                    pa.array(
                        [e[0][j] for e in entries],
                        type=key_field_types[j],  # type: ignore[index]
                    )
                )
                names.append("k_%d" % j)
            for i, field_names in enumerate(self._field_names_by_udf):
                structs = [
                    {name: e[1][i][t] for t, name in enumerate(field_names)} for e in entries
                ]
                arrays.append(pa.array(structs, type=self._buffer_arrow_types[i]))
                names.append(self._buffer_col_names[i])
            return pa.RecordBatch.from_arrays(arrays, names)

        for batch in data:
            if key_field_types is None:
                key_field_types = [batch.schema.field(j).type for j in range(num_grouping_keys)]
            pylist_by_offset = {o: batch.column(o).to_pylist() for o in self._needed_offsets}
            key_cols = [pylist_by_offset[j] for j in range(num_grouping_keys)]
            udf_cols = [
                [pylist_by_offset[o] for o in offsets] for offsets in self._input_offsets_by_udf
            ]
            for r in range(batch.num_rows):
                key_values = tuple(key_cols[j][r] for j in range(num_grouping_keys))
                hashable_key = hashable_grouping_key(key_values)
                entry = groups.get(hashable_key)
                if entry is None:
                    buffers = [agg.zero() for agg in self._aggs]
                    groups[hashable_key] = (key_values, buffers)
                else:
                    buffers = entry[1]
                for i, agg in enumerate(self._aggs):
                    cols_i = udf_cols[i]
                    buffers[i] = agg.reduce(buffers[i], tuple(c[r] for c in cols_i))
            if cap is not None and len(groups) >= cap:
                yield make_batch(list(groups.values()))
                groups = {}

        if not groups:
            # Empty partition (or fully flushed above): emit nothing more. The FINAL stage
            # supplies the global-aggregation identity row when there is no input at all.
            return

        entries = list(groups.values())
        if cap is None:
            yield make_batch(entries)
        else:
            for start in range(0, len(entries), cap):
                yield make_batch(entries[start : start + cap])


class ArrowGroupedAggIncrementalFinalUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_AGG_ARROW_INCREMENTAL_FINAL_UDF: post-shuffle FINAL stage of an
    incremental ``Aggregator``. Merge each group's partial buffers via the aggregator's
    ``merge`` and produce the output via ``finish``, streaming the buffers one batch at
    a time.

    Every group the JVM sends yields exactly one output row; null partial-buffer rows
    are skipped, so an empty global aggregation (a single all-null buffer row injected
    by the operator) still produces ``finish(zero)``."""

    eval_type = PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_FINAL_UDF

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        self._col_names = ["_%d" % i for i in range(len(udfs))]
        self._return_schema = to_arrow_schema(
            StructType([StructField(n, udf.return_type) for n, udf in zip(self._col_names, udfs)]),
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )
        # The Aggregator object itself, unwrapped (see ArrowGroupedAggIncrementalPartialUDFHandler).
        self._aggs = self._raw_funcs
        # Buffer field names are invariant across groups and batches; compute once per aggregator.
        self._field_names_by_udf = [[f.name for f in agg.bufferSchema.fields] for agg in self._aggs]

    def run(self, split_index: int, data: Iterator[GroupedBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        for group in data:
            merged: list = [None] * len(self._aggs)
            for batch in group:
                for i, (udf, agg) in enumerate(zip(self._udfs, self._aggs)):
                    field_names = self._field_names_by_udf[i]
                    m = merged[i]
                    for row in batch.column(udf.args_offsets[0]).to_pylist():
                        if row is None:
                            continue
                        partial = tuple(row[name] for name in field_names)
                        m = partial if m is None else agg.merge(m, partial)
                    merged[i] = m
            results = []
            for i, agg in enumerate(self._aggs):
                m = merged[i] if merged[i] is not None else agg.zero()
                results.append(agg.finish(m))
            # Type each output array explicitly (mirroring the PARTIAL stage) so a non-trivial
            # outputType or an all-None column does not depend on Arrow type inference.
            result_arrays = [
                pa.array([r], type=self._return_schema.field(i).type) for i, r in enumerate(results)
            ]
            batch = pa.RecordBatch.from_arrays(result_arrays, self._col_names)
            yield ArrowBatchTransformer.enforce_schema(batch, self._return_schema)


class ArrowWindowAggIncrementalUDFHandler(GroupedEvalTypeHandler["pa.RecordBatch"]):
    """SQL_WINDOW_AGG_ARROW_INCREMENTAL_UDF: window aggregation with an incremental
    ``Aggregator``. The operator sends each frame -- the whole partition for an
    unbounded frame, or per-row ``[begin, end)`` slices for a bounded one -- and each
    frame's rows are folded with ``reduce`` (from a fresh ``zero``) and finished with
    ``finish``, one output value per input row. A window has no shuffle, so the
    intermediate buffer never leaves the worker; ``merge`` is not used here."""

    eval_type = PythonEvalType.SQL_WINDOW_AGG_ARROW_INCREMENTAL_UDF

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        self._window_bound_types = runner_conf.window_bound_types
        self._col_names = ["_%d" % i for i in range(len(udfs))]
        self._return_schema = to_arrow_schema(
            StructType([StructField(n, udf.return_type) for n, udf in zip(self._col_names, udfs)]),
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )
        # The Aggregator object itself, unwrapped (see ArrowGroupedAggIncrementalPartialUDFHandler).
        self._aggs = self._raw_funcs

    def run(self, split_index: int, data: Iterator[GroupedBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        def fold(agg: Any, buffer: Any, value_cols: list, start: int, end: int) -> Any:
            # Fold rows ``[start, end)`` (each a tuple across ``value_cols``, matching the
            # call-site argument order) into ``buffer`` via the aggregator's ``reduce``.
            for r in range(start, end):
                buffer = agg.reduce(buffer, tuple(c[r] for c in value_cols))
            return buffer

        for group in data:
            concatenated = ArrowBatchTransformer.concat_batches(group)
            num_rows = concatenated.num_rows

            result_arrays = []
            for udf_index, (udf, agg) in enumerate(zip(self._udfs, self._aggs)):
                args_offsets, kwargs_offsets = udf.args_offsets, udf.kwargs_offsets
                bound_type = self._window_bound_types[udf_index]
                result_type = self._return_schema.field(udf_index).type
                if bound_type == "unbounded":
                    # One frame spanning the whole partition: compute once, repeat per row.
                    value_cols = [concatenated.column(o).to_pylist() for o in args_offsets] + [
                        concatenated.column(v).to_pylist() for v in kwargs_offsets.values()
                    ]
                    result = agg.finish(fold(agg, agg.zero(), value_cols, 0, num_rows))
                    result_arrays.append(pa.array([result] * num_rows, type=result_type))
                elif bound_type == "bounded":
                    # Per-row frame ``[begin, end)``. Materialize the aggregator's input columns
                    # once; frames index into them by row.
                    begin_col = concatenated.column(args_offsets[0])
                    end_col = concatenated.column(args_offsets[1])
                    data_offsets = list(args_offsets[2:]) + list(kwargs_offsets.values())
                    value_cols = [concatenated.column(o).to_pylist() for o in data_offsets]
                    # When consecutive frames share the same lower bound and only grow on the
                    # right (e.g. rowsBetween(unboundedPreceding, currentRow)), extend the
                    # running buffer by the newly-included rows instead of refolding from
                    # ``zero`` -- O(n) overall rather than O(n^2). Otherwise -- the lower bound
                    # advanced (a row left the window, which ``reduce`` cannot subtract) or the
                    # frame shrank -- refold the frame from ``zero``.
                    results = []
                    have_running = False
                    running: Any = None
                    prev_begin = -1
                    prev_end = 0
                    for i in range(num_rows):
                        begin = begin_col[i].as_py()
                        end = end_col[i].as_py()
                        if have_running and begin == prev_begin and end >= prev_end:
                            running = fold(agg, running, value_cols, prev_end, end)
                        else:
                            running = fold(agg, agg.zero(), value_cols, begin, end)
                        have_running = True
                        prev_begin, prev_end = begin, end
                        results.append(agg.finish(running))
                    result_arrays.append(pa.array(results, type=result_type))
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
    _single_udf = True
    _iter_based = True

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        # Pre-processing
        input_batches = map(ArrowBatchTransformer.flatten_struct, data)

        # invoke the UDF
        output_batches = self._func(input_batches)

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
    _single_udf = True
    _iter_based = True

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        self._arrow_return_type = to_arrow_type(
            self._udf.return_type,
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        args_offsets = self._udf.args_offsets
        num_input_rows = 0

        def extract_args(batch: pa.RecordBatch) -> Any:
            nonlocal num_input_rows
            args = tuple(batch.column(o) for o in args_offsets)
            num_input_rows += batch.num_rows
            return args[0] if len(args) == 1 else args

        # Extract args from input batches (streaming)
        args_iter = map(extract_args, data)

        # Call UDF and verify result type (iterator of pa.Array)
        verified_iter = verify_return_type(self._func(args_iter), Iterator[pa.Array])

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

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)
        self._col_names = ["_%d" % i for i in range(len(udfs))]
        self._combined_arrow_schema = to_arrow_schema(
            StructType([StructField(n, udf.return_type) for n, udf in zip(self._col_names, udfs)]),
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        for batch in data:
            output_batch = pa.RecordBatch.from_arrays(
                [
                    func(
                        *[batch.column(o) for o in udf.args_offsets],
                        **{k: batch.column(v) for k, v in udf.kwargs_offsets.items()},
                    )
                    for udf, func in zip(self._udfs, self._funcs)
                ],
                self._col_names,
            )
            output_batch = ArrowBatchTransformer.enforce_schema(
                output_batch, self._combined_arrow_schema
            )
            verify_scalar_result(output_batch, batch.num_rows)
            yield output_batch


class ArrowBatchedUDFArrowHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_ARROW_BATCHED_UDF (Arrow-optimized ``@udf``) with the Arrow conversion: convert each
    input RecordBatch to Python values, invoke each UDF row by row, and convert the results
    directly to Arrow arrays of the declared return types."""

    eval_type = PythonEvalType.SQL_ARROW_BATCHED_UDF
    variant_conf = "use_legacy_pandas_udf_conversion"
    variant_value = False
    # run wraps the row-by-row evaluation in fail_on_stopiteration itself.
    _fail_on_stopiteration = False

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)

        # --- UDF preparation ---
        self._udf_infos = []
        for udf, func in zip(udfs, self._funcs):
            wrapped_func, args_kwargs_offsets = wrap_kwargs_support(
                func, udf.args_offsets, udf.kwargs_offsets
            )
            zero_arg = len(args_kwargs_offsets) == 0
            self._udf_infos.append(
                (
                    wrapped_func,
                    args_kwargs_offsets or (0,),
                    zero_arg,
                    to_arrow_type(
                        udf.return_type,
                        timezone="UTC",
                        prefers_large_types=runner_conf.use_large_var_types,
                    ),
                    LocalDataToArrowConversion._create_converter(
                        udf.return_type,
                        none_on_identity=True,
                        int_to_decimal_coercion_enabled=runner_conf.int_to_decimal_coercion_enabled,
                    ),
                )
            )
        assert self._udf_infos, "At least one Arrow UDF expected here."
        self._col_names = [f"_{i}" for i in range(len(udfs))]

        # --- Input preparation ---
        self._arrow_to_py_converters = [
            ArrowTableToRowsConversion._create_converter(
                f.dataType, none_on_identity=True, binary_as_bytes=runner_conf.binary_as_bytes
            )
            for f in eval_conf.input_type  # type: ignore[union-attr]
        ]

    @property
    def serializer(self) -> ArrowStreamSerializer:
        return ArrowStreamSerializer(
            write_start_stream=True,
            collect_timing=self._runner_conf.detailed_timing_enabled,
        )

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        runner_conf = self._runner_conf
        metrics = WorkerMetrics()
        if runner_conf.detailed_timing_enabled:
            phase_timers = (
                metrics.measure("pythonInputPreparationTime"),
                metrics.measure("pythonUDFExecutionTime"),
                metrics.measure("pythonOutputPreparationTime"),
            )
            metrics.set("pythonNumTimingReports", 1)
            metrics.set("pythonNumTimedBatches", 0)
        else:
            phase_timers = None

        @fail_on_stopiteration
        def _evaluate_batch_udf(udf_func: Any, rows: list[tuple]) -> list[Any]:
            if runner_conf.arrow_concurrency_level <= 0:
                return [udf_func(*row) for row in rows]
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=runner_conf.arrow_concurrency_level) as pool:
                return list(pool.map(lambda row: udf_func(*row), rows))

        # Keep phase totals and the batch count local, then publish them once per task.
        input_duration_ns = 0
        udf_duration_ns = 0
        output_duration_ns = 0
        num_timed_batches = 0
        clock = time.perf_counter_ns
        num_udfs = len(self._udf_infos)
        try:
            for input_batch in data:
                if phase_timers is not None:
                    num_timed_batches += 1
                    input_start_ns = clock()
                num_rows = input_batch.num_rows

                # --- Input: Arrow -> Python columns ---
                columns = [
                    (
                        [conv(v) for v in ArrowTableToRowsConversion._to_pylist(col)]
                        if conv is not None
                        else ArrowTableToRowsConversion._to_pylist(col)
                    )
                    for col, conv in zip(input_batch.itercolumns(), self._arrow_to_py_converters)
                ]
                if not columns:
                    columns = [[_NoValue] * num_rows]

                # --- Process: evaluate each UDF row-by-row ---
                output_arrays: list[pa.Array] = []
                for udf_func, offsets, zero_arg, arrow_return_type, result_conv in self._udf_infos:
                    if phase_timers is not None and output_arrays:
                        # Reuse the batch start for the first UDF, avoiding another clock read.
                        # Later UDFs start input after the loop transition.
                        input_start_ns = clock()
                    rows = (
                        [() for _ in range(num_rows)]
                        if zero_arg
                        else list(zip(*[columns[o] for o in offsets]))
                    )
                    if phase_timers is not None:
                        udf_start_ns = clock()
                        input_duration_ns += udf_start_ns - input_start_ns
                    results = _evaluate_batch_udf(udf_func, rows)
                    if phase_timers is not None:
                        output_start_ns = clock()
                        udf_duration_ns += output_start_ns - udf_start_ns
                    verify_result_row_count(len(results), num_rows)

                    # --- Output: Python -> Arrow ---
                    converted = (
                        [result_conv(r) for r in results] if result_conv is not None else results
                    )
                    try:
                        arr = pa.array(converted, type=arrow_return_type)
                    except pa.lib.ArrowInvalid:
                        arr = pa.array(converted).cast(
                            target_type=arrow_return_type, safe=runner_conf.safecheck
                        )
                    output_arrays.append(arr)
                    # All UDF results for this batch are ready; time its assembly as output.
                    if len(output_arrays) == num_udfs:
                        output_batch = pa.RecordBatch.from_arrays(output_arrays, self._col_names)
                    if phase_timers is not None:
                        output_duration_ns += clock() - output_start_ns

                yield output_batch
                del output_batch
        finally:
            # Publish the count and completed intervals if the generator closes early or raises.
            if phase_timers is not None:
                metrics.set("pythonNumTimedBatches", num_timed_batches)
                input_timer, udf_timer, output_timer = phase_timers
                input_timer.add_duration_ns(input_duration_ns)
                udf_timer.add_duration_ns(udf_duration_ns)
                output_timer.add_duration_ns(output_duration_ns)
