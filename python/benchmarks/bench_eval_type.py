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
Microbenchmarks for PySpark UDF eval types.

Each benchmark class simulates the full worker pipeline for one eval type
by constructing the complete binary protocol that ``worker.py``'s
``main(infile, outfile)`` expects.
"""

import io
import os
import json
import struct
import sys
import tempfile
from typing import Any, Callable, Iterator

import numpy as np
import pyarrow as pa

from pyspark.cloudpickle import dumps as cloudpickle_dumps
from pyspark.serializers import write_int, write_long
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)
from pyspark.util import PythonEvalType
from pyspark.worker import main as worker_main

# ---------------------------------------------------------------------------
# Wire-format helpers
# ---------------------------------------------------------------------------


def _write_utf8(s: str, buf: io.BytesIO) -> None:
    """Write a length-prefixed UTF-8 string (matches ``UTF8Deserializer.loads``)."""
    encoded = s.encode("utf-8")
    write_int(len(encoded), buf)
    buf.write(encoded)


def _write_bool(val: bool, buf: io.BytesIO) -> None:
    buf.write(struct.pack("!?", val))


# ---------------------------------------------------------------------------
# Worker protocol builder
# ---------------------------------------------------------------------------


def _build_preamble(buf: io.BytesIO) -> None:
    """Write everything ``main()`` reads before ``eval_type``."""
    write_int(0, buf)  # split_index
    _write_utf8(f"{sys.version_info[0]}.{sys.version_info[1]}", buf)  # python version
    _write_utf8(
        json.dumps(
            {
                "isBarrier": False,
                "stageId": 0,
                "partitionId": 0,
                "attemptNumber": 0,
                "taskAttemptId": 0,
                "cpus": 1,
                "resources": {},
                "localProperties": {},
            }
        ),
        buf,
    )
    _write_utf8("/tmp", buf)  # spark_files_dir
    write_int(0, buf)  # num_python_includes
    _write_bool(False, buf)  # needs_broadcast_decryption_server
    write_int(0, buf)  # num_broadcast_variables


def _build_udf_payload(
    udf_func: Callable[..., Any],
    return_type: StructType,
    arg_offsets: list[int],
    buf: io.BytesIO,
) -> None:
    """Write the ``read_single_udf`` portion of the protocol."""
    write_int(1, buf)  # num_udfs
    write_int(len(arg_offsets), buf)  # num_arg
    for offset in arg_offsets:
        write_int(offset, buf)
        _write_bool(False, buf)  # is_kwarg
    write_int(1, buf)  # num_chained
    command = cloudpickle_dumps((udf_func, return_type))
    write_int(len(command), buf)
    buf.write(command)
    write_long(0, buf)  # result_id


def _write_arrow_ipc_batches(batch_iter: Iterator[pa.RecordBatch], buf: io.BufferedIOBase) -> None:
    """Write a plain Arrow IPC stream from an iterator of Arrow batches."""
    first_batch = next(batch_iter)
    writer = pa.RecordBatchStreamWriter(buf, first_batch.schema)
    writer.write_batch(first_batch)
    for batch in batch_iter:
        writer.write_batch(batch)
    writer.close()


def _write_worker_input(
    eval_type: int,
    write_command: Callable[[io.BufferedIOBase], None],
    write_data: Callable[[io.BufferedIOBase], None],
    buf: io.BufferedIOBase,
) -> None:
    """Write the full worker binary stream: preamble + command + data + end.

    This is the general skeleton shared by all eval types.  Callers provide
    *write_command* (e.g. ``_build_udf_payload``) and *write_data*
    (e.g. ``_write_arrow_ipc_batches``) to plug in protocol specifics.
    """
    _build_preamble(buf)
    write_int(eval_type, buf)
    write_int(0, buf)  # RunnerConf  (0 key-value pairs)
    write_int(0, buf)  # EvalConf    (0 key-value pairs)
    write_command(buf)
    write_data(buf)
    write_int(-4, buf)  # SpecialLengths.END_OF_STREAM


def _run_worker_from_replayed_file(write_input: Callable[[io.BufferedIOBase], None]) -> None:
    """Write input to a temp file, then replay it through ``worker_main``."""
    fd, path = tempfile.mkstemp(prefix="spark-bench-replay-", suffix=".bin")
    try:
        with os.fdopen(fd, "w+b") as infile:
            write_input(infile)
            infile.flush()
            infile.seek(0)
            worker_main(infile, io.BytesIO())
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def _make_typed_batch(rows: int, n_cols: int) -> tuple[pa.RecordBatch, IntegerType]:
    """Columns cycling through int64, string, binary, boolean — reflects realistic serde costs."""
    type_cycle = [
        (lambda r: pa.array(np.random.randint(0, 1000, r, dtype=np.int64)), IntegerType()),
        (lambda r: pa.array([f"s{j}" for j in range(r)]), StringType()),
        (lambda r: pa.array([f"b{j}".encode() for j in range(r)]), BinaryType()),
        (lambda r: pa.array(np.random.choice([True, False], r)), BooleanType()),
    ]
    arrays = [type_cycle[i % len(type_cycle)][0](rows) for i in range(n_cols)]
    fields = [StructField(f"col_{i}", type_cycle[i % len(type_cycle)][1]) for i in range(n_cols)]
    return (
        pa.RecordBatch.from_arrays(arrays, names=[f.name for f in fields]),
        IntegerType(),
    )


def _make_grouped_batch(rows_per_group, n_cols):
    """``group_key (int64)`` + ``(n_cols - 1)`` float32 value columns."""
    arrays = [pa.array(np.zeros(rows_per_group, dtype=np.int64))] + [
        pa.array(np.random.rand(rows_per_group).astype(np.float32)) for _ in range(n_cols - 1)
    ]
    fields = [StructField("group_key", IntegerType())] + [
        StructField(f"val_{i}", DoubleType()) for i in range(n_cols - 1)
    ]
    return (
        pa.RecordBatch.from_arrays(arrays, names=[f.name for f in fields]),
        StructType(fields),
    )


def _make_mixed_batch(rows_per_group):
    """``id``, ``str_col``, ``float_col``, ``double_col``, ``long_col``."""
    arrays = [
        pa.array(np.zeros(rows_per_group, dtype=np.int64)),
        pa.array([f"s{j}" for j in range(rows_per_group)]),
        pa.array(np.random.rand(rows_per_group).astype(np.float32)),
        pa.array(np.random.rand(rows_per_group)),
        pa.array(np.zeros(rows_per_group, dtype=np.int64)),
    ]
    fields = [
        StructField("id", IntegerType()),
        StructField("str_col", StringType()),
        StructField("float_col", DoubleType()),
        StructField("double_col", DoubleType()),
        StructField("long_col", IntegerType()),
    ]
    return (
        pa.RecordBatch.from_arrays(arrays, names=[f.name for f in fields]),
        StructType(fields),
    )


def _build_grouped_arg_offsets(n_cols, n_keys=0):
    """``[len, num_keys, key_col_0, ..., val_col_0, ...]``"""
    keys = list(range(n_keys))
    vals = list(range(n_keys, n_cols))
    offsets = [n_keys] + keys + vals
    return [len(offsets)] + offsets


# ---------------------------------------------------------------------------
# General benchmark base classes
# ---------------------------------------------------------------------------


class _TimeBenchBase:
    """Base for ``time_*`` benchmarks (any eval type).

    Setup materializes full input bytes in memory so that disk I/O does not
    affect latency measurements.

    Subclasses provide ``params``, ``param_names``, and ``_write_scenario``.
    """

    def setup(self, *args):
        buf = io.BytesIO()
        self._write_scenario(*args, buf)
        self._input = buf.getvalue()

    def time_worker(self, *args):
        worker_main(io.BytesIO(self._input), io.BytesIO())


class _PeakmemBenchBase:
    """Base for ``peakmem_*`` benchmarks (any eval type).

    Benchmark streams input to a temp file and replays from disk so that
    setup memory does not inflate peak-memory measurements.

    Subclasses provide ``params``, ``param_names``, and ``_write_scenario``.
    """

    def setup(self, *args):
        self._args = args

    def peakmem_worker(self, *args):
        _run_worker_from_replayed_file(lambda buf: self._write_scenario(*self._args, buf))


# ---------------------------------------------------------------------------
# Non-grouped Arrow UDF benchmarks
# ---------------------------------------------------------------------------

# Data-shape scenarios shared by all non-grouped eval types.
# Each entry maps to a ``(batch, num_batches, col0_type)`` tuple; the UDF is
# selected independently via the ``udf`` ASV parameter.
# ``col0_type`` is the Spark type of column 0, used as the default UDF return
# type when the UDF declaration leaves it as ``None``.


def _make_pure_batch(rows, n_cols, make_array, spark_type):
    """Create a batch where all columns share the same Arrow type."""
    arrays = [make_array(rows) for _ in range(n_cols)]
    fields = [StructField(f"col_{i}", spark_type) for i in range(n_cols)]
    return pa.RecordBatch.from_arrays(arrays, names=[f.name for f in fields])


def _build_non_grouped_scenarios():
    """Build data-shape scenarios for non-grouped Arrow eval types.

    Returns a dict mapping scenario name to ``(batch, num_batches, col0_type)``.
    """
    scenarios = {}

    # Varying batch size and column count (mixed types cycling int/str/bin/bool)
    for name, (rows, n_cols, num_batches) in {
        "sm_batch_few_col": (1_000, 5, 1_500),
        "sm_batch_many_col": (1_000, 50, 200),
        "lg_batch_few_col": (10_000, 5, 3_500),
        "lg_batch_many_col": (10_000, 50, 400),
    }.items():
        batch, col0_type = _make_typed_batch(rows, n_cols)
        scenarios[name] = (batch, num_batches, col0_type)

    # Pure-type scenarios (5000 rows, 10 cols, 1000 batches)
    _PURE_ROWS, _PURE_COLS, _PURE_BATCHES = 5_000, 10, 1_000

    scenarios["pure_ints"] = (
        _make_pure_batch(
            _PURE_ROWS,
            _PURE_COLS,
            lambda r: pa.array(np.random.randint(0, 1000, r, dtype=np.int64)),
            IntegerType(),
        ),
        _PURE_BATCHES,
        IntegerType(),
    )
    scenarios["pure_floats"] = (
        _make_pure_batch(
            _PURE_ROWS,
            _PURE_COLS,
            lambda r: pa.array(np.random.rand(r)),
            DoubleType(),
        ),
        _PURE_BATCHES,
        DoubleType(),
    )
    scenarios["pure_strings"] = (
        _make_pure_batch(
            _PURE_ROWS,
            _PURE_COLS,
            lambda r: pa.array([f"s{j}" for j in range(r)]),
            StringType(),
        ),
        _PURE_BATCHES,
        StringType(),
    )
    scenarios["pure_ts"] = (
        _make_pure_batch(
            _PURE_ROWS,
            _PURE_COLS,
            lambda r: pa.array(
                np.arange(0, r, dtype="datetime64[us]"), type=pa.timestamp("us", tz=None)
            ),
            TimestampNTZType(),
        ),
        _PURE_BATCHES,
        TimestampNTZType(),
    )
    scenarios["mixed_types"] = (
        _make_typed_batch(_PURE_ROWS, _PURE_COLS)[0],
        _PURE_BATCHES,
        IntegerType(),
    )

    return scenarios


# -- SQL_ARROW_BATCHED_UDF --------------------------------------------------
# Arrow-optimized Python UDF: receives individual Python values per row,
# returns a single Python value.  The wire protocol includes an extra
# ``input_type`` (StructType JSON) before the UDF payload.


def _build_arrow_batched_scenarios():
    """Build scenarios for SQL_ARROW_BATCHED_UDF.

    Returns a dict mapping scenario name to
    ``(batch, num_batches, input_struct_type, col0_type)``.
    ``input_struct_type`` is a StructType matching the batch schema,
    needed for the wire protocol.
    """
    scenarios = {}

    # Row-by-row processing is ~100x slower than columnar Arrow UDFs,
    # so batch counts are much smaller to keep benchmarks under 60s.
    for name, (rows, n_cols, num_batches) in {
        "sm_batch_few_col": (1_000, 5, 20),
        "sm_batch_many_col": (1_000, 50, 5),
        "lg_batch_few_col": (10_000, 5, 5),
        "lg_batch_many_col": (10_000, 50, 2),
    }.items():
        batch, col0_type = _make_typed_batch(rows, n_cols)
        type_cycle = [IntegerType(), StringType(), BinaryType(), BooleanType()]
        input_struct = StructType(
            [StructField(f"col_{i}", type_cycle[i % len(type_cycle)]) for i in range(n_cols)]
        )
        scenarios[name] = (batch, num_batches, input_struct, col0_type)

    _PURE_ROWS, _PURE_COLS, _PURE_BATCHES = 5_000, 10, 10

    for scenario_name, make_array, spark_type in [
        (
            "pure_ints",
            lambda r: pa.array(np.random.randint(0, 1000, r, dtype=np.int64)),
            IntegerType(),
        ),
        ("pure_floats", lambda r: pa.array(np.random.rand(r)), DoubleType()),
        ("pure_strings", lambda r: pa.array([f"s{j}" for j in range(r)]), StringType()),
    ]:
        batch = _make_pure_batch(_PURE_ROWS, _PURE_COLS, make_array, spark_type)
        input_struct = StructType([StructField(f"col_{i}", spark_type) for i in range(_PURE_COLS)])
        scenarios[scenario_name] = (batch, _PURE_BATCHES, input_struct, spark_type)

    # mixed types
    batch, col0_type = _make_typed_batch(_PURE_ROWS, _PURE_COLS)
    type_cycle = [IntegerType(), StringType(), BinaryType(), BooleanType()]
    input_struct = StructType(
        [StructField(f"col_{i}", type_cycle[i % len(type_cycle)]) for i in range(_PURE_COLS)]
    )
    scenarios["mixed_types"] = (batch, _PURE_BATCHES, input_struct, col0_type)

    return scenarios


class _ArrowBatchedBenchMixin:
    """Provides ``_write_scenario`` for SQL_ARROW_BATCHED_UDF.

    Writes the extra ``input_type`` (StructType JSON) that the wire protocol
    requires before the UDF payload.
    """

    _scenarios = _build_arrow_batched_scenarios()
    # UDFs for SQL_ARROW_BATCHED_UDF operate on individual Python values.
    # arg_offsets=[0] means the UDF receives column 0 value per row.
    _udfs = {
        "identity_udf": (lambda x: x, None, [0]),
        "stringify_udf": (lambda x: str(x), StringType(), [0]),
        "nullcheck_udf": (lambda x: x is not None, BooleanType(), [0]),
    }
    params = [list(_scenarios), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        batch, num_batches, input_struct, col0_type = self._scenarios[scenario]
        udf_func, ret_type, arg_offsets = self._udfs[udf_name]
        if ret_type is None:
            ret_type = col0_type

        def write_command(b):
            # input_type is read before UDF payloads for ARROW_BATCHED_UDF
            _write_utf8(input_struct.json(), b)
            _build_udf_payload(udf_func, ret_type, arg_offsets, b)

        _write_worker_input(
            PythonEvalType.SQL_ARROW_BATCHED_UDF,
            write_command,
            lambda b: _write_arrow_ipc_batches((batch for _ in range(num_batches)), b),
            buf,
        )


class ArrowBatchedUDFTimeBench(_ArrowBatchedBenchMixin, _TimeBenchBase):
    pass


class ArrowBatchedUDFPeakmemBench(_ArrowBatchedBenchMixin, _PeakmemBenchBase):
    pass


# ---------------------------------------------------------------------------
# Grouped Arrow UDF helpers
# ---------------------------------------------------------------------------


def _write_grouped_arrow_data(
    groups: list[tuple[pa.RecordBatch, ...]],
    num_dfs: int,
    buf: io.BufferedIOBase,
    max_records_per_batch: int | None = None,
) -> None:
    """Write grouped Arrow data in the wire protocol expected by _load_group_dataframes."""
    for group in groups:
        write_int(num_dfs, buf)
        for df_batch in group:
            if max_records_per_batch and df_batch.num_rows > max_records_per_batch:
                sub_batches = [
                    df_batch.slice(offset, max_records_per_batch)
                    for offset in range(0, df_batch.num_rows, max_records_per_batch)
                ]
                _write_arrow_ipc_batches(iter(sub_batches), buf)
            else:
                _write_arrow_ipc_batches(iter([df_batch]), buf)
    write_int(0, buf)  # end of groups


def _make_grouped_arg_offsets(num_key_cols: int, num_value_cols: int) -> list[int]:
    """Build arg_offsets for grouped map UDFs.

    Format: [total_len, num_keys, key_idx..., value_idx...]
    All columns are in a single struct, so key indexes come first,
    then value indexes.
    """
    total = num_key_cols + num_value_cols + 1  # +1 for the num_keys prefix
    key_idxs = list(range(num_key_cols))
    value_idxs = list(range(num_key_cols, num_key_cols + num_value_cols))
    return [total, num_key_cols] + key_idxs + value_idxs


def _make_grouped_batches(
    num_groups: int,
    rows_per_group: int,
    num_key_cols: int,
    num_value_cols: int,
) -> tuple[list[tuple[pa.RecordBatch, ...]], StructType]:
    """Create grouped data: each group is a single batch wrapped in a struct column.

    Returns (groups, return_type) where each group is a 1-tuple of a
    struct-wrapped RecordBatch suitable for ArrowStreamGroupUDFSerializer.
    """
    n_cols = num_key_cols + num_value_cols
    type_cycle = [
        (lambda r: pa.array(np.random.randint(0, 1000, r, dtype=np.int64)), LongType()),
        (lambda r: pa.array([f"s{j}" for j in range(r)]), StringType()),
        (lambda r: pa.array(np.random.choice([True, False], r)), BooleanType()),
        (lambda r: pa.array(np.random.rand(r)), DoubleType()),
    ]

    groups = []
    for g in range(num_groups):
        arrays = [type_cycle[i % len(type_cycle)][0](rows_per_group) for i in range(n_cols)]
        names = [f"col_{i}" for i in range(n_cols)]
        batch = pa.RecordBatch.from_arrays(arrays, names=names)
        # Wrap in struct to match JVM-side encoding (flatten_struct undoes this)
        struct_array = pa.StructArray.from_arrays(batch.columns, names=names)
        wrapped = pa.RecordBatch.from_arrays([struct_array], names=["_0"])
        groups.append((wrapped,))

    value_fields = [
        StructField(f"col_{i}", type_cycle[i % len(type_cycle)][1])
        for i in range(num_key_cols, n_cols)
    ]
    return_type = StructType(value_fields)

    return groups, return_type


# -- SQL_GROUPED_AGG_ARROW_UDF ------------------------------------------------
# UDF receives pa.Array columns per group, returns scalar.


def _grouped_agg_arrow_sum(col):
    """Sum a single Arrow column."""
    import pyarrow.compute as pc

    return pc.sum(col).as_py()


def _grouped_agg_arrow_mean_multi(col0, col1):
    """Mean of two Arrow columns combined."""
    import pyarrow.compute as pc

    return (pc.mean(col0).as_py() or 0) + (pc.mean(col1).as_py() or 0)


# -- SQL_GROUPED_AGG_ARROW_ITER_UDF ------------------------------------------
# UDF receives Iterator[pa.Array] (or Iterator[Tuple[pa.Array, ...]]) per group,
# returns scalar.


def _grouped_agg_arrow_iter_sum(batch_iter):
    """Sum across batches via iterator."""
    import pyarrow.compute as pc

    total = 0
    for col in batch_iter:
        total += pc.sum(col).as_py() or 0
    return total


def _grouped_agg_arrow_iter_mean_multi(batch_iter):
    """Mean across batches of tuples via iterator."""
    import pyarrow.compute as pc

    total = 0.0
    for col0, col1 in batch_iter:
        total += (pc.mean(col0).as_py() or 0) + (pc.mean(col1).as_py() or 0)
    return total


def _make_agg_arrow_groups(
    num_groups: int,
    rows_per_group: int,
    n_cols: int,
) -> list[tuple[pa.RecordBatch, ...]]:
    """Create grouped data for agg Arrow UDFs (plain batches, not struct-wrapped)."""
    type_cycle = [
        lambda r: pa.array(np.random.randint(0, 1000, r, dtype=np.int64)),
        lambda r: pa.array(np.random.rand(r)),
        lambda r: pa.array(np.random.randint(0, 100, r, dtype=np.int64)),
        lambda r: pa.array(np.random.rand(r)),
    ]

    groups = []
    for _ in range(num_groups):
        arrays = [type_cycle[i % len(type_cycle)](rows_per_group) for i in range(n_cols)]
        names = [f"col_{i}" for i in range(n_cols)]
        batch = pa.RecordBatch.from_arrays(arrays, names=names)
        groups.append((batch,))

    return groups


def _build_grouped_agg_arrow_scenarios():
    """Build scenarios for SQL_GROUPED_AGG_ARROW_UDF / AGG_ARROW_ITER_UDF.

    Returns dict mapping name to (groups, n_cols, arg_offsets_map) where
    arg_offsets_map provides offsets per UDF name.
    """
    scenarios = {}

    for name, (num_groups, rows_per_group, n_cols) in {
        "few_groups_sm": (50, 5_000, 5),
        "few_groups_lg": (50, 50_000, 5),
        "many_groups_sm": (2_000, 500, 5),
        "many_groups_lg": (500, 10_000, 5),
        "wide_cols": (200, 5_000, 20),
    }.items():
        groups = _make_agg_arrow_groups(num_groups, rows_per_group, n_cols)
        scenarios[name] = (groups, n_cols)

    return scenarios


class _GroupedAggArrowBenchMixin:
    """Provides _write_scenario for SQL_GROUPED_AGG_ARROW_UDF."""

    _scenarios = _build_grouped_agg_arrow_scenarios()
    _udfs = {
        "sum_udf": _grouped_agg_arrow_sum,
        "mean_multi_udf": _grouped_agg_arrow_mean_multi,
    }
    params = [list(_scenarios), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        groups, n_cols = self._scenarios[scenario]
        udf_func = self._udfs[udf_name]

        # sum_udf uses 1 arg, mean_multi_udf uses 2 args
        if "multi" in udf_name:
            arg_offsets = [0, 1]
        else:
            arg_offsets = [0]

        return_type = DoubleType()

        def write_command(b):
            _build_udf_payload(udf_func, return_type, arg_offsets, b)

        _write_worker_input(
            PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF,
            write_command,
            lambda b: _write_grouped_arrow_data(groups, num_dfs=1, buf=b),
            buf,
        )


class GroupedAggArrowUDFTimeBench(_GroupedAggArrowBenchMixin, _TimeBenchBase):
    pass


class GroupedAggArrowUDFPeakmemBench(_GroupedAggArrowBenchMixin, _PeakmemBenchBase):
    pass


class _GroupedAggArrowIterBenchMixin(_GroupedAggArrowBenchMixin):
    """Provides _write_scenario for SQL_GROUPED_AGG_ARROW_ITER_UDF."""

    _udfs = {
        "sum_udf": _grouped_agg_arrow_iter_sum,
        "mean_multi_udf": _grouped_agg_arrow_iter_mean_multi,
    }
    params = [_GroupedAggArrowBenchMixin.params[0], list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        groups, n_cols = self._scenarios[scenario]
        udf_func = self._udfs[udf_name]

        # sum_udf uses 1 arg, mean_multi_udf uses 2 args
        if "multi" in udf_name:
            arg_offsets = [0, 1]
        else:
            arg_offsets = [0]

        return_type = DoubleType()

        def write_command(b):
            _build_udf_payload(udf_func, return_type, arg_offsets, b)

        _write_worker_input(
            PythonEvalType.SQL_GROUPED_AGG_ARROW_ITER_UDF,
            write_command,
            lambda b: _write_grouped_arrow_data(groups, num_dfs=1, buf=b),
            buf,
        )


class GroupedAggArrowIterUDFTimeBench(_GroupedAggArrowIterBenchMixin, _TimeBenchBase):
    pass


class GroupedAggArrowIterUDFPeakmemBench(_GroupedAggArrowIterBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_GROUPED_MAP_ARROW_UDF ------------------------------------------------
# UDF receives (key: pa.RecordBatch, values: Iterator[pa.RecordBatch]),
# returns Iterator[pa.RecordBatch].


def _grouped_map_arrow_identity(table):
    """Identity grouped map UDF: takes a pa.Table, returns it as-is."""
    return table


def _grouped_map_arrow_sort(table):
    """Sort by first column."""
    return table.sort_by([(table.column_names[0], "ascending")])


def _grouped_map_arrow_filter(table):
    """Filter rows where first column is valid."""
    import pyarrow.compute as pc

    return table.filter(pc.is_valid(table.column(0)))


def _build_grouped_map_arrow_scenarios():
    """Build scenarios for SQL_GROUPED_MAP_ARROW_UDF.

    Returns dict mapping name to (groups, return_type, arg_offsets).
    """
    scenarios = {}

    for name, (num_groups, rows_per_group, num_key_cols, num_value_cols) in {
        "few_groups_sm": (50, 5_000, 1, 4),
        "few_groups_lg": (50, 50_000, 1, 4),
        "many_groups_sm": (2_000, 500, 1, 4),
        "many_groups_lg": (500, 10_000, 1, 4),
        "wide_values": (200, 5_000, 1, 20),
        "multi_key": (200, 5_000, 3, 5),
    }.items():
        groups, return_type = _make_grouped_batches(
            num_groups, rows_per_group, num_key_cols, num_value_cols
        )
        arg_offsets = _make_grouped_arg_offsets(num_key_cols, num_value_cols)
        scenarios[name] = (groups, return_type, arg_offsets)

    return scenarios


class _GroupedMapArrowBenchMixin:
    """Provides _write_scenario for SQL_GROUPED_MAP_ARROW_UDF."""

    _scenarios = _build_grouped_map_arrow_scenarios()
    _udfs = {
        "identity_udf": _grouped_map_arrow_identity,
        "sort_udf": _grouped_map_arrow_sort,
        "filter_udf": _grouped_map_arrow_filter,
    }
    params = [list(_scenarios), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        groups, return_type, arg_offsets = self._scenarios[scenario]
        udf_func = self._udfs[udf_name]

        def write_command(b):
            # Grouped map uses a single UDF with grouped arg_offsets
            write_int(1, b)  # num_udfs
            write_int(len(arg_offsets), b)  # num_arg
            for offset in arg_offsets:
                write_int(offset, b)
                _write_bool(False, b)  # is_kwarg
            write_int(1, b)  # num_chained
            command = cloudpickle_dumps((udf_func, return_type))
            write_int(len(command), b)
            b.write(command)
            write_long(0, b)  # result_id

        _write_worker_input(
            PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF,
            write_command,
            lambda b: _write_grouped_arrow_data(groups, num_dfs=1, buf=b),
            buf,
        )


class GroupedMapArrowUDFTimeBench(_GroupedMapArrowBenchMixin, _TimeBenchBase):
    pass


class GroupedMapArrowUDFPeakmemBench(_GroupedMapArrowBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_GROUPED_MAP_ARROW_ITER_UDF ------------------------------------------
# UDF receives Iterator[pa.RecordBatch] per group,
# returns Iterator[pa.RecordBatch].
# Uses the same wire format and serializer as SQL_GROUPED_MAP_ARROW_UDF.


def _grouped_map_arrow_iter_identity(batches):
    """Identity grouped map iter UDF: yields each batch as-is."""
    yield from batches


def _grouped_map_arrow_iter_sort(batches):
    """Sort each batch by first column."""
    for batch in batches:
        yield batch.sort_by([(batch.column_names[0], "ascending")])


def _grouped_map_arrow_iter_filter(batches):
    """Filter rows where first column is valid."""
    import pyarrow.compute as pc

    for batch in batches:
        yield batch.filter(pc.is_valid(batch.column(0)))


class _GroupedMapArrowIterBenchMixin(_GroupedMapArrowBenchMixin):
    """Provides _write_scenario for SQL_GROUPED_MAP_ARROW_ITER_UDF."""

    _udfs = {
        "identity_udf": _grouped_map_arrow_iter_identity,
        "sort_udf": _grouped_map_arrow_iter_sort,
        "filter_udf": _grouped_map_arrow_iter_filter,
    }
    params = [_GroupedMapArrowBenchMixin.params[0], list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        groups, return_type, arg_offsets = self._scenarios[scenario]
        udf_func = self._udfs[udf_name]

        def write_command(b):
            write_int(1, b)  # num_udfs
            write_int(len(arg_offsets), b)  # num_arg
            for offset in arg_offsets:
                write_int(offset, b)
                _write_bool(False, b)  # is_kwarg
            write_int(1, b)  # num_chained
            command = cloudpickle_dumps((udf_func, return_type))
            write_int(len(command), b)
            b.write(command)
            write_long(0, b)  # result_id

        _write_worker_input(
            PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF,
            write_command,
            lambda b: _write_grouped_arrow_data(groups, num_dfs=1, buf=b),
            buf,
        )


class GroupedMapArrowIterUDFTimeBench(_GroupedMapArrowIterBenchMixin, _TimeBenchBase):
    pass


class GroupedMapArrowIterUDFPeakmemBench(_GroupedMapArrowIterBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_GROUPED_MAP_PANDAS_UDF ------------------------------------------------
# UDF receives a ``pandas.DataFrame`` per group, returns a ``pandas.DataFrame``.
# Groups are sent as separate Arrow IPC streams with optional sub-batching
# (``spark.sql.execution.arrow.maxRecordsPerBatch``).

_MAX_RECORDS_PER_BATCH = 10_000


def _build_grouped_map_pandas_scenarios():
    scenarios = {}

    for name, (rows, n_cols, num_groups, max_rpb) in {
        "sm_grp_few_col": (1_000, 5, 200, None),
        "sm_grp_many_col": (1_000, 50, 30, None),
        "lg_grp_few_col": (100_000, 5, 30, _MAX_RECORDS_PER_BATCH),
        "lg_grp_many_col": (100_000, 50, 5, _MAX_RECORDS_PER_BATCH),
    }.items():
        batch, schema = _make_grouped_batch(rows, n_cols)
        scenarios[name] = (batch, num_groups, schema, max_rpb)

    # mixed column types, small groups
    batch, schema = _make_mixed_batch(3)
    scenarios["mixed_types"] = (batch, 200, schema, None)

    return scenarios


class _GroupedMapPandasBenchMixin:
    """Provides ``_write_scenario`` for SQL_GROUPED_MAP_PANDAS_UDF.

    Each scenario stores ``(batch, num_groups, schema, max_rpb)``.
    Groups are written as separate Arrow IPC streams.
    """

    _scenarios = _build_grouped_map_pandas_scenarios()
    # Each UDF entry: (func, ret_type, n_args).
    # ret_type=None means "use the input schema" (excluding key columns for n_args=2).
    # n_args=1 -> func(pdf), n_args=2 -> func(key, pdf).
    _udfs = {
        "identity_udf": (lambda df: df, None, 1),
        "sort_udf": (lambda df: df.sort_values(df.columns[0]), None, 1),
        "key_identity_udf": (lambda key, df: df, None, 2),
    }
    params = [list(_scenarios), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        batch, num_groups, schema, max_rpb = self._scenarios[scenario]
        udf_func, ret_type, n_args = self._udfs[udf_name]
        if ret_type is None:
            # 2-arg UDFs receive (key, pdf) where pdf excludes key columns,
            # so the return schema must also exclude them.
            ret_type = StructType(schema.fields[n_args - 1 :]) if n_args > 1 else schema
        n_cols = len(schema.fields)
        arg_offsets = _build_grouped_arg_offsets(n_cols, n_keys=n_args - 1)
        _write_worker_input(
            PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
            lambda b: _build_udf_payload(udf_func, ret_type, arg_offsets, b),
            lambda b: _write_grouped_arrow_data(
                [(batch,)] * num_groups,
                num_dfs=1,
                buf=b,
                max_records_per_batch=max_rpb,
            ),
            buf,
        )


class GroupedMapPandasUDFTimeBench(_GroupedMapPandasBenchMixin, _TimeBenchBase):
    pass


class GroupedMapPandasUDFPeakmemBench(_GroupedMapPandasBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_MAP_ARROW_ITER_UDF ------------------------------------------------
# UDF receives ``Iterator[pa.RecordBatch]``, returns ``Iterator[pa.RecordBatch]``.
# Used by ``mapInArrow``.


def _identity_batch_iter(it):
    yield from it


def _sort_batch_iter(it):
    import pyarrow.compute as pc

    for batch in it:
        indices = pc.sort_indices(batch.column(0))
        yield batch.take(indices)


def _filter_batch_iter(it):
    import pyarrow.compute as pc

    for batch in it:
        mask = pc.is_valid(batch.column(0))
        yield batch.filter(mask)


def _build_map_arrow_iter_scenarios():
    """Build scenarios for SQL_MAP_ARROW_ITER_UDF.

    Same data shapes as non-grouped scenarios but with reduced batch counts
    to account for the struct wrap/unwrap overhead per batch.
    """
    scenarios = {}

    for name, (rows, n_cols, num_batches) in {
        "sm_batch_few_col": (1_000, 5, 500),
        "sm_batch_many_col": (1_000, 50, 50),
        "lg_batch_few_col": (10_000, 5, 500),
        "lg_batch_many_col": (10_000, 50, 50),
    }.items():
        batch, col0_type = _make_typed_batch(rows, n_cols)
        scenarios[name] = (batch, num_batches, col0_type)

    _PURE_ROWS, _PURE_COLS, _PURE_BATCHES = 5_000, 10, 200

    for scenario_name, make_array, spark_type in [
        (
            "pure_ints",
            lambda r: pa.array(np.random.randint(0, 1000, r, dtype=np.int64)),
            IntegerType(),
        ),
        ("pure_floats", lambda r: pa.array(np.random.rand(r)), DoubleType()),
        ("pure_strings", lambda r: pa.array([f"s{j}" for j in range(r)]), StringType()),
        (
            "pure_ts",
            lambda r: pa.array(
                np.arange(0, r, dtype="datetime64[us]"), type=pa.timestamp("us", tz=None)
            ),
            TimestampNTZType(),
        ),
    ]:
        batch = _make_pure_batch(_PURE_ROWS, _PURE_COLS, make_array, spark_type)
        scenarios[scenario_name] = (batch, _PURE_BATCHES, spark_type)

    scenarios["mixed_types"] = (
        _make_typed_batch(_PURE_ROWS, _PURE_COLS)[0],
        _PURE_BATCHES,
        IntegerType(),
    )

    return scenarios


def _wrap_batch_in_struct(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Wrap all columns into a single struct column, mimicking JVM-side encoding."""
    struct_array = pa.StructArray.from_arrays(batch.columns, names=batch.schema.names)
    return pa.RecordBatch.from_arrays([struct_array], names=["_0"])


class _MapArrowIterBenchMixin:
    """Provides ``_write_scenario`` for SQL_MAP_ARROW_ITER_UDF.

    Wraps input batches in a struct column to match the JVM-side wire format
    (``flatten_struct`` undoes this).
    """

    _scenarios = _build_map_arrow_iter_scenarios()
    _udfs = {
        "identity_udf": (_identity_batch_iter, None, [0]),
        "sort_udf": (_sort_batch_iter, None, [0]),
        "filter_udf": (_filter_batch_iter, None, [0]),
    }
    params = [list(_scenarios), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        batch, num_batches, col0_type = self._scenarios[scenario]
        udf_func, ret_type, arg_offsets = self._udfs[udf_name]
        if ret_type is None:
            ret_type = col0_type
        wrapped = _wrap_batch_in_struct(batch)
        _write_worker_input(
            PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
            lambda b: _build_udf_payload(udf_func, ret_type, arg_offsets, b),
            lambda b: _write_arrow_ipc_batches((wrapped for _ in range(num_batches)), b),
            buf,
        )


class MapArrowIterUDFTimeBench(_MapArrowIterBenchMixin, _TimeBenchBase):
    pass


class MapArrowIterUDFPeakmemBench(_MapArrowIterBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_SCALAR_ARROW_UDF ---------------------------------------------------
# UDF receives individual ``pa.Array`` columns, returns a ``pa.Array``.
# All UDFs operate on arg_offsets=[0] so they work with any column type.


def _sort_arrow(c):
    import pyarrow.compute as pc

    return pc.take(c, pc.sort_indices(c))


def _nullcheck_arrow(c):
    import pyarrow.compute as pc

    return pc.is_valid(c)


class _ScalarArrowBenchMixin:
    """Mixin for SQL_SCALAR_ARROW_UDF benchmarks."""

    _eval_type = PythonEvalType.SQL_SCALAR_ARROW_UDF
    _scenarios = _build_non_grouped_scenarios()
    # ret_type=None means "use col0_type from the scenario"
    _udfs = {
        "identity_udf": (lambda c: c, None, [0]),
        "sort_udf": (_sort_arrow, None, [0]),
        "nullcheck_udf": (_nullcheck_arrow, BooleanType(), [0]),
    }
    params = [list(_scenarios), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        batch, num_batches, col0_type = self._scenarios[scenario]
        udf_func, ret_type, arg_offsets = self._udfs[udf_name]
        if ret_type is None:
            ret_type = col0_type
        _write_worker_input(
            self._eval_type,
            lambda b: _build_udf_payload(udf_func, ret_type, arg_offsets, b),
            lambda b: _write_arrow_ipc_batches((batch for _ in range(num_batches)), b),
            buf,
        )


class ScalarArrowUDFTimeBench(_ScalarArrowBenchMixin, _TimeBenchBase):
    pass


class ScalarArrowUDFPeakmemBench(_ScalarArrowBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_SCALAR_ARROW_ITER_UDF ----------------------------------------------
# UDF receives ``Iterator[pa.Array]``, returns ``Iterator[pa.Array]``.


def _identity_iter(it):
    return (c for c in it)


def _sort_iter(it):
    import pyarrow.compute as pc

    for c in it:
        yield pc.take(c, pc.sort_indices(c))


def _nullcheck_iter(it):
    import pyarrow.compute as pc

    for c in it:
        yield pc.is_valid(c)


class _ScalarArrowIterBenchMixin(_ScalarArrowBenchMixin):
    """Mixin for SQL_SCALAR_ARROW_ITER_UDF benchmarks."""

    _eval_type = PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF
    _udfs = {
        "identity_udf": (_identity_iter, None, [0]),
        "sort_udf": (_sort_iter, None, [0]),
        "nullcheck_udf": (_nullcheck_iter, BooleanType(), [0]),
    }
    params = [list(_ScalarArrowBenchMixin._scenarios), list(_udfs)]
    param_names = ["scenario", "udf"]


class ScalarArrowIterUDFTimeBench(_ScalarArrowIterBenchMixin, _TimeBenchBase):
    pass


class ScalarArrowIterUDFPeakmemBench(_ScalarArrowIterBenchMixin, _PeakmemBenchBase):
    pass
