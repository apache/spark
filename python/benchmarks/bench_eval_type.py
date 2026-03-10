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


_NON_GROUPED_SCENARIOS = _build_non_grouped_scenarios()


class _NonGroupedBenchMixin:
    """Provides ``_write_scenario`` for non-grouped Arrow eval types.

    Subclasses set ``_eval_type``, ``_scenarios``, and ``_udfs``.
    UDF entries with ``ret_type=None`` inherit ``col0_type`` from the scenario.
    """

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


_ARROW_BATCHED_SCENARIOS = _build_arrow_batched_scenarios()


# UDFs for SQL_ARROW_BATCHED_UDF operate on individual Python values.
# arg_offsets=[0] means the UDF receives column 0 value per row.
_ARROW_BATCHED_UDFS = {
    "identity_udf": (lambda x: x, None, [0]),
    "stringify_udf": (lambda x: str(x), StringType(), [0]),
    "nullcheck_udf": (lambda x: x is not None, BooleanType(), [0]),
}


class _ArrowBatchedBenchMixin:
    """Provides ``_write_scenario`` for SQL_ARROW_BATCHED_UDF.

    Like ``_NonGroupedBenchMixin`` but writes the extra ``input_type``
    (StructType JSON) that the wire protocol requires.
    """

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
    _scenarios = _ARROW_BATCHED_SCENARIOS
    _udfs = _ARROW_BATCHED_UDFS
    params = [list(_ARROW_BATCHED_SCENARIOS), list(_ARROW_BATCHED_UDFS)]
    param_names = ["scenario", "udf"]


class ArrowBatchedUDFPeakmemBench(_ArrowBatchedBenchMixin, _PeakmemBenchBase):
    _scenarios = _ARROW_BATCHED_SCENARIOS
    _udfs = _ARROW_BATCHED_UDFS
    params = [list(_ARROW_BATCHED_SCENARIOS), list(_ARROW_BATCHED_UDFS)]
    param_names = ["scenario", "udf"]


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


_MAP_ARROW_ITER_UDFS = {
    "identity_udf": (_identity_batch_iter, None, [0]),
    "sort_udf": (_sort_batch_iter, None, [0]),
    "filter_udf": (_filter_batch_iter, None, [0]),
}


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


_MAP_ARROW_ITER_SCENARIOS = _build_map_arrow_iter_scenarios()


def _wrap_batch_in_struct(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Wrap all columns into a single struct column, mimicking JVM-side encoding."""
    struct_array = pa.StructArray.from_arrays(batch.columns, names=batch.schema.names)
    return pa.RecordBatch.from_arrays([struct_array], names=["_0"])


class _MapArrowIterBenchMixin:
    """Provides ``_write_scenario`` for SQL_MAP_ARROW_ITER_UDF.

    Like ``_NonGroupedBenchMixin`` but wraps input batches in a struct column
    to match the JVM-side wire format (``flatten_struct`` undoes this).
    """

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
    _scenarios = _MAP_ARROW_ITER_SCENARIOS
    _udfs = _MAP_ARROW_ITER_UDFS
    params = [list(_MAP_ARROW_ITER_SCENARIOS), list(_MAP_ARROW_ITER_UDFS)]
    param_names = ["scenario", "udf"]


class MapArrowIterUDFPeakmemBench(_MapArrowIterBenchMixin, _PeakmemBenchBase):
    _scenarios = _MAP_ARROW_ITER_SCENARIOS
    _udfs = _MAP_ARROW_ITER_UDFS
    params = [list(_MAP_ARROW_ITER_SCENARIOS), list(_MAP_ARROW_ITER_UDFS)]
    param_names = ["scenario", "udf"]


# -- SQL_SCALAR_ARROW_UDF ---------------------------------------------------
# UDF receives individual ``pa.Array`` columns, returns a ``pa.Array``.
# All UDFs operate on arg_offsets=[0] so they work with any column type.


def _sort_arrow(c):
    import pyarrow.compute as pc

    return pc.take(c, pc.sort_indices(c))


def _nullcheck_arrow(c):
    import pyarrow.compute as pc

    return pc.is_valid(c)


# ret_type=None means "use col0_type from the scenario"
_SCALAR_ARROW_UDFS = {
    "identity_udf": (lambda c: c, None, [0]),
    "sort_udf": (_sort_arrow, None, [0]),
    "nullcheck_udf": (_nullcheck_arrow, BooleanType(), [0]),
}


class ScalarArrowUDFTimeBench(_NonGroupedBenchMixin, _TimeBenchBase):
    _eval_type = PythonEvalType.SQL_SCALAR_ARROW_UDF
    _scenarios = _NON_GROUPED_SCENARIOS
    _udfs = _SCALAR_ARROW_UDFS
    params = [list(_NON_GROUPED_SCENARIOS), list(_SCALAR_ARROW_UDFS)]
    param_names = ["scenario", "udf"]


class ScalarArrowUDFPeakmemBench(_NonGroupedBenchMixin, _PeakmemBenchBase):
    _eval_type = PythonEvalType.SQL_SCALAR_ARROW_UDF
    _scenarios = _NON_GROUPED_SCENARIOS
    _udfs = _SCALAR_ARROW_UDFS
    params = [list(_NON_GROUPED_SCENARIOS), list(_SCALAR_ARROW_UDFS)]
    param_names = ["scenario", "udf"]


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


_SCALAR_ARROW_ITER_UDFS = {
    "identity_udf": (_identity_iter, None, [0]),
    "sort_udf": (_sort_iter, None, [0]),
    "nullcheck_udf": (_nullcheck_iter, BooleanType(), [0]),
}


class ScalarArrowIterUDFTimeBench(_NonGroupedBenchMixin, _TimeBenchBase):
    _eval_type = PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF
    _scenarios = _NON_GROUPED_SCENARIOS
    _udfs = _SCALAR_ARROW_ITER_UDFS
    params = [list(_NON_GROUPED_SCENARIOS), list(_SCALAR_ARROW_ITER_UDFS)]
    param_names = ["scenario", "udf"]


class ScalarArrowIterUDFPeakmemBench(_NonGroupedBenchMixin, _PeakmemBenchBase):
    _eval_type = PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF
    _scenarios = _NON_GROUPED_SCENARIOS
    _udfs = _SCALAR_ARROW_ITER_UDFS
    params = [list(_NON_GROUPED_SCENARIOS), list(_SCALAR_ARROW_ITER_UDFS)]
    param_names = ["scenario", "udf"]
