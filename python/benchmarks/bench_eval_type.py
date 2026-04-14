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
# Mock helpers: protocol writer, data factory, UDF factory
# ---------------------------------------------------------------------------


class MockProtocolWriter:
    """Constructs the binary wire protocol that ``worker.py``'s ``main()`` expects."""

    @classmethod
    def write_bool(cls, val: bool, buf: io.BytesIO) -> None:
        buf.write(struct.pack("!?", val))

    @classmethod
    def write_data_payload(
        cls, batch_iter: Iterator[pa.RecordBatch], buf: io.BufferedIOBase
    ) -> None:
        """Write a plain Arrow IPC stream from an iterator of Arrow batches."""
        first_batch = next(batch_iter)
        writer = pa.RecordBatchStreamWriter(buf, first_batch.schema)
        writer.write_batch(first_batch)
        for batch in batch_iter:
            writer.write_batch(batch)
        writer.close()

    @classmethod
    def write_grouped_data_payload(
        cls,
        groups: list[tuple[pa.RecordBatch, ...]],
        num_dfs: int,
        buf: io.BufferedIOBase,
    ) -> None:
        """Write grouped Arrow data in the wire protocol expected by _load_group_dataframes."""
        for group in groups:
            write_int(num_dfs, buf)
            for df_batch in group:
                cls.write_data_payload(iter([df_batch]), buf)
        write_int(0, buf)  # end of groups

    @classmethod
    def write_preamble(cls, buf: io.BytesIO) -> None:
        """Write everything ``main()`` reads before ``eval_type``."""
        write_int(0, buf)  # split_index
        cls.write_utf8(f"{sys.version_info[0]}.{sys.version_info[1]}", buf)  # python version
        cls.write_utf8(
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
        cls.write_utf8("/tmp", buf)  # spark_files_dir
        write_int(0, buf)  # num_python_includes
        cls.write_bool(False, buf)  # needs_broadcast_decryption_server
        write_int(0, buf)  # num_broadcast_variables

    @classmethod
    def write_udf_payload(
        cls,
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
            cls.write_bool(False, buf)  # is_kwarg
        write_int(1, buf)  # num_chained
        command = cloudpickle_dumps((udf_func, return_type))
        write_int(len(command), buf)
        buf.write(command)
        write_long(0, buf)  # result_id

    @classmethod
    def write_utf8(cls, s: str, buf: io.BytesIO) -> None:
        """Write a length-prefixed UTF-8 string (matches ``UTF8Deserializer.loads``)."""
        encoded = s.encode("utf-8")
        write_int(len(encoded), buf)
        buf.write(encoded)

    @classmethod
    def write_worker_input(
        cls,
        eval_type: int,
        write_udf: Callable[[io.BufferedIOBase], None],
        write_data: Callable[[io.BufferedIOBase], None],
        buf: io.BufferedIOBase,
        runner_conf: dict[str, str] | None = None,
    ) -> None:
        """Write the full worker binary stream: preamble + command + data + end."""
        cls.write_preamble(buf)
        write_int(eval_type, buf)
        if runner_conf:
            write_int(len(runner_conf), buf)
            for k, v in runner_conf.items():
                cls.write_utf8(k, buf)
                cls.write_utf8(v, buf)
        else:
            write_int(0, buf)  # RunnerConf  (0 key-value pairs)
        write_int(0, buf)  # EvalConf    (0 key-value pairs)
        write_udf(buf)
        write_data(buf)
        write_int(-4, buf)  # SpecialLengths.END_OF_STREAM


class MockDataFactory:
    """Creates mock Arrow batches and group structures for benchmarks."""

    MAX_RECORDS_PER_BATCH = 10_000

    TYPE_REGISTRY: dict[str, tuple[Callable, Any]] = {
        "int": (lambda r: pa.array(np.random.randint(0, 1000, r, dtype=np.int32)), IntegerType()),
        "double": (lambda r: pa.array(np.random.rand(r)), DoubleType()),
        "string": (lambda r: pa.array([f"s{j}" for j in range(r)]), StringType()),
        "binary": (lambda r: pa.array([f"b{j}".encode() for j in range(r)]), BinaryType()),
        "boolean": (lambda r: pa.array(np.random.choice([True, False], r)), BooleanType()),
    }

    MIXED_TYPES = [
        TYPE_REGISTRY["int"],
        TYPE_REGISTRY["string"],
        TYPE_REGISTRY["binary"],
        TYPE_REGISTRY["boolean"],
    ]
    NUMERIC_TYPES = [TYPE_REGISTRY["int"], TYPE_REGISTRY["double"]]

    NAMED_TYPE_POOLS: dict[str, list[tuple[Callable, Any]]] = {
        "mixed": MIXED_TYPES,
        "pure_ints": [
            (lambda r: pa.array(np.random.randint(0, 1000, r, dtype=np.int64)), IntegerType())
        ],
        "pure_floats": [(lambda r: pa.array(np.random.rand(r)), DoubleType())],
        "pure_strings": [(lambda r: pa.array([f"s{j}" for j in range(r)]), StringType())],
        "pure_ts": [
            (
                lambda r: pa.array(
                    np.arange(0, r, dtype="datetime64[us]"),
                    type=pa.timestamp("us", tz=None),
                ),
                TimestampNTZType(),
            )
        ],
    }

    @classmethod
    def make_struct_type(
        cls,
        *,
        num_fields: int,
        base_types: list[tuple[Callable, Any]],
    ) -> tuple[Callable, StructType]:
        """Compose flat ``base_types`` into a single struct type pool entry.

        Returns ``(factory_fn, StructType)`` suitable for inclusion in a
        ``spark_type_pool`` list.  The factory produces a ``pa.StructArray`` whose
        sub-fields cycle through ``base_types``.
        """
        fields = [
            StructField(f"col_{i}", base_types[i % len(base_types)][1]) for i in range(num_fields)
        ]

        def factory(r):
            arrays = [base_types[i % len(base_types)][0](r) for i in range(num_fields)]
            names = [f.name for f in fields]
            return pa.StructArray.from_arrays(arrays, names=names)

        return (factory, StructType(fields))

    @classmethod
    def make_batches(
        cls,
        *,
        num_rows: int,
        num_cols: int,
        batch_size: int = MAX_RECORDS_PER_BATCH,
        spark_type_pool: list[tuple[Callable, Any]],
    ) -> tuple[list[pa.RecordBatch], StructType]:
        """Create RecordBatches with columns cycling through ``spark_type_pool``.

        Splits ``num_rows`` into batches of at most ``batch_size`` rows.
        Each pool entry is ``(factory_fn, SparkType)``; if an entry produces
        a ``pa.StructArray``, it becomes a struct column naturally.
        """
        batches = []
        for offset in range(0, num_rows, batch_size):
            rows = min(batch_size, num_rows - offset)
            arrays = [spark_type_pool[i % len(spark_type_pool)][0](rows) for i in range(num_cols)]
            names = [f"col_{i}" for i in range(num_cols)]
            batches.append(pa.RecordBatch.from_arrays(arrays, names=names))
        schema = StructType(
            [
                StructField(f"col_{i}", spark_type_pool[i % len(spark_type_pool)][1])
                for i in range(num_cols)
            ]
        )
        return batches, schema

    @classmethod
    def make_grouped_batches(
        cls,
        *,
        num_groups: int,
        num_rows: int,
        num_cols: int,
        batch_size: int = MAX_RECORDS_PER_BATCH,
        spark_type_pool: list[tuple[Callable, Any]],
    ) -> tuple[list[tuple[pa.RecordBatch, ...]], StructType]:
        """Create groups of batches.

        Each group has ``num_rows`` total rows, split into batches of ``batch_size``.
        """
        groups = []
        for _ in range(num_groups):
            batches, _ = cls.make_batches(
                num_rows=num_rows,
                num_cols=num_cols,
                batch_size=batch_size,
                spark_type_pool=spark_type_pool,
            )
            groups.append(tuple(batches))

        schema = StructType(
            [
                StructField(f"col_{i}", spark_type_pool[i % len(spark_type_pool)][1])
                for i in range(num_cols)
            ]
        )
        return groups, schema

    @classmethod
    def make_cogrouped_batches(
        cls,
        *,
        num_groups: int,
        num_rows: int,
        num_cols: int,
        batch_size: int = MAX_RECORDS_PER_BATCH,
        spark_type_pool: list[tuple[Callable, Any]],
    ) -> tuple[list[tuple[pa.RecordBatch, pa.RecordBatch]], StructType]:
        """Create cogroups of batch pairs (left, right).

        Each cogroup has two DataFrames with identical schema but independent
        data, each with ``num_rows`` rows and ``num_cols`` flat columns.
        """
        left_groups, schema = cls.make_grouped_batches(
            num_groups=num_groups,
            num_rows=num_rows,
            num_cols=num_cols,
            batch_size=batch_size,
            spark_type_pool=spark_type_pool,
        )
        right_groups, _ = cls.make_grouped_batches(
            num_groups=num_groups,
            num_rows=num_rows,
            num_cols=num_cols,
            batch_size=batch_size,
            spark_type_pool=spark_type_pool,
        )
        cogroups = [(left_groups[i][0], right_groups[i][0]) for i in range(num_groups)]
        return cogroups, schema


class MockUDFFactory:
    """Constructs UDF command payloads for the worker protocol."""

    @classmethod
    def make_grouped_arg_offsets(cls, num_key_cols: int, num_value_cols: int) -> list[int]:
        """Build arg_offsets: ``[total_len, num_keys, key_idx..., value_idx...]``."""
        key_idxs = list(range(num_key_cols))
        value_idxs = list(range(num_key_cols, num_key_cols + num_value_cols))
        offsets = [num_key_cols] + key_idxs + value_idxs
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
        fd, path = tempfile.mkstemp(prefix="spark-bench-replay-", suffix=".bin")
        with os.fdopen(fd, "wb") as f:
            self._write_scenario(*args, f)
        self._replay_path = path

    def teardown(self, *args):
        try:
            os.remove(self._replay_path)
        except (FileNotFoundError, AttributeError):
            pass

    def peakmem_worker(self, *args):
        with open(self._replay_path, "rb") as infile:
            worker_main(infile, io.BytesIO())


# -- SQL_ARROW_BATCHED_UDF --------------------------------------------------
# UDF receives individual Python values per row, returns a single Python value.


class _ArrowBatchedBenchMixin:
    """Provides ``_write_scenario`` for SQL_ARROW_BATCHED_UDF.

    Writes the extra ``input_type`` (StructType JSON) that the wire protocol
    requires before the UDF payload.
    """

    # Row-by-row processing is ~100x slower than columnar Arrow UDFs,
    # so batch counts are much smaller to keep benchmarks under 60s.
    _scenario_configs = {
        "sm_batch_few_col": ("mixed", 20_000, 5, 1_000),
        "sm_batch_many_col": ("mixed", 5_000, 50, 1_000),
        "lg_batch_few_col": ("mixed", 50_000, 5, 10_000),
        "lg_batch_many_col": ("mixed", 20_000, 50, 10_000),
        "pure_ints": ("pure_ints", 50_000, 10, 5_000),
        "pure_floats": ("pure_floats", 50_000, 10, 5_000),
        "pure_strings": ("pure_strings", 50_000, 10, 5_000),
        "mixed_types": ("mixed", 50_000, 10, 5_000),
    }

    @staticmethod
    def _build_scenario(name):
        """Build a single scenario by name."""
        np.random.seed(42)
        type_key, num_rows, num_cols, batch_size = _ArrowBatchedBenchMixin._scenario_configs[name]
        pool = MockDataFactory.NAMED_TYPE_POOLS[type_key]
        return MockDataFactory.make_batches(
            num_rows=num_rows,
            num_cols=num_cols,
            spark_type_pool=pool,
            batch_size=batch_size,
        )

    # UDFs for SQL_ARROW_BATCHED_UDF operate on individual Python values.
    # arg_offsets=[0] means the UDF receives column 0 value per row.
    _udfs = {
        "identity_udf": (lambda x: x, None, [0]),
        "stringify_udf": (lambda x: str(x), StringType(), [0]),
        "nullcheck_udf": (lambda x: x is not None, BooleanType(), [0]),
    }
    params = [list(_scenario_configs), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        batches, schema = self._build_scenario(scenario)
        udf_func, ret_type, arg_offsets = self._udfs[udf_name]
        if ret_type is None:
            ret_type = schema.fields[0].dataType

        def write_udf(b):
            MockProtocolWriter.write_utf8(schema.json(), b)
            MockProtocolWriter.write_udf_payload(udf_func, ret_type, arg_offsets, b)

        MockProtocolWriter.write_worker_input(
            PythonEvalType.SQL_ARROW_BATCHED_UDF,
            write_udf,
            lambda b: MockProtocolWriter.write_data_payload(iter(batches), b),
            buf,
        )


class ArrowBatchedUDFTimeBench(_ArrowBatchedBenchMixin, _TimeBenchBase):
    pass


class ArrowBatchedUDFPeakmemBench(_ArrowBatchedBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_COGROUPED_MAP_ARROW_UDF ------------------------------------------------
# UDF receives two ``pa.Table`` (left, right) per co-group, returns ``pa.Table``.


class _CogroupedMapArrowBenchMixin:
    """Provides _write_scenario for SQL_COGROUPED_MAP_ARROW_UDF."""

    def _cogrouped_map_arrow_identity(left, right):
        """Identity cogroup UDF: returns left table as-is."""
        return left

    def _cogrouped_map_arrow_concat(left, right):
        """Concat cogroup UDF: vertically concatenates left and right tables."""
        import pyarrow as pa

        return pa.concat_tables([left, right])

    def _cogrouped_map_arrow_left_semi(left, right):
        """Left-semi cogroup UDF: filters left rows whose key exists in right."""
        key_col = left.column_names[0]
        return left.join(right.select([key_col]), keys=key_col, join_type="left semi")

    _scenario_configs = {
        "few_groups_sm": (50, 5_000, 1, 4),
        "few_groups_lg": (50, 50_000, 1, 4),
        "many_groups_sm": (2_000, 500, 1, 4),
        "many_groups_lg": (500, 10_000, 1, 4),
        "wide_values": (200, 5_000, 1, 20),
        "multi_key": (200, 5_000, 3, 5),
    }

    @staticmethod
    def _build_scenario(name):
        """Build a cogroup scenario: two DataFrames with the same grouping structure.

        Unlike grouped map (which wraps columns in a struct), cogroup batches
        have flat columns: [key_col_0, ..., key_col_k, val_col_0, ..., val_col_v].
        """
        np.random.seed(42)
        num_groups, rows_per_group, num_key_cols, num_value_cols = (
            _CogroupedMapArrowBenchMixin._scenario_configs[name]
        )
        n_cols = num_key_cols + num_value_cols
        type_pool = MockDataFactory.MIXED_TYPES[:n_cols]
        while len(type_pool) < n_cols:
            type_pool = type_pool + MockDataFactory.MIXED_TYPES[: n_cols - len(type_pool)]

        cogroups, schema = MockDataFactory.make_cogrouped_batches(
            num_groups=num_groups,
            num_rows=rows_per_group,
            num_cols=n_cols,
            spark_type_pool=type_pool,
            batch_size=rows_per_group,
        )
        return_type = StructType(schema.fields[num_key_cols:])
        return (cogroups, return_type, num_key_cols, num_value_cols)

    _udfs = {
        "identity_udf": _cogrouped_map_arrow_identity,
        "concat_udf": _cogrouped_map_arrow_concat,
        "left_semi_udf": _cogrouped_map_arrow_left_semi,
    }
    params = [list(_scenario_configs), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        groups, schema, num_key_cols, num_value_cols = self._build_scenario(scenario)
        udf_func = self._udfs[udf_name]
        left_offsets = MockUDFFactory.make_grouped_arg_offsets(num_key_cols, num_value_cols)
        right_offsets = MockUDFFactory.make_grouped_arg_offsets(num_key_cols, num_value_cols)
        arg_offsets = left_offsets + right_offsets
        MockProtocolWriter.write_worker_input(
            PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF,
            lambda b: MockProtocolWriter.write_udf_payload(udf_func, schema, arg_offsets, b),
            lambda b: MockProtocolWriter.write_grouped_data_payload(groups, num_dfs=2, buf=b),
            buf,
        )


class CogroupedMapArrowUDFTimeBench(_CogroupedMapArrowBenchMixin, _TimeBenchBase):
    pass


class CogroupedMapArrowUDFPeakmemBench(_CogroupedMapArrowBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_GROUPED_AGG_ARROW_UDF ------------------------------------------------
# UDF receives ``pa.Array`` columns per group, returns scalar.


class _GroupedAggArrowBenchMixin:
    """Provides _write_scenario for SQL_GROUPED_AGG_ARROW_UDF."""

    def _grouped_agg_arrow_sum(col):
        """Sum a single Arrow column."""
        import pyarrow.compute as pc

        return pc.sum(col).as_py()

    def _grouped_agg_arrow_mean_multi(col0, col1):
        """Mean of two Arrow columns combined."""
        import pyarrow.compute as pc

        return (pc.mean(col0).as_py() or 0) + (pc.mean(col1).as_py() or 0)

    _scenario_configs = {
        "few_groups_sm": (50, 5_000, 5),
        "few_groups_lg": (50, 50_000, 5),
        "many_groups_sm": (2_000, 500, 5),
        "many_groups_lg": (500, 10_000, 5),
        "wide_cols": (200, 5_000, 20),
    }

    @staticmethod
    def _build_scenario(name):
        """Build a single scenario by name."""
        np.random.seed(42)
        num_groups, rows_per_group, n_cols = _GroupedAggArrowBenchMixin._scenario_configs[name]
        return MockDataFactory.make_grouped_batches(
            num_groups=num_groups,
            num_rows=rows_per_group,
            num_cols=n_cols,
            spark_type_pool=MockDataFactory.NUMERIC_TYPES,
            batch_size=rows_per_group,
        )

    _udfs = {
        "sum_udf": _grouped_agg_arrow_sum,
        "mean_multi_udf": _grouped_agg_arrow_mean_multi,
    }
    params = [list(_scenario_configs), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        groups, _schema = self._build_scenario(scenario)
        udf_func = self._udfs[udf_name]

        # sum_udf uses 1 arg, mean_multi_udf uses 2 args
        if "multi" in udf_name:
            arg_offsets = [0, 1]
        else:
            arg_offsets = [0]

        return_type = DoubleType()

        def write_udf(b):
            MockProtocolWriter.write_udf_payload(udf_func, return_type, arg_offsets, b)

        MockProtocolWriter.write_worker_input(
            PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF,
            write_udf,
            lambda b: MockProtocolWriter.write_grouped_data_payload(groups, num_dfs=1, buf=b),
            buf,
        )


class GroupedAggArrowUDFTimeBench(_GroupedAggArrowBenchMixin, _TimeBenchBase):
    pass


class GroupedAggArrowUDFPeakmemBench(_GroupedAggArrowBenchMixin, _PeakmemBenchBase):
    pass


class _GroupedAggArrowIterBenchMixin(_GroupedAggArrowBenchMixin):
    """Provides _write_scenario for SQL_GROUPED_AGG_ARROW_ITER_UDF."""

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

    _udfs = {
        "sum_udf": _grouped_agg_arrow_iter_sum,
        "mean_multi_udf": _grouped_agg_arrow_iter_mean_multi,
    }
    params = [list(_GroupedAggArrowBenchMixin._scenario_configs), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        groups, _schema = self._build_scenario(scenario)
        udf_func = self._udfs[udf_name]

        # sum_udf uses 1 arg, mean_multi_udf uses 2 args
        if "multi" in udf_name:
            arg_offsets = [0, 1]
        else:
            arg_offsets = [0]

        return_type = DoubleType()

        def write_udf(b):
            MockProtocolWriter.write_udf_payload(udf_func, return_type, arg_offsets, b)

        MockProtocolWriter.write_worker_input(
            PythonEvalType.SQL_GROUPED_AGG_ARROW_ITER_UDF,
            write_udf,
            lambda b: MockProtocolWriter.write_grouped_data_payload(groups, num_dfs=1, buf=b),
            buf,
        )


class GroupedAggArrowIterUDFTimeBench(_GroupedAggArrowIterBenchMixin, _TimeBenchBase):
    pass


class GroupedAggArrowIterUDFPeakmemBench(_GroupedAggArrowIterBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_GROUPED_MAP_ARROW_UDF ------------------------------------------------
# UDF receives ``pa.Table``, returns ``pa.Table``.


class _GroupedMapArrowBenchMixin:
    """Provides _write_scenario for SQL_GROUPED_MAP_ARROW_UDF."""

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

    _scenario_configs = {
        "few_groups_sm": (50, 5_000, 1, 4),
        "few_groups_lg": (50, 50_000, 1, 4),
        "many_groups_sm": (2_000, 500, 1, 4),
        "many_groups_lg": (500, 10_000, 1, 4),
        "wide_values": (200, 5_000, 1, 20),
        "multi_key": (200, 5_000, 3, 5),
    }

    @staticmethod
    def _build_scenario(name):
        """Build a single scenario by name."""
        np.random.seed(42)
        num_groups, rows_per_group, num_key_cols, num_value_cols = (
            _GroupedMapArrowBenchMixin._scenario_configs[name]
        )
        n_fields = num_key_cols + num_value_cols
        struct_type = MockDataFactory.make_struct_type(
            num_fields=n_fields,
            base_types=MockDataFactory.MIXED_TYPES,
        )
        groups, schema = MockDataFactory.make_grouped_batches(
            num_groups=num_groups,
            num_rows=rows_per_group,
            num_cols=1,
            spark_type_pool=[struct_type],
            batch_size=rows_per_group,
        )
        inner_fields = schema.fields[0].dataType.fields
        return_type = StructType(inner_fields[num_key_cols:])
        return (groups, return_type)

    _udfs = {
        "identity_udf": _grouped_map_arrow_identity,
        "sort_udf": _grouped_map_arrow_sort,
        "filter_udf": _grouped_map_arrow_filter,
    }
    params = [list(_scenario_configs), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        groups, schema = self._build_scenario(scenario)
        udf_func = self._udfs[udf_name]
        n_total = groups[0][0].column(0).type.num_fields
        n_values = len(schema.fields)
        num_key_cols = n_total - n_values
        arg_offsets = MockUDFFactory.make_grouped_arg_offsets(num_key_cols, n_values)
        MockProtocolWriter.write_worker_input(
            PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF,
            lambda b: MockProtocolWriter.write_udf_payload(udf_func, schema, arg_offsets, b),
            lambda b: MockProtocolWriter.write_grouped_data_payload(groups, num_dfs=1, buf=b),
            buf,
        )


class GroupedMapArrowUDFTimeBench(_GroupedMapArrowBenchMixin, _TimeBenchBase):
    pass


class GroupedMapArrowUDFPeakmemBench(_GroupedMapArrowBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_GROUPED_MAP_ARROW_ITER_UDF ------------------------------------------
# UDF receives ``Iterator[pa.RecordBatch]`` per group, returns ``Iterator[pa.RecordBatch]``.


class _GroupedMapArrowIterBenchMixin(_GroupedMapArrowBenchMixin):
    """Provides _write_scenario for SQL_GROUPED_MAP_ARROW_ITER_UDF."""

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

    _udfs = {
        "identity_udf": _grouped_map_arrow_iter_identity,
        "sort_udf": _grouped_map_arrow_iter_sort,
        "filter_udf": _grouped_map_arrow_iter_filter,
    }
    params = [list(_GroupedMapArrowBenchMixin._scenario_configs), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        groups, schema = self._build_scenario(scenario)
        udf_func = self._udfs[udf_name]
        n_total = groups[0][0].column(0).type.num_fields
        n_values = len(schema.fields)
        num_key_cols = n_total - n_values
        arg_offsets = MockUDFFactory.make_grouped_arg_offsets(num_key_cols, n_values)
        MockProtocolWriter.write_worker_input(
            PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF,
            lambda b: MockProtocolWriter.write_udf_payload(udf_func, schema, arg_offsets, b),
            lambda b: MockProtocolWriter.write_grouped_data_payload(groups, num_dfs=1, buf=b),
            buf,
        )


class GroupedMapArrowIterUDFTimeBench(_GroupedMapArrowIterBenchMixin, _TimeBenchBase):
    pass


class GroupedMapArrowIterUDFPeakmemBench(_GroupedMapArrowIterBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_GROUPED_MAP_PANDAS_UDF ------------------------------------------------
# UDF receives ``pandas.DataFrame`` per group, returns ``pandas.DataFrame``.


class _GroupedMapPandasBenchMixin:
    """Provides ``_write_scenario`` for SQL_GROUPED_MAP_PANDAS_UDF.

    Each scenario stores ``(groups, schema)``.
    Groups are written as separate Arrow IPC streams.
    """

    _scenario_configs = {
        "sm_grp_few_col": ("numeric", 1_000, 5, 200),
        "sm_grp_many_col": ("numeric", 1_000, 50, 30),
        "lg_grp_few_col": ("numeric", 100_000, 5, 30),
        "lg_grp_many_col": ("numeric", 100_000, 50, 5),
        "mixed_types": ("mixed", None, None, None),
    }

    @staticmethod
    def _build_scenario(name):
        """Build a single scenario by name."""
        np.random.seed(42)
        cfg = _GroupedMapPandasBenchMixin._scenario_configs[name]
        if cfg[0] == "mixed":
            batches, schema = MockDataFactory.make_batches(
                num_rows=3,
                num_cols=5,
                spark_type_pool=MockDataFactory.MIXED_TYPES,
                batch_size=3,
            )
            return ([(b,) for b in batches] * 200, schema)
        _kind, rows, n_cols, num_groups = cfg
        groups, schema = MockDataFactory.make_grouped_batches(
            num_groups=num_groups,
            num_rows=rows,
            num_cols=n_cols,
            spark_type_pool=MockDataFactory.NUMERIC_TYPES,
        )
        return (groups, schema)

    # Each UDF entry: (func, ret_type, n_args).
    # ret_type=None means "use the input schema" (excluding key columns for n_args=2).
    # n_args=1 -> func(pdf), n_args=2 -> func(key, pdf).
    _udfs = {
        "identity_udf": (lambda df: df, None, 1),
        "sort_udf": (lambda df: df.sort_values(df.columns[0]), None, 1),
        "key_identity_udf": (lambda key, df: df, None, 2),
    }
    params = [list(_scenario_configs), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        groups, schema = self._build_scenario(scenario)
        udf_func, ret_type, n_args = self._udfs[udf_name]
        if ret_type is None:
            # 2-arg UDFs receive (key, pdf) where pdf excludes key columns,
            # so the return schema must also exclude them.
            ret_type = StructType(schema.fields[n_args - 1 :]) if n_args > 1 else schema
        n_cols = len(schema.fields)
        arg_offsets = MockUDFFactory.make_grouped_arg_offsets(n_args - 1, n_cols - (n_args - 1))
        MockProtocolWriter.write_worker_input(
            PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
            lambda b: MockProtocolWriter.write_udf_payload(udf_func, ret_type, arg_offsets, b),
            lambda b: MockProtocolWriter.write_grouped_data_payload(
                groups,
                num_dfs=1,
                buf=b,
            ),
            buf,
        )


class GroupedMapPandasUDFTimeBench(_GroupedMapPandasBenchMixin, _TimeBenchBase):
    pass


class GroupedMapPandasUDFPeakmemBench(_GroupedMapPandasBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_MAP_ARROW_ITER_UDF ------------------------------------------------
# UDF receives ``Iterator[pa.RecordBatch]``, returns ``Iterator[pa.RecordBatch]``.


class _MapArrowIterBenchMixin:
    """Provides ``_write_scenario`` for SQL_MAP_ARROW_ITER_UDF.

    Wraps input batches in a struct column to match the JVM-side wire format
    (``flatten_struct`` undoes this).
    """

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

    _scenario_configs = {
        "sm_batch_few_col": ("mixed", 500_000, 5, 1_000),
        "sm_batch_many_col": ("mixed", 50_000, 50, 1_000),
        "lg_batch_few_col": ("mixed", 5_000_000, 5, 10_000),
        "lg_batch_many_col": ("mixed", 500_000, 50, 10_000),
        "pure_ints": ("pure_ints", 1_000_000, 10, 5_000),
        "pure_floats": ("pure_floats", 1_000_000, 10, 5_000),
        "pure_strings": ("pure_strings", 1_000_000, 10, 5_000),
        "pure_ts": ("pure_ts", 1_000_000, 10, 5_000),
        "mixed_types": ("mixed", 1_000_000, 10, 5_000),
    }

    @staticmethod
    def _build_scenario(name):
        """Build a single scenario by name."""
        np.random.seed(42)
        type_key, num_rows, num_cols, batch_size = _MapArrowIterBenchMixin._scenario_configs[name]
        pool = MockDataFactory.NAMED_TYPE_POOLS[type_key]
        struct_type = MockDataFactory.make_struct_type(
            num_fields=num_cols,
            base_types=pool,
        )
        return MockDataFactory.make_batches(
            num_rows=num_rows,
            num_cols=1,
            spark_type_pool=[struct_type],
            batch_size=batch_size,
        )

    _udfs = {
        "identity_udf": (_identity_batch_iter, None, [0]),
        "sort_udf": (_sort_batch_iter, None, [0]),
        "filter_udf": (_filter_batch_iter, None, [0]),
    }
    params = [list(_scenario_configs), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        batches, schema = self._build_scenario(scenario)
        udf_func, ret_type, arg_offsets = self._udfs[udf_name]
        if ret_type is None:
            ret_type = schema.fields[0].dataType.fields[0].dataType
        MockProtocolWriter.write_worker_input(
            PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
            lambda b: MockProtocolWriter.write_udf_payload(udf_func, ret_type, arg_offsets, b),
            lambda b: MockProtocolWriter.write_data_payload(iter(batches), b),
            buf,
        )


class MapArrowIterUDFTimeBench(_MapArrowIterBenchMixin, _TimeBenchBase):
    pass


class MapArrowIterUDFPeakmemBench(_MapArrowIterBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_SCALAR_ARROW_UDF ---------------------------------------------------
# UDF receives ``pa.Array`` columns, returns ``pa.Array``.


class _ScalarArrowBenchMixin:
    """Mixin for SQL_SCALAR_ARROW_UDF benchmarks."""

    def _sort_arrow(c):
        import pyarrow.compute as pc

        return pc.take(c, pc.sort_indices(c))

    def _nullcheck_arrow(c):
        import pyarrow.compute as pc

        return pc.is_valid(c)

    _scenario_configs = {
        "sm_batch_few_col": ("mixed", 1_500_000, 5, 1_000),
        "sm_batch_many_col": ("mixed", 200_000, 50, 1_000),
        "lg_batch_few_col": ("mixed", 35_000_000, 5, 10_000),
        "lg_batch_many_col": ("mixed", 4_000_000, 50, 10_000),
        "pure_ints": ("pure_ints", 5_000_000, 10, 5_000),
        "pure_floats": ("pure_floats", 5_000_000, 10, 5_000),
        "pure_strings": ("pure_strings", 5_000_000, 10, 5_000),
        "pure_ts": ("pure_ts", 5_000_000, 10, 5_000),
        "mixed_types": ("mixed", 5_000_000, 10, 5_000),
    }

    @staticmethod
    def _build_scenario(name):
        """Build a single scenario by name."""
        np.random.seed(42)
        type_key, num_rows, num_cols, batch_size = _ScalarArrowBenchMixin._scenario_configs[name]
        pool = MockDataFactory.NAMED_TYPE_POOLS[type_key]
        return MockDataFactory.make_batches(
            num_rows=num_rows,
            num_cols=num_cols,
            spark_type_pool=pool,
            batch_size=batch_size,
        )

    _eval_type = PythonEvalType.SQL_SCALAR_ARROW_UDF
    # ret_type=None means "use schema.fields[0].dataType from the scenario"
    _udfs = {
        "identity_udf": (lambda c: c, None, [0]),
        "sort_udf": (_sort_arrow, None, [0]),
        "nullcheck_udf": (_nullcheck_arrow, BooleanType(), [0]),
    }
    params = [list(_scenario_configs), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        batches, schema = self._build_scenario(scenario)
        udf_func, ret_type, arg_offsets = self._udfs[udf_name]
        if ret_type is None:
            ret_type = schema.fields[0].dataType
        MockProtocolWriter.write_worker_input(
            self._eval_type,
            lambda b: MockProtocolWriter.write_udf_payload(udf_func, ret_type, arg_offsets, b),
            lambda b: MockProtocolWriter.write_data_payload(iter(batches), b),
            buf,
        )


class ScalarArrowUDFTimeBench(_ScalarArrowBenchMixin, _TimeBenchBase):
    pass


class ScalarArrowUDFPeakmemBench(_ScalarArrowBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_SCALAR_ARROW_ITER_UDF ----------------------------------------------
# UDF receives ``Iterator[pa.Array]``, returns ``Iterator[pa.Array]``.


class _ScalarArrowIterBenchMixin(_ScalarArrowBenchMixin):
    """Mixin for SQL_SCALAR_ARROW_ITER_UDF benchmarks."""

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

    _eval_type = PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF
    _udfs = {
        "identity_udf": (_identity_iter, None, [0]),
        "sort_udf": (_sort_iter, None, [0]),
        "nullcheck_udf": (_nullcheck_iter, BooleanType(), [0]),
    }
    params = [list(_ScalarArrowBenchMixin._scenario_configs), list(_udfs)]
    param_names = ["scenario", "udf"]


class ScalarArrowIterUDFTimeBench(_ScalarArrowIterBenchMixin, _TimeBenchBase):
    pass


class ScalarArrowIterUDFPeakmemBench(_ScalarArrowIterBenchMixin, _PeakmemBenchBase):
    pass


# -- SQL_WINDOW_AGG_ARROW_UDF ------------------------------------------------
# UDF receives ``pa.Array`` columns for the entire window partition, returns scalar.


class _WindowAggArrowBenchMixin:
    """Provides _write_scenario for SQL_WINDOW_AGG_ARROW_UDF."""

    def _window_agg_arrow_sum(col):
        """Sum a single Arrow column."""
        import pyarrow.compute as pc

        return pc.sum(col).as_py()

    def _window_agg_arrow_mean_multi(col0, col1):
        """Mean of two Arrow columns combined."""
        import pyarrow.compute as pc

        return (pc.mean(col0).as_py() or 0) + (pc.mean(col1).as_py() or 0)

    def _build_scenarios():
        """Build scenarios for SQL_WINDOW_AGG_ARROW_UDF.

        Returns a dict mapping scenario name to ``(groups, schema)``.
        """
        scenarios = {}

        for name, (num_groups, rows_per_group, n_cols) in {
            "few_groups_sm": (50, 5_000, 5),
            "few_groups_lg": (50, 50_000, 5),
            "many_groups_sm": (2_000, 500, 5),
            "many_groups_lg": (500, 10_000, 5),
            "wide_cols": (200, 5_000, 20),
        }.items():
            groups, schema = MockDataFactory.make_grouped_batches(
                num_groups=num_groups,
                num_rows=rows_per_group,
                num_cols=n_cols,
                spark_type_pool=MockDataFactory.NUMERIC_TYPES,
                batch_size=rows_per_group,
            )
            scenarios[name] = (groups, schema)

        return scenarios

    _scenarios = _build_scenarios()
    _udfs = {
        "sum_udf": _window_agg_arrow_sum,
        "mean_multi_udf": _window_agg_arrow_mean_multi,
    }
    params = [list(_scenarios), list(_udfs)]
    param_names = ["scenario", "udf"]

    def _write_scenario(self, scenario, udf_name, buf):
        groups, _schema = self._scenarios[scenario]
        udf_func = self._udfs[udf_name]

        # sum_udf uses 1 arg, mean_multi_udf uses 2 args
        if "multi" in udf_name:
            arg_offsets = [0, 1]
        else:
            arg_offsets = [0]

        return_type = DoubleType()

        def write_udf(b):
            MockProtocolWriter.write_udf_payload(udf_func, return_type, arg_offsets, b)

        MockProtocolWriter.write_worker_input(
            PythonEvalType.SQL_WINDOW_AGG_ARROW_UDF,
            write_udf,
            lambda b: MockProtocolWriter.write_grouped_data_payload(groups, num_dfs=1, buf=b),
            buf,
            runner_conf={"window_bound_types": "unbounded"},
        )


class WindowAggArrowUDFTimeBench(_WindowAggArrowBenchMixin, _TimeBenchBase):
    pass


class WindowAggArrowUDFPeakmemBench(_WindowAggArrowBenchMixin, _PeakmemBenchBase):
    pass
