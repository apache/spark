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
import json
import struct
import sys

import numpy as np
import pyarrow as pa

from pyspark.cloudpickle import dumps as cloudpickle_dumps
from pyspark.serializers import write_int, write_long
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.util import PythonEvalType
from pyspark.worker import main as worker_main


# ---------------------------------------------------------------------------
# Wire-format helpers
# ---------------------------------------------------------------------------


def _write_utf8(s, buf):
    """Write a length-prefixed UTF-8 string (matches ``UTF8Deserializer.loads``)."""
    encoded = s.encode("utf-8")
    write_int(len(encoded), buf)
    buf.write(encoded)


def _write_bool(val, buf):
    buf.write(struct.pack("!?", val))


# ---------------------------------------------------------------------------
# Worker protocol builder
# ---------------------------------------------------------------------------


def _build_preamble(buf):
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


def _build_udf_payload(udf_func, return_type, arg_offsets, buf):
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


def _build_grouped_arrow_data(arrow_batch, num_groups, buf, max_records_per_batch=None):
    """Write grouped-map Arrow data: ``(write_int(1) + IPC) * N + write_int(0)``.

    When *max_records_per_batch* is set and the batch exceeds that limit, each
    group is split into multiple smaller Arrow batches inside the same IPC stream,
    mirroring what the JVM does under ``spark.sql.execution.arrow.maxRecordsPerBatch``.
    """
    for _ in range(num_groups):
        write_int(1, buf)
        writer = pa.RecordBatchStreamWriter(buf, arrow_batch.schema)
        if max_records_per_batch and arrow_batch.num_rows > max_records_per_batch:
            for offset in range(0, arrow_batch.num_rows, max_records_per_batch):
                writer.write_batch(arrow_batch.slice(offset, max_records_per_batch))
        else:
            writer.write_batch(arrow_batch)
        writer.close()
    write_int(0, buf)


def _build_worker_input(
    eval_type,
    udf_func,
    return_type,
    arg_offsets,
    arrow_batch,
    num_groups,
    max_records_per_batch=None,
):
    """Assemble the full binary stream consumed by ``worker_main(infile, outfile)``.

    Parameters
    ----------
    max_records_per_batch : int, optional
        When set, each group's Arrow data is split into sub-batches of this
        size, mirroring the JVM-side behaviour of
        ``spark.sql.execution.arrow.maxRecordsPerBatch``.
    """
    buf = io.BytesIO()

    _build_preamble(buf)
    write_int(eval_type, buf)

    write_int(0, buf)  # RunnerConf  (0 key-value pairs)
    write_int(0, buf)  # EvalConf    (0 key-value pairs)

    _build_udf_payload(udf_func, return_type, arg_offsets, buf)
    _build_grouped_arrow_data(arrow_batch, num_groups, buf, max_records_per_batch)
    write_int(-4, buf)  # SpecialLengths.END_OF_STREAM

    return buf.getvalue()


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------


def _build_grouped_arg_offsets(n_cols, n_keys=0):
    """``[len, num_keys, key_col_0, …, val_col_0, …]``"""
    keys = list(range(n_keys))
    vals = list(range(n_keys, n_cols))
    offsets = [n_keys] + keys + vals
    return [len(offsets)] + offsets


def _make_grouped_batch(rows_per_group, n_cols):
    """``group_key (int64)`` + ``(n_cols - 1)`` float32 value columns."""
    arrays = [pa.array(np.zeros(rows_per_group, dtype=np.int64))] + [
        pa.array(np.random.rand(rows_per_group).astype(np.float32)) for _ in range(n_cols - 1)
    ]
    fields = [StructField("group_key", IntegerType())] + [
        StructField(f"some_field_{i}", DoubleType()) for i in range(n_cols - 1)
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


# ---------------------------------------------------------------------------
# SQL_GROUPED_MAP_PANDAS_UDF
# ---------------------------------------------------------------------------


class GroupedMapPandasUDFBench:
    """Full worker round-trip for ``SQL_GROUPED_MAP_PANDAS_UDF``.

    Large groups (100k rows) are split into Arrow sub-batches of at most
    ``_MAX_RECORDS_PER_BATCH`` rows, mirroring the JVM-side splitting
    behaviour (``spark.sql.execution.arrow.maxRecordsPerBatch`` default 10 000).
    Small groups (1k rows) are unaffected.
    """

    _MAX_RECORDS_PER_BATCH = 10_000  # matches spark.sql.execution.arrow.maxRecordsPerBatch default

    def setup(self):
        eval_type = PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF

        # ---- varying group size (float data, identity UDF) ----
        for name, (rows_per_group, n_cols, num_groups) in {
            "small_few": (1_000, 5, 1_500),
            "small_many": (1_000, 50, 200),
            "large_few": (100_000, 5, 350),
            "large_many": (100_000, 50, 40),
        }.items():
            batch, schema = _make_grouped_batch(rows_per_group, n_cols)
            setattr(
                self,
                f"_{name}_input",
                _build_worker_input(
                    eval_type,
                    lambda df: df,
                    schema,
                    _build_grouped_arg_offsets(n_cols),
                    batch,
                    num_groups=num_groups,
                    max_records_per_batch=self._MAX_RECORDS_PER_BATCH,
                ),
            )

        # ---- mixed types, 1-arg UDF ----
        mixed_batch, mixed_schema = _make_mixed_batch(3)
        n_mixed = len(mixed_schema.fields)

        def double_col_add_mean(pdf):
            return pdf.assign(double_col=pdf["double_col"] + pdf["double_col"].mean())

        self._mixed_input = _build_worker_input(
            eval_type,
            double_col_add_mean,
            mixed_schema,
            _build_grouped_arg_offsets(n_mixed),
            mixed_batch,
            num_groups=1_300,
        )

        # ---- mixed types, 2-arg UDF with key ----
        two_arg_schema = StructType(
            [StructField("group_key", IntegerType()), StructField("double_col_mean", DoubleType())]
        )

        def double_col_key_add_mean(key, pdf):
            import pandas as pd

            return pd.DataFrame(
                [{"group_key": key[0], "double_col_mean": pdf["double_col"].mean()}]
            )

        self._two_args_input = _build_worker_input(
            eval_type,
            double_col_key_add_mean,
            two_arg_schema,
            _build_grouped_arg_offsets(n_mixed, n_keys=1),
            mixed_batch,
            num_groups=1_600,
        )

    # -- benchmarks ---------------------------------------------------------

    def _run(self, input_bytes):
        worker_main(io.BytesIO(input_bytes), io.BytesIO())

    def time_small_groups_few_cols(self):
        """1k rows/group, 5 cols, 1500 groups."""
        self._run(self._small_few_input)

    def peakmem_small_groups_few_cols(self):
        """1k rows/group, 5 cols, 1500 groups."""
        self._run(self._small_few_input)

    def time_small_groups_many_cols(self):
        """1k rows/group, 50 cols, 200 groups."""
        self._run(self._small_many_input)

    def peakmem_small_groups_many_cols(self):
        """1k rows/group, 50 cols, 200 groups."""
        self._run(self._small_many_input)

    def time_large_groups_few_cols(self):
        """100k rows/group, 5 cols, 350 groups, split at 10k rows/batch."""
        self._run(self._large_few_input)

    def peakmem_large_groups_few_cols(self):
        """100k rows/group, 5 cols, 350 groups, split at 10k rows/batch."""
        self._run(self._large_few_input)

    def time_large_groups_many_cols(self):
        """100k rows/group, 50 cols, 40 groups, split at 10k rows/batch."""
        self._run(self._large_many_input)

    def peakmem_large_groups_many_cols(self):
        """100k rows/group, 50 cols, 40 groups, split at 10k rows/batch."""
        self._run(self._large_many_input)

    def time_mixed_types(self):
        """Mixed column types, 1-arg UDF, 3 rows/group, 1300 groups."""
        self._run(self._mixed_input)

    def peakmem_mixed_types(self):
        """Mixed column types, 1-arg UDF, 3 rows/group, 1300 groups."""
        self._run(self._mixed_input)

    def time_mixed_types_two_args(self):
        """Mixed column types, 2-arg UDF with key, 3 rows/group, 1600 groups."""
        self._run(self._two_args_input)

    def peakmem_mixed_types_two_args(self):
        """Mixed column types, 2-arg UDF with key, 3 rows/group, 1600 groups."""
        self._run(self._two_args_input)
