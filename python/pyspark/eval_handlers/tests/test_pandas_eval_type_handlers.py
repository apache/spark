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

"""Tests for the pandas eval type handlers (``_pandas``).

The handler tests build handler input in the same wire format the serializers
produce -- a flat RecordBatch, one column per UDF argument -- and assert on the
output batches via ``run(0, <input>)``. Handlers are built on a real ``RunnerConf``
(from a plain conf dict), so the tests exercise the actual conf keys the handler
reads; the forwarding tests pass a single conf key to confirm it reaches the
Arrow<->pandas conversion.
"""

import os
import unittest
from io import BytesIO
from unittest.mock import patch

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.serializers import read_int
from pyspark.sql.pandas.serializers import ArrowStreamSerializer, SpecialLengths
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import PythonEvalType

with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker_util import RunnerConf

if have_pandas and have_pyarrow:
    import pandas as pd
    import pyarrow as pa

    from pyspark.eval_handlers._pandas import PandasScalarUDFHandler

_missing_message = pandas_requirement_message or pyarrow_requirement_message


def _batch(**columns):
    """A RecordBatch of int64 columns, one per ``name=values`` kwarg."""
    return pa.RecordBatch.from_arrays(
        [pa.array(values, type=pa.int64()) for values in columns.values()],
        list(columns),
    )


def _udf(func, return_type=None, args=(0,), kwargs=None):
    """A scalar pandas UDF tuple ``(func, args_offsets, kwargs_offsets, return_type)``."""
    return (func, list(args), dict(kwargs or {}), return_type or LongType())


def _handler(*udfs, runner_conf=None):
    """Build a PandasScalarUDFHandler from one or more ``_udf`` tuples."""
    return PandasScalarUDFHandler(
        udfs=list(udfs), runner_conf=runner_conf or RunnerConf({}), eval_conf=None
    )


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasEvalTypeHandlerRegistrationTests(unittest.TestCase):
    def test_pandas_eval_types_are_registered(self):
        # The migrated pandas eval type dispatches to its handler by lookup.
        self.assertIs(
            get_eval_type_handler(PythonEvalType.SQL_SCALAR_PANDAS_UDF),
            PandasScalarUDFHandler,
        )


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasScalarUDFHandlerTests(unittest.TestCase):
    def test_phase_boundaries_include_arguments_and_validation_but_exclude_iteration(self):
        now = 0
        batch = _batch(a=[1, 2])

        def clock():
            nonlocal now
            # Each timed block spans one millisecond plus any simulated work inside it.
            now += 1_000_000
            return now

        def udf(values):
            nonlocal now
            now += 5_000_000
            return values + 1

        def inputs():
            nonlocal now
            for _ in range(2):
                now += 100_000_000
                yield batch

        handler = _handler(_udf(udf), _udf(udf))
        metrics = handler.metrics
        with patch("pyspark.worker_metrics.time.perf_counter_ns", side_effect=clock):
            for output in handler.run(0, inputs()):
                self.assertEqual(output.column(0).to_pylist(), [2, 3])
                self.assertEqual(output.column(1).to_pylist(), [2, 3])
                now += 1_000_000_000

        self.assertEqual(
            metrics.to_dict(),
            {
                "pythonDataReadTime": 0,
                "pythonInputPreparationTime": 6,
                "pythonUDFExecutionTime": 24,
                "pythonOutputPreparationTime": 6,
                "pythonDataWriteTime": 0,
                "pythonNumTimingReports": 1,
                "pythonNumTimedBatches": 2,
            },
        )

    def test_empty_partition_reports_supported_zero_timings(self):
        handler = _handler(_udf(lambda values: values))
        metrics = handler.metrics
        self.assertEqual(list(handler.run(0, iter(()))), [])
        self.assertEqual(
            metrics.to_dict(),
            {
                "pythonDataReadTime": 0,
                "pythonInputPreparationTime": 0,
                "pythonUDFExecutionTime": 0,
                "pythonOutputPreparationTime": 0,
                "pythonDataWriteTime": 0,
                "pythonNumTimingReports": 1,
                "pythonNumTimedBatches": 0,
            },
        )

    def test_stream_timings_exclude_lazy_udf_execution(self):
        now = 0

        class TimedStream(BytesIO):
            elapsed_ns = 0

            def read(self, size=-1):
                nonlocal now
                now += 1_000_000
                self.elapsed_ns += 1_000_000
                return super().read(size)

            def write(self, data):
                nonlocal now
                now += 2_000_000
                self.elapsed_ns += 2_000_000
                return super().write(data)

        def udf(values):
            nonlocal now
            now += 5_000_000
            return values + 1

        encoded = BytesIO()
        ArrowStreamSerializer().dump_stream(iter([_batch(a=[1, 2])]), encoded)
        input_stream = TimedStream(encoded.getvalue())
        output_stream = TimedStream()
        handler = _handler(_udf(udf))
        serializer = handler.serializer
        with patch("pyspark.worker_metrics.time.perf_counter_ns", side_effect=lambda: now):
            serializer.dump_stream(
                handler.run(0, serializer.load_stream(input_stream)), output_stream
            )

        metrics = handler.metrics.to_dict()
        self.assertGreater(metrics["pythonDataReadTime"], 0)
        self.assertGreater(metrics["pythonDataWriteTime"], 0)
        self.assertEqual(metrics["pythonDataReadTime"], input_stream.elapsed_ns // 1_000_000)
        self.assertEqual(metrics["pythonDataWriteTime"], output_stream.elapsed_ns // 1_000_000)
        self.assertEqual(metrics["pythonUDFExecutionTime"], 5)
        output_stream.seek(0)
        self.assertEqual(read_int(output_stream), SpecialLengths.START_ARROW_STREAM)
        output = list(ArrowStreamSerializer().load_stream(output_stream))
        self.assertEqual(output[0].column(0).to_pylist(), [2, 3])

    def test_invokes_udf_per_batch(self):
        handler = _handler(_udf(lambda s: s + 1))
        out = list(handler.run(0, iter([_batch(a=[1, 2, 3])])))
        self.assertEqual([b.column(0).to_pylist() for b in out], [[2, 3, 4]])

    def test_coerces_output_to_return_type(self):
        # The UDF returns int32, but the declared return type is LongType (int64).
        handler = _handler(_udf(lambda s: (s + 1).astype("int32")))
        out = list(handler.run(0, iter([_batch(a=[10, 20])])))
        self.assertEqual(out[0].schema.field(0).type, pa.int64())
        self.assertEqual(out[0].column(0).to_pylist(), [11, 21])

    def test_multiple_udfs_produce_one_column_each(self):
        handler = _handler(
            _udf(lambda s: s + 1, args=(0,)),
            _udf(lambda s: s * 2, args=(1,)),
        )
        out = list(handler.run(0, iter([_batch(a=[1, 2], b=[10, 20])])))
        self.assertEqual(out[0].column(0).to_pylist(), [2, 3])
        self.assertEqual(out[0].column(1).to_pylist(), [20, 40])

    def test_passes_arg_by_keyword_offset(self):
        # Validates the inline args/kwargs offset handling: y is bound by keyword.
        handler = _handler(_udf(lambda x, y: x - y, args=(0,), kwargs={"y": 1}))
        out = list(handler.run(0, iter([_batch(a=[10, 20], b=[3, 5])])))
        self.assertEqual(out[0].column(0).to_pylist(), [7, 15])

    def test_struct_return_type_takes_dataframe(self):
        # struct_in_pandas="dict" + df_for_struct=True: a struct return is a DataFrame.
        struct_type = StructType([StructField("x", LongType()), StructField("y", LongType())])
        handler = _handler(
            _udf(lambda s: pd.DataFrame({"x": s, "y": s * 10}), return_type=struct_type)
        )
        out = list(handler.run(0, iter([_batch(a=[1, 2])])))
        self.assertEqual(
            out[0].column(0).to_pylist(),
            [{"x": 1, "y": 10}, {"x": 2, "y": 20}],
        )

    def test_rejects_non_sized_result(self):
        handler = _handler(_udf(lambda s: 42))
        with self.assertRaises(PySparkTypeError):
            list(handler.run(0, iter([_batch(a=[1, 2])])))

    def test_rejects_row_count_mismatch(self):
        handler = _handler(_udf(lambda s: s.head(1)))
        with self.assertRaises(PySparkRuntimeError):
            list(handler.run(0, iter([_batch(a=[1, 2, 3])])))

    def test_requires_dataframe_for_struct_return(self):
        struct_type = StructType([StructField("x", LongType())])
        handler = _handler(_udf(lambda s: s, return_type=struct_type))
        with self.assertRaises(PySparkValueError):
            list(handler.run(0, iter([_batch(a=[1, 2])])))

    def test_forwards_prefer_int_ext_dtype_to_input_conversion(self):
        # preferIntExtensionDtype flows to the Arrow->pandas conversion: the UDF sees
        # a nullable "Int64" input instead of the default "int64".
        dtype_name = _udf(lambda s: pd.Series([str(s.dtype)] * len(s)), return_type=StringType())
        conf = RunnerConf({"spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype": "true"})
        out = list(_handler(dtype_name, runner_conf=conf).run(0, iter([_batch(a=[1, 2])])))
        self.assertEqual(out[0].column(0).to_pylist(), ["Int64", "Int64"])

    def test_forwards_use_large_var_types_to_output_conversion(self):
        # useLargeVarTypes flows to the pandas->Arrow conversion: a string result
        # becomes large_string rather than string.
        to_str = _udf(lambda s: s.astype(str), return_type=StringType())
        conf = RunnerConf({"spark.sql.execution.arrow.useLargeVarTypes": "true"})
        out = list(_handler(to_str, runner_conf=conf).run(0, iter([_batch(a=[1, 2])])))
        self.assertTrue(pa.types.is_large_string(out[0].schema.field(0).type))


if __name__ == "__main__":
    from pyspark.testing import main

    main()
