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

"""Tests for the Arrow eval type handlers (``_arrow``).

Each handler has one test class. The helpers below build handler input in the
same wire format the serializers produce, so every test constructs its input the
same way: ``run(0, <input>)`` and assert on the output batches.
"""

import os
import unittest
from io import BytesIO
from unittest.mock import patch

from pyspark.errors import PySparkRuntimeError
from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.eval_handlers._typing import UDF
from pyspark.eval_handlers.utils import hashable_grouping_key
from pyspark.sql.pandas.serializers import ArrowStreamCoGroupSerializer, ArrowStreamSerializer
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.testing.utils import PySparkBaseTestCase, have_pyarrow, pyarrow_requirement_message
from pyspark.util import PythonEvalType
from pyspark.worker_metrics import WorkerMetrics

with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker_util import EvalConf, RunnerConf

if have_pyarrow:
    import pyarrow as pa

    from pyspark.eval_handlers._arrow import (
        ArrowBatchedUDFArrowHandler,
        ArrowCoGroupedMapUDFHandler,
        ArrowGroupedAggIncrementalFinalUDFHandler,
        ArrowGroupedAggIncrementalPartialUDFHandler,
        ArrowGroupedAggIterUDFHandler,
        ArrowGroupedAggUDFHandler,
        ArrowGroupedMapIterUDFHandler,
        ArrowGroupedMapUDFHandler,
        ArrowMapUDFHandler,
        ArrowScalarIterUDFHandler,
        ArrowScalarUDFHandler,
        ArrowWindowAggIncrementalUDFHandler,
        ArrowWindowAggUDFHandler,
    )
    from pyspark.sql.conversion import ArrowBatchTransformer


class _SumAggregator:
    """Incremental ``Aggregator`` stub: sums non-null inputs, with a ``(sum, count)`` buffer.

    ``reduce_calls`` counts ``reduce`` invocations so tests can assert how a window frame was
    folded (extended vs refolded from ``zero``).
    """

    bufferSchema = StructType([StructField("s", LongType()), StructField("c", LongType())])

    def __init__(self):
        self.reduce_calls = 0

    def zero(self):
        return (0, 0)

    def reduce(self, buffer, values):
        self.reduce_calls += 1
        (v,) = values
        return buffer if v is None else (buffer[0] + v, buffer[1] + 1)

    def merge(self, left, right):
        return (left[0] + right[0], left[1] + right[1])

    def finish(self, buffer):
        return buffer[0]


class ArrowEvalTypeHandlerTestCase(PySparkBaseTestCase):
    """Builders for handler input in the wire format the serializers produce."""

    _RETURN_TYPE = StructType([StructField("v", LongType())])

    @staticmethod
    def _batch(**columns):
        """A RecordBatch of int64 columns, one per ``name=values`` kwarg."""
        return pa.RecordBatch.from_arrays(
            [pa.array(values, type=pa.int64()) for values in columns.values()],
            list(columns),
        )

    @classmethod
    def _struct_batch(cls, **columns):
        """``_batch`` wrapped into a single struct column (the grouped/map wire format)."""
        return ArrowBatchTransformer.wrap_struct(cls._batch(**columns))

    @staticmethod
    def _one_group(*batches):
        """One group of the given batches, shaped as the group serializer yields it."""
        return iter([iter(batches)])

    @staticmethod
    def _grouped_arg_offsets(*dataframes):
        """Encode ``arg_offsets`` from ``(key_cols, value_cols)`` per DataFrame.

        Mirrors PandasGroupUtils.resolveArgOffsets: each DataFrame is laid out as
        ``[length, num_keys, *key_cols, *value_cols]``.
        """
        offsets: list = []
        for key_cols, value_cols in dataframes:
            group = [len(key_cols), *key_cols, *value_cols]
            offsets += [len(group), *group]
        return offsets

    @staticmethod
    def _udf(func, args_offsets, kwargs_offsets=None, return_type=None):
        """A ``UDF`` record as the worker builds it."""
        return UDF(
            func=func,
            args_offsets=args_offsets,
            kwargs_offsets=kwargs_offsets or {},
            return_type=return_type or LongType(),
            result_id=0,
        )

    @classmethod
    def _grouped_handler(cls, handler_cls, udf, arg_offsets):
        """Build a grouped/cogrouped-map handler with the shared return type. The function the
        handler wraps loses its signature, so the handler reads it from the record's unwrapped
        ``func``."""
        return handler_cls(
            udfs=[cls._udf(udf, arg_offsets, return_type=cls._RETURN_TYPE)],
            runner_conf=RunnerConf({}),
            eval_conf=None,
        )


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowEvalTypeHandlerRegistrationTests(PySparkBaseTestCase):
    def test_arrow_eval_types_are_registered(self):
        # Every migrated Arrow eval type dispatches to its handler by lookup.
        for eval_type, handler_cls in (
            (PythonEvalType.SQL_SCALAR_ARROW_UDF, ArrowScalarUDFHandler),
            (PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF, ArrowScalarIterUDFHandler),
            (PythonEvalType.SQL_MAP_ARROW_ITER_UDF, ArrowMapUDFHandler),
            (PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF, ArrowGroupedMapUDFHandler),
            (PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF, ArrowGroupedMapIterUDFHandler),
            (PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF, ArrowCoGroupedMapUDFHandler),
            (PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF, ArrowGroupedAggUDFHandler),
            (PythonEvalType.SQL_GROUPED_AGG_ARROW_ITER_UDF, ArrowGroupedAggIterUDFHandler),
            (PythonEvalType.SQL_WINDOW_AGG_ARROW_UDF, ArrowWindowAggUDFHandler),
            (
                PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_PARTIAL_UDF,
                ArrowGroupedAggIncrementalPartialUDFHandler,
            ),
            (
                PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_FINAL_UDF,
                ArrowGroupedAggIncrementalFinalUDFHandler,
            ),
            (
                PythonEvalType.SQL_WINDOW_AGG_ARROW_INCREMENTAL_UDF,
                ArrowWindowAggIncrementalUDFHandler,
            ),
        ):
            self.assertIs(get_eval_type_handler(eval_type, RunnerConf({})), handler_cls)

    def test_arrow_batched_udf_arrow_variant_is_registered(self):
        # SQL_ARROW_BATCHED_UDF picks its variant by the legacy pandas conversion conf; the
        # Arrow variant serves the default and the explicit "false".
        legacy_conf = "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled"
        for conf in ({}, {legacy_conf: "false"}):
            with self.subTest(conf=conf):
                self.assertIs(
                    get_eval_type_handler(PythonEvalType.SQL_ARROW_BATCHED_UDF, RunnerConf(conf)),
                    ArrowBatchedUDFArrowHandler,
                )


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowBatchedUDFArrowHandlerTests(ArrowEvalTypeHandlerTestCase):
    _INPUT_TYPE = StructType([StructField("a", LongType()), StructField("b", LongType())])

    def _handler(self, *udfs, runner_conf=None, input_type=None):
        """Build the handler from ``(func, args_offsets, kwargs_offsets, return_type)`` tuples."""
        return ArrowBatchedUDFArrowHandler(
            udfs=[self._udf(*u) for u in udfs],
            runner_conf=runner_conf or RunnerConf({}),
            eval_conf=EvalConf({"input_type": (input_type or self._INPUT_TYPE).json()}),
        )

    def test_serializer(self):
        handler = self._handler((lambda a: a, [0], {}, LongType()))
        self.assertIsInstance(handler.serializer, ArrowStreamSerializer)

    def test_detailed_timing_uses_handler_gate(self):
        metrics = WorkerMetrics()
        self.addCleanup(metrics.reset)
        timing_conf = "spark.sql.execution.pythonUDF.detailedTiming.enabled"
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                metrics.reset()
                handler = self._handler(
                    (lambda a: a + 1, [0], {}, LongType()),
                    (lambda a, b: a * b, [0, 1], {}, LongType()),
                    runner_conf=RunnerConf({timing_conf: str(enabled).lower()}),
                )
                self.assertEqual(handler.serializer._collect_timing, enabled)
                output = list(
                    handler.run(
                        0,
                        iter([self._batch(a=[1, 2], b=[10, 20]), self._batch(a=[3], b=[4])]),
                    )
                )
                self.assertEqual([batch.num_rows for batch in output], [2, 1])
                report = metrics.to_dict()
                if enabled:
                    self.assertEqual(report["pythonNumTimingReports"], 1)
                    self.assertEqual(report["pythonNumTimedBatches"], 2)
                    for name in (
                        "pythonInputPreparationTime",
                        "pythonUDFExecutionTime",
                        "pythonOutputPreparationTime",
                    ):
                        self.assertIn(name, report)
                else:
                    self.assertNotIn("pythonNumTimingReports", report)
                    self.assertNotIn("pythonInputPreparationTime", report)

    def test_phase_timing_boundaries(self):
        metrics = WorkerMetrics()
        metrics.reset()
        self.addCleanup(metrics.reset)
        clock_ns = 0
        udf_rows = 0
        input_ns = 0
        output_ns = 0

        class TimedBatch:
            def __init__(self, batch):
                self.batch = batch

            @property
            def num_rows(self):
                return self.batch.num_rows

            def itercolumns(self):
                nonlocal clock_ns, input_ns
                clock_ns += 3_000_000
                input_ns += 3_000_000
                return self.batch.itercolumns()

        class TimedInt:
            def __init__(self, value):
                self.value = value

            def __index__(self):
                nonlocal clock_ns, output_ns
                clock_ns += 5_000_000
                output_ns += 5_000_000
                return self.value

        def udf(value):
            nonlocal clock_ns, udf_rows
            clock_ns += 100_000_000
            udf_rows += 1
            return TimedInt(value + 1)

        class OutputStream(BytesIO):
            write_ns = 0
            rows_at_write = None

            def write(self, data):
                nonlocal clock_ns
                if self.rows_at_write is None:
                    self.rows_at_write = udf_rows
                clock_ns += 5_000_000
                self.write_ns += 5_000_000
                return super().write(data)

        handler = self._handler(
            (udf, [0], {}, LongType()),
            runner_conf=RunnerConf(
                {"spark.sql.execution.pythonUDF.detailedTiming.enabled": "true"}
            ),
        )
        output = OutputStream()
        batches = [self._batch(a=[1, 2], b=[10, 20]), self._batch(a=[3], b=[30])]

        def input_batches():
            nonlocal clock_ns
            for batch in batches:
                clock_ns += 100_000_000
                yield TimedBatch(batch)

        with patch("pyspark.eval_handlers._arrow.time.perf_counter_ns", new=lambda: clock_ns):
            handler.serializer.dump_stream(handler.run(0, input_batches()), output)

        report = metrics.to_dict()
        self.assertEqual(output.rows_at_write, 2)
        self.assertEqual(report["pythonNumTimedBatches"], 2)
        self.assertGreater(input_ns, 0)
        self.assertEqual(report["pythonInputPreparationTime"], input_ns // 1_000_000)
        self.assertEqual(report["pythonUDFExecutionTime"], 300)
        self.assertGreater(output_ns, 0)
        self.assertEqual(report["pythonOutputPreparationTime"], output_ns // 1_000_000)
        self.assertEqual(report["pythonDataWriteTime"], output.write_ns // 1_000_000)

    def test_invokes_udf_per_row(self):
        handler = self._handler(
            (lambda a: a + 1, [0], {}, LongType()),
            (lambda a, b: a * b, [0, 1], {}, LongType()),
        )
        out = list(
            handler.run(0, iter([self._batch(a=[1, 2], b=[10, 20]), self._batch(a=[3], b=[4])]))
        )
        self.assertEqual([b.schema.names for b in out], [["_0", "_1"], ["_0", "_1"]])
        self.assertEqual(
            [b.to_pydict() for b in out],
            [{"_0": [2, 3], "_1": [10, 40]}, {"_0": [4], "_1": [12]}],
        )

    def test_passes_arg_by_keyword_offset(self):
        handler = self._handler((lambda x, y: x - y, [0], {"y": 1}, LongType()))
        out = list(handler.run(0, iter([self._batch(a=[10, 20], b=[3, 5])])))
        self.assertEqual(out[0].column(0).to_pylist(), [7, 15])

    def test_zero_arg_udf(self):
        handler = self._handler((lambda: 7, [], {}, LongType()), input_type=StructType())
        empty = pa.RecordBatch.from_pydict({"a": [1, 2, 3]}).select([])
        out = list(handler.run(0, iter([empty])))
        self.assertEqual(out[0].column(0).to_pylist(), [7, 7, 7])

    def test_none_result_is_null(self):
        handler = self._handler((lambda a: None if a == 2 else a, [0], {}, LongType()))
        out = list(handler.run(0, iter([self._batch(a=[1, 2, 3], b=[0, 0, 0])])))
        self.assertEqual(out[0].column(0).to_pylist(), [1, None, 3])

    def test_converts_result_to_return_type(self):
        # Results are converted directly to the declared Arrow type, without the legacy
        # str coercion: an int result for a StringType UDF is cast to its string form.
        handler = self._handler((lambda a: a, [0], {}, StringType()))
        out = list(handler.run(0, iter([self._batch(a=[1, 2], b=[0, 0])])))
        self.assertEqual(out[0].schema.field(0).type, pa.string())
        self.assertEqual(out[0].column(0).to_pylist(), ["1", "2"])

    def test_forwards_use_large_var_types_to_output_conversion(self):
        conf = RunnerConf({"spark.sql.execution.arrow.useLargeVarTypes": "true"})
        handler = self._handler((lambda a: str(a), [0], {}, StringType()), runner_conf=conf)
        out = list(handler.run(0, iter([self._batch(a=[1], b=[0])])))
        self.assertTrue(pa.types.is_large_string(out[0].schema.field(0).type))

    def test_concurrency_level(self):
        conf = RunnerConf({"spark.sql.execution.pythonUDF.arrow.concurrency.level": "4"})
        handler = self._handler((lambda a: a * 2, [0], {}, LongType()), runner_conf=conf)
        out = list(handler.run(0, iter([self._batch(a=list(range(10)), b=[0] * 10)])))
        self.assertEqual(out[0].column(0).to_pylist(), [v * 2 for v in range(10)])

    def test_stop_iteration_raises_runtime_error(self):
        def stop(a):
            raise StopIteration()

        handler = self._handler((stop, [0], {}, LongType()))
        with self.assertRaises(PySparkRuntimeError):
            list(handler.run(0, iter([self._batch(a=[1], b=[0])])))


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowScalarUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    def test_invokes_udf_per_batch(self):
        def add_one(col):
            return pa.array([v.as_py() + 1 for v in col], type=pa.int64())

        handler = ArrowScalarUDFHandler(
            udfs=[self._udf(add_one, [0])], runner_conf=RunnerConf({}), eval_conf=None
        )
        out = list(handler.run(0, iter([self._batch(a=[1, 2, 3])])))
        self.assertEqual([b.column(0).to_pylist() for b in out], [[2, 3, 4]])

    def test_coerces_output_to_return_type(self):
        # The UDF returns int32, but the declared return type is LongType (int64).
        def add_one(col):
            return pa.array([v.as_py() + 1 for v in col], type=pa.int32())

        handler = ArrowScalarUDFHandler(
            udfs=[self._udf(add_one, [0])], runner_conf=RunnerConf({}), eval_conf=None
        )
        out = list(handler.run(0, iter([self._batch(a=[10, 20])])))
        self.assertEqual(out[0].schema.field(0).type, pa.int64())
        self.assertEqual(out[0].column(0).to_pylist(), [11, 21])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowScalarIterUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    def test_invokes_udf_over_batch_stream(self):
        def add_one(col_iter):
            for col in col_iter:
                yield pa.array([v.as_py() + 1 for v in col], type=pa.int64())

        handler = ArrowScalarIterUDFHandler(
            udfs=[self._udf(add_one, [0])], runner_conf=RunnerConf({}), eval_conf=None
        )
        out = list(handler.run(0, iter([self._batch(a=[1, 2]), self._batch(a=[3])])))
        self.assertEqual([b.column(0).to_pylist() for b in out], [[2, 3], [4]])

    def test_rejects_row_count_mismatch(self):
        # Emitting more rows than were consumed must fail (fail-fast row limit).
        def too_many(col_iter):
            for col in col_iter:
                yield pa.array(list(range(len(col) + 1)), type=pa.int64())

        handler = ArrowScalarIterUDFHandler(
            udfs=[self._udf(too_many, [0])], runner_conf=RunnerConf({}), eval_conf=None
        )
        with self.assertRaises(PySparkRuntimeError):
            list(handler.run(0, iter([self._batch(a=[1, 2])])))


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowMapUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    def test_maps_batch_stream(self):
        # mapInArrow flattens the wire struct for the UDF and re-wraps its output.
        def double_v(batch_iter):
            for batch in batch_iter:
                yield self._batch(v=[c.as_py() * 2 for c in batch.column("v")])

        handler = ArrowMapUDFHandler(
            udfs=[self._udf(double_v, [])], runner_conf=RunnerConf({}), eval_conf=None
        )
        out = list(handler.run(0, iter([self._struct_batch(v=[1, 2, 3])])))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [2, 4, 6])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedMapUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    # arg_offsets for one DataFrame with key column 0 and value column 1.
    _GROUP_OFFSETS = ArrowEvalTypeHandlerTestCase._grouped_arg_offsets(([0], [1]))

    def test_applies_udf_per_group(self):
        def grouped_udf(value_table):
            return pa.table({"v": pa.array([c.as_py() * 10 for c in value_table.column("v")])})

        handler = self._grouped_handler(ArrowGroupedMapUDFHandler, grouped_udf, self._GROUP_OFFSETS)
        out = list(handler.run(0, self._one_group(self._struct_batch(k=[7, 7], v=[1, 2]))))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [10, 20])

    def test_passes_key_when_udf_takes_key(self):
        def grouped_udf(key, value_table):
            k = key[0].as_py()
            return pa.table({"v": pa.array([c.as_py() + k for c in value_table.column("v")])})

        handler = self._grouped_handler(ArrowGroupedMapUDFHandler, grouped_udf, self._GROUP_OFFSETS)
        out = list(handler.run(0, self._one_group(self._struct_batch(k=[7, 7], v=[1, 2]))))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [8, 9])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedMapIterUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    # arg_offsets for one DataFrame with key column 0 and value column 1.
    _GROUP_OFFSETS = ArrowEvalTypeHandlerTestCase._grouped_arg_offsets(([0], [1]))

    def test_applies_udf_per_group(self):
        def grouped_udf(value_batches):
            for batch in value_batches:
                yield self._batch(v=[c.as_py() + 1 for c in batch.column("v")])

        handler = self._grouped_handler(
            ArrowGroupedMapIterUDFHandler, grouped_udf, self._GROUP_OFFSETS
        )
        out = list(handler.run(0, self._one_group(self._struct_batch(k=[7], v=[41]))))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [42])

    def test_passes_key_when_udf_takes_key(self):
        def grouped_udf(key, value_batches):
            k = key[0].as_py()
            for batch in value_batches:
                yield self._batch(v=[c.as_py() + k for c in batch.column("v")])

        handler = self._grouped_handler(
            ArrowGroupedMapIterUDFHandler, grouped_udf, self._GROUP_OFFSETS
        )
        out = list(handler.run(0, self._one_group(self._struct_batch(k=[10], v=[5]))))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [15])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowCoGroupedMapUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    # Co-group batches arrive un-wrapped (columns k, v), unlike the grouped-map wire format.
    # arg_offsets for two DataFrames, each with key column 0 and value column 1.
    _COGROUP_OFFSETS = ArrowEvalTypeHandlerTestCase._grouped_arg_offsets(([0], [1]), ([0], [1]))

    @staticmethod
    def _one_cogroup(left, right):
        """One co-group of a left and a right batch, as the co-group serializer yields it."""
        return iter([([left], [right])])

    def test_applies_udf_per_cogroup(self):
        def cogrouped_udf(left_values, right_values):
            total = left_values.column("v")[0].as_py() + right_values.column("v")[0].as_py()
            return pa.table({"v": pa.array([total], type=pa.int64())})

        handler = self._grouped_handler(
            ArrowCoGroupedMapUDFHandler, cogrouped_udf, self._COGROUP_OFFSETS
        )
        out = list(
            handler.run(
                0, self._one_cogroup(self._batch(k=[5], v=[10]), self._batch(k=[5], v=[20]))
            )
        )
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [30])

    def test_passes_key_when_udf_takes_key(self):
        def cogrouped_udf(key, left_values, right_values):
            k = key[0].as_py()
            total = left_values.column("v")[0].as_py() + right_values.column("v")[0].as_py()
            return pa.table({"v": pa.array([total + k], type=pa.int64())})

        handler = self._grouped_handler(
            ArrowCoGroupedMapUDFHandler, cogrouped_udf, self._COGROUP_OFFSETS
        )
        out = list(
            handler.run(
                0, self._one_cogroup(self._batch(k=[5], v=[10]), self._batch(k=[5], v=[20]))
            )
        )
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [35])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedAggUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    # Grouped-agg batches arrive un-wrapped (flat columns); the UDF reads columns by offset and
    # returns one scalar, emitted as a single-row batch per group.
    def test_reduces_group_to_one_row(self):
        def sum_udf(col):
            return sum(c.as_py() for c in col)

        handler = ArrowGroupedAggUDFHandler(
            udfs=[self._udf(sum_udf, [0])], runner_conf=RunnerConf({}), eval_conf=None
        )
        out = list(handler.run(0, self._one_group(self._batch(v=[1, 2, 3]), self._batch(v=[4]))))
        self.assertEqual(out[0].column("_0").to_pylist(), [10])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedAggIterUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    # The UDF receives the group's input columns as an iterator and returns one scalar.
    def test_reduces_group_to_one_row(self):
        def sum_iter_udf(col_iter):
            return sum(c.as_py() for col in col_iter for c in col)

        handler = ArrowGroupedAggIterUDFHandler(
            udfs=[self._udf(sum_iter_udf, [0])], runner_conf=RunnerConf({}), eval_conf=None
        )
        out = list(handler.run(0, self._one_group(self._batch(v=[10]), self._batch(v=[20]))))
        self.assertEqual(out[0].column("_0").to_pylist(), [30])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowWindowAggUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    # One output value per input row over the UDF's window frame.
    def test_unbounded_frame_repeats_one_value(self):
        def sum_udf(col):
            return sum(c.as_py() for c in col)

        handler = ArrowWindowAggUDFHandler(
            udfs=[self._udf(sum_udf, [0])],
            runner_conf=RunnerConf({"window_bound_types": "unbounded"}),
            eval_conf=None,
        )
        out = list(handler.run(0, self._one_group(self._batch(v=[1, 2, 3]))))
        self.assertEqual(out[0].column("_0").to_pylist(), [6, 6, 6])

    def test_bounded_frame_slices_per_row(self):
        # args_offsets = [begin_col, end_col, *value_cols]; each row's frame is ``[begin, end)``.
        def sum_udf(col):
            return sum(c.as_py() for c in col)

        handler = ArrowWindowAggUDFHandler(
            udfs=[self._udf(sum_udf, [0, 1, 2])],
            runner_conf=RunnerConf({"window_bound_types": "bounded"}),
            eval_conf=None,
        )
        # Row 0 frame [0, 1) -> [10]; row 1 frame [0, 2) -> [10, 20].
        out = list(
            handler.run(0, self._one_group(self._batch(begin=[0, 0], end=[1, 2], v=[10, 20])))
        )
        self.assertEqual(out[0].column("_0").to_pylist(), [10, 30])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedAggIncrementalPartialUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    # Input columns are ``[k, v]``: the leading grouping key, then the aggregator input. Output
    # is one row per key: ``k_0`` followed by the ``_0`` buffer struct.
    _KEY_SCHEMA = StructType([StructField("k", LongType())])

    def _handler(self, cap=10000):
        # The handler takes the Aggregator itself from the record's unwrapped func.
        return ArrowGroupedAggIncrementalPartialUDFHandler(
            udfs=[self._udf(_SumAggregator(), [1])],
            runner_conf=RunnerConf({"spark.sql.execution.arrow.maxRecordsPerBatch": str(cap)}),
            eval_conf=EvalConf({"grouping_key_schema": self._KEY_SCHEMA.json()}),
        )

    @staticmethod
    def _rows(out):
        return [
            (k, buf["s"], buf["c"])
            for b in out
            for k, buf in zip(b.column("k_0").to_pylist(), b.column("_0").to_pylist())
        ]

    def test_combines_rows_per_key_across_batches(self):
        out = list(
            self._handler().run(
                0, iter([self._batch(k=[1, 2, 1], v=[1, 2, 3]), self._batch(k=[2], v=[4])])
            )
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(sorted(self._rows(out)), [(1, 4, 2), (2, 6, 2)])

    def test_empty_partition_emits_nothing(self):
        self.assertEqual(list(self._handler().run(0, iter([]))), [])

    def test_flushes_when_key_count_reaches_cap(self):
        # cap=2: the check runs after each input batch. The first batch brings the map to exactly
        # 2 keys, so it flushes (>= cap); key 1 then reappears in a fresh map, and the FINAL stage
        # merges the duplicate downstream. A mid-stream flush is not chunked, so the second batch
        # flushes all 3 of its keys at once.
        out = list(
            self._handler(cap=2).run(
                0, iter([self._batch(k=[1, 2], v=[1, 2]), self._batch(k=[1, 3, 4], v=[10, 3, 4])])
            )
        )
        self.assertEqual([b.num_rows for b in out], [2, 3])
        self.assertEqual(sorted(self._rows(out[:1])), [(1, 1, 1), (2, 2, 1)])
        self.assertEqual(sorted(self._rows(out[1:])), [(1, 10, 1), (3, 3, 1), (4, 4, 1)])

    def test_below_cap_is_not_flushed(self):
        out = list(
            self._handler(cap=3).run(
                0, iter([self._batch(k=[1, 2], v=[1, 2]), self._batch(k=[1], v=[5])])
            )
        )
        self.assertEqual([b.num_rows for b in out], [2])
        self.assertEqual(sorted(self._rows(out)), [(1, 6, 2), (2, 2, 1)])

    def test_non_positive_cap_is_unbounded(self):
        out = list(self._handler(cap=0).run(0, iter([self._batch(k=[1, 2, 3], v=[1, 2, 3])])))
        self.assertEqual([b.num_rows for b in out], [3])

    def test_nan_keys_are_combined(self):
        batch = pa.RecordBatch.from_arrays(
            [pa.array([float("nan"), float("nan")]), pa.array([1, 2], type=pa.int64())],
            ["k", "v"],
        )
        handler = ArrowGroupedAggIncrementalPartialUDFHandler(
            udfs=[self._udf(_SumAggregator(), [1])],
            runner_conf=RunnerConf({}),
            eval_conf=EvalConf(
                {"grouping_key_schema": StructType([StructField("k", DoubleType())]).json()}
            ),
        )
        out = list(handler.run(0, iter([batch])))
        self.assertEqual(out[0].num_rows, 1)
        self.assertEqual(out[0].column("_0").to_pylist(), [{"s": 3, "c": 2}])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedAggIncrementalFinalUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    # Each group's rows carry one partial-buffer struct column; the handler merges them and
    # emits one ``finish`` value per group.
    _BUFFER_TYPE = pa.struct([("s", pa.int64()), ("c", pa.int64())]) if have_pyarrow else None

    def _buffers(self, buffers):
        return pa.RecordBatch.from_arrays([pa.array(buffers, type=self._BUFFER_TYPE)], ["buf"])

    def _handler(self):
        # The handler takes the Aggregator itself from the record's unwrapped func.
        return ArrowGroupedAggIncrementalFinalUDFHandler(
            udfs=[self._udf(_SumAggregator(), [0])], runner_conf=RunnerConf({}), eval_conf=None
        )

    def test_merges_partial_buffers_per_group(self):
        groups = iter(
            [
                iter([self._buffers([{"s": 1, "c": 1}]), self._buffers([{"s": 2, "c": 1}])]),
                iter([self._buffers([{"s": 10, "c": 2}])]),
            ]
        )
        out = list(self._handler().run(0, groups))
        self.assertEqual([b.column("_0").to_pylist() for b in out], [[3], [10]])

    def test_null_buffers_are_skipped(self):
        # An all-null buffer group (the operator's empty global aggregation) finishes ``zero``.
        out = list(self._handler().run(0, self._one_group(self._buffers([None]))))
        self.assertEqual(out[0].column("_0").to_pylist(), [0])
        out = list(self._handler().run(0, self._one_group(self._buffers([None, {"s": 5, "c": 1}]))))
        self.assertEqual(out[0].column("_0").to_pylist(), [5])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowWindowAggIncrementalUDFHandlerTests(ArrowEvalTypeHandlerTestCase):
    def _handler(self, agg, bound_type, args_offsets):
        return ArrowWindowAggIncrementalUDFHandler(
            udfs=[self._udf(agg, args_offsets)],
            runner_conf=RunnerConf({"window_bound_types": bound_type}),
            eval_conf=None,
        )

    def test_unbounded_frame_repeats_one_value(self):
        handler = self._handler(_SumAggregator(), "unbounded", [0])
        out = list(handler.run(0, self._one_group(self._batch(v=[1, 2]), self._batch(v=[3]))))
        self.assertEqual(out[0].column("_0").to_pylist(), [6, 6, 6])

    def test_growing_frame_extends_running_buffer(self):
        # rowsBetween(unboundedPreceding, currentRow): same lower bound, growing upper bound,
        # so each row folds only its newly-included row.
        agg = _SumAggregator()
        handler = self._handler(agg, "bounded", [0, 1, 2])
        batch = self._batch(begin=[0, 0, 0, 0], end=[1, 2, 3, 4], v=[1, 2, 3, 4])
        out = list(handler.run(0, self._one_group(batch)))
        self.assertEqual(out[0].column("_0").to_pylist(), [1, 3, 6, 10])
        self.assertEqual(agg.reduce_calls, 4)

    def test_sliding_frame_refolds_from_zero(self):
        # rowsBetween(-1, currentRow): the lower bound advances, so each frame refolds.
        agg = _SumAggregator()
        handler = self._handler(agg, "bounded", [0, 1, 2])
        batch = self._batch(begin=[0, 0, 1, 2], end=[1, 2, 3, 4], v=[1, 2, 3, 4])
        out = list(handler.run(0, self._one_group(batch)))
        self.assertEqual(out[0].column("_0").to_pylist(), [1, 3, 5, 7])
        self.assertEqual(agg.reduce_calls, 1 + 1 + 2 + 2)

    def test_invalid_bound_type_raises(self):
        handler = self._handler(_SumAggregator(), "bogus", [0])
        with self.assertRaises(PySparkRuntimeError):
            list(handler.run(0, self._one_group(self._batch(v=[1]))))


class HashableGroupingKeyTests(PySparkBaseTestCase):
    def test_nested_values_hash_by_value(self):
        self.assertEqual(
            hashable_grouping_key(([1, [2]], {"a": 1})), hashable_grouping_key(([1, [2]], {"a": 1}))
        )

    def test_nan_values_collapse(self):
        self.assertEqual(
            hashable_grouping_key((float("nan"), [float("nan")])),
            hashable_grouping_key((float("nan"), [float("nan")])),
        )

    def test_unhashable_value_gets_a_unique_key(self):
        self.assertNotEqual(hashable_grouping_key(({1},)), hashable_grouping_key(({1},)))


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class CoGroupedBatchTests(ArrowEvalTypeHandlerTestCase):
    def test_deserialized_co_group_is_a_pair_of_lists(self):
        # CoGroupedBatch must match what ArrowStreamCoGroupSerializer yields: the
        # serializer eagerly materializes each side as a list, not an iterator.
        import io

        from pyspark.serializers import write_int

        def arrow_bytes(batches):
            buf = io.BytesIO()
            ArrowStreamSerializer().dump_stream(iter(batches), buf)
            return buf.getvalue()

        stream = io.BytesIO()
        write_int(2, stream)  # two DataFrames in the co-group
        stream.write(arrow_bytes([self._batch(v=[1, 2])]))
        stream.write(arrow_bytes([self._batch(v=[9])]))
        write_int(0, stream)  # end of stream
        stream.seek(0)

        groups = list(ArrowStreamCoGroupSerializer().load_stream(stream))
        self.assertEqual(len(groups), 1)
        left_side, right_side = groups[0]
        self.assertIsInstance(left_side, list)
        self.assertIsInstance(right_side, list)
        self.assertEqual([b.num_rows for b in left_side], [2])
        self.assertEqual([b.num_rows for b in right_side], [1])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
