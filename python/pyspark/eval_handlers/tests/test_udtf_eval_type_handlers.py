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

"""Tests for the UDTF eval type handlers (``_udtf``).

Each test builds the handler from the ``UDTF`` record of a UDTF class, calls
``run(0, <input batches>)`` and asserts on the output batches.
"""

import os
import unittest
from unittest.mock import patch

from pyspark.errors import PySparkRuntimeError
from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.eval_handlers._typing import UDTF
from pyspark.eval_handlers._udtf import TableUDTFHandler, UDTFWithPartitions
from pyspark.serializers import BatchedSerializer, CPickleSerializer
from pyspark.sql.functions import SkipRestOfInputTableException
from pyspark.sql.pandas.serializers import ArrowStreamSerializer
from pyspark.sql.types import LongType, StructField, StructType
from pyspark.sql.udtf import AnalyzeResult
from pyspark.testing.utils import (
    PySparkBaseTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import PythonEvalType

with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker_util import EvalConf, RunnerConf

if have_pyarrow:
    import pyarrow as pa

    from pyspark.eval_handlers._udtf import (
        ArrowTableUDTFArrowHandler,
        ArrowTableUDTFLegacyPandasHandler,
        ArrowUDTFHandler,
        ArrowUDTFWithPartition,
        UDTFEvalTypeHandler,
    )
    from pyspark.sql.conversion import ArrowBatchTransformer


class _RecordingUDTF:
    """Arrow UDTF that echoes its TABLE argument and records the lifecycle calls."""

    # Raise SkipRestOfInputTableException on the eval call after this many; None never skips.
    _skip_after = None

    def __init__(self):
        self.calls = []

    def eval(self, table):
        self.calls.append("eval")
        if self._skip_after is not None and len(self.calls) > self._skip_after:
            raise SkipRestOfInputTableException()
        yield pa.table({"x": table.column(0)})

    def terminate(self):
        self.calls.append("terminate")
        yield pa.table({"x": pa.array([-1], type=pa.int64())})

    def cleanup(self):
        self.calls.append("cleanup")


class _RowUDTF:
    """Row-wise UDTF that emits its argument and its double, and records the lifecycle calls."""

    # Raise SkipRestOfInputTableException on the eval call after this many; None never skips.
    _skip_after = None

    def __init__(self):
        self.calls = []

    def eval(self, a):
        self.calls.append("eval")
        if self._skip_after is not None and len(self.calls) > self._skip_after:
            raise SkipRestOfInputTableException()
        yield a, a * 2

    def terminate(self):
        self.calls.append("terminate")
        yield -1, -2

    def cleanup(self):
        self.calls.append("cleanup")


class UDTFEvalTypeHandlerTestCase(PySparkBaseTestCase):
    """Shared return type and helpers for the UDTF handler tests."""

    _RETURN_TYPE = StructType([StructField("x", LongType())])

    @staticmethod
    def _udtf_record(
        udtf_cls,
        return_type,
        args=(0,),
        kwargs=None,
        partition_child_indexes=(),
        analyze_result=None,
    ):
        """The ``UDTF`` record the worker builds for ``udtf_cls``."""
        return UDTF(
            udtf_cls=udtf_cls,
            args_offsets=list(args),
            kwargs_offsets=kwargs or {},
            return_type=return_type,
            partition_child_indexes=list(partition_child_indexes),
            analyze_result=analyze_result,
            name="test_udtf",
        )


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class UDTFEvalTypeHandlerRegistrationTests(PySparkBaseTestCase):
    def test_registered(self):
        legacy_conf = "spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled"
        # Each UDTF eval type dispatches to its handler by lookup; SQL_ARROW_TABLE_UDF picks
        # its variant by the legacy pandas conversion conf.
        for eval_type, conf, handler_cls in (
            (PythonEvalType.SQL_TABLE_UDF, {}, TableUDTFHandler),
            (PythonEvalType.SQL_TABLE_UDF, {legacy_conf: "true"}, TableUDTFHandler),
            (PythonEvalType.SQL_ARROW_UDTF, {}, ArrowUDTFHandler),
            (PythonEvalType.SQL_ARROW_TABLE_UDF, {}, ArrowTableUDTFArrowHandler),
            (
                PythonEvalType.SQL_ARROW_TABLE_UDF,
                {legacy_conf: "false"},
                ArrowTableUDTFArrowHandler,
            ),
            (
                PythonEvalType.SQL_ARROW_TABLE_UDF,
                {legacy_conf: "true"},
                ArrowTableUDTFLegacyPandasHandler,
            ),
        ):
            with self.subTest(eval_type=eval_type, conf=conf):
                self.assertIs(get_eval_type_handler(eval_type, RunnerConf(conf)), handler_cls)


class TableUDTFHandlerTests(UDTFEvalTypeHandlerTestCase):
    _ROW_RETURN_TYPE = StructType([StructField("x", LongType()), StructField("y", LongType())])

    def _handler(self, udtf_cls, args=(0,), kwargs=None, return_type=None):
        udtf = self._udtf_record(udtf_cls, return_type or self._ROW_RETURN_TYPE, args, kwargs)
        return TableUDTFHandler(udfs=[udtf], runner_conf=RunnerConf({}), eval_conf=EvalConf({}))

    def _run(self, handler, *rows):
        # Each input row is a list of argument values; each output is the tuple of rows one
        # eval/terminate call returned.
        return list(handler.run(0, iter([list(r) for r in rows])))

    def test_serializer_and_partition_wrapper(self):
        ser = self._handler(_RowUDTF).serializer
        self.assertIsInstance(ser, BatchedSerializer)
        self.assertIsInstance(ser.serializer, CPickleSerializer)
        self.assertEqual(ser.batchSize, 1)
        self.assertIs(TableUDTFHandler.partition_wrapper, UDTFWithPartitions)

    def test_eval_terminate_cleanup(self):
        handler = self._handler(_RowUDTF)
        out = self._run(handler, [1], [2])
        self.assertEqual(out, [((1, 2),), ((2, 4),), ((-1, -2),)])
        self.assertEqual(handler._udtf.calls, ["eval", "eval", "terminate", "cleanup"])

    def test_no_terminate_or_cleanup(self):
        class EvalOnly:
            def eval(self, a):
                yield a, a

        self.assertEqual(self._run(self._handler(EvalOnly), [1]), [((1, 1),)])

    def test_kwargs(self):
        class KwargsUDTF:
            def eval(self, *, a):
                yield a, -a

        out = self._run(self._handler(KwargsUDTF, args=(), kwargs={"a": 0}), [7])
        self.assertEqual(out, [((7, -7),)])

    def test_skip_rest_of_input_table(self):
        class SkipUDTF(_RowUDTF):
            _skip_after = 1

        handler = self._handler(SkipUDTF)
        out = self._run(handler, [1], [2], [3])
        self.assertEqual(out, [((1, 2),), ((-1, -2),)])
        self.assertEqual(handler._udtf.calls, ["eval", "eval", "terminate", "cleanup"])

    def test_none_result(self):
        class NoneUDTF:
            def eval(self, a):
                return None

        self.assertEqual(self._run(self._handler(NoneUDTF), [1]), [()])

    def test_non_nullable_column(self):
        class NullUDTF:
            def eval(self, a):
                yield None, a

        return_type = StructType(
            [StructField("x", LongType(), nullable=False), StructField("y", LongType())]
        )
        with self.assertRaises(PySparkRuntimeError) as ctx:
            self._run(self._handler(NullUDTF, return_type=return_type), [1])
        self.assertEqual(ctx.exception.getCondition(), "UDTF_EXEC_ERROR")

    def test_errors(self):
        class Raises:
            def eval(self, a):
                raise ValueError("boom")

        class NotIterable:
            def eval(self, a):
                return 1

        class WrongColumns:
            def eval(self, a):
                yield a, a, a

        class NotARow:
            def eval(self, a):
                yield a

        for udtf_cls, error_class in [
            (Raises, "UDTF_EXEC_ERROR"),
            (NotIterable, "UDTF_RETURN_NOT_ITERABLE"),
            (WrongColumns, "UDTF_RETURN_SCHEMA_MISMATCH"),
            (NotARow, "UDTF_INVALID_OUTPUT_ROW_TYPE"),
        ]:
            with self.subTest(error_class=error_class):
                with self.assertRaises(PySparkRuntimeError) as ctx:
                    self._run(self._handler(udtf_cls), [1])
                self.assertEqual(ctx.exception.getCondition(), error_class)

    def test_cleanup_on_error(self):
        class NotIterable(_RowUDTF):
            def eval(self, a):
                return 1

        handler = self._handler(NotIterable)
        with self.assertRaises(PySparkRuntimeError):
            self._run(handler, [1])
        self.assertEqual(handler._udtf.calls, ["cleanup"])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowUDTFHandlerTests(UDTFEvalTypeHandlerTestCase):
    def _handler(
        self, udtf_cls, args=(0,), kwargs=None, table_arg_offsets="0", partition_child_indexes=()
    ):
        conf = {} if table_arg_offsets is None else {"table_arg_offsets": table_arg_offsets}
        udtf = self._udtf_record(udtf_cls, self._RETURN_TYPE, args, kwargs, partition_child_indexes)
        return ArrowUDTFHandler(udfs=[udtf], runner_conf=RunnerConf({}), eval_conf=EvalConf(conf))

    def test_serializer(self):
        self.assertIsInstance(self._handler(_RecordingUDTF).serializer, ArrowStreamSerializer)

    def test_partition_wrapper(self):
        self.assertIs(UDTFEvalTypeHandler.partition_wrapper, UDTFWithPartitions)
        self.assertIs(ArrowUDTFHandler.partition_wrapper, ArrowUDTFWithPartition)

    def test_partitioned_terminate_per_partition(self):
        # Column 1 is the projected PARTITION BY key; the UDTF only sees column 0.
        created = []

        class Udtf(_RecordingUDTF):
            _skip_after = 1

            def __init__(self):
                super().__init__()
                created.append(self)

        batches = [
            ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [1, 2, 3], "k": [0, 0, 1]})),
            ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [4, 5], "k": [1, 2]})),
        ]
        handler = self._handler(Udtf, partition_child_indexes=[1])
        self.assertIsInstance(handler._udtf, ArrowUDTFWithPartition)
        out = list(handler.run(0, iter(batches)))
        # Partition k=1 spans both batches; its second eval call is skipped.
        self.assertEqual(
            [r["x"] for b in out for r in b.column(0).to_pylist()], [1, 2, -1, 3, -1, 5, -1]
        )
        self.assertEqual(
            [u.calls for u in created],
            [
                ["eval", "terminate"],
                ["eval", "eval", "terminate"],
                ["eval", "terminate", "cleanup"],
            ],
        )

    def test_eval_terminate_cleanup(self):
        handler = self._handler(_RecordingUDTF)
        out = list(
            handler.run(
                0,
                iter(
                    [
                        ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [1, 2]})),
                        ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [3]})),
                    ]
                ),
            )
        )
        self.assertEqual([r["x"] for b in out for r in b.column(0).to_pylist()], [1, 2, 3, -1])
        self.assertEqual(handler._udtf.calls, ["eval", "eval", "terminate", "cleanup"])

    def test_kwargs(self):
        class KwargsUDTF:
            def eval(self, *, table):
                yield pa.table({"x": table.column(0)})

        out = list(
            self._handler(KwargsUDTF, args=(), kwargs={"table": 0}).run(
                0, iter([ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [7]}))])
            )
        )
        self.assertEqual([r["x"] for b in out for r in b.column(0).to_pylist()], [7])

    def test_scalar_arg_is_not_flattened(self):
        class ScalarUDTF:
            def eval(self, arr):
                assert isinstance(arr, pa.Array), type(arr)
                yield pa.table({"x": pa.compute.multiply(arr, 10)})

        batch = pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.int64())], ["a"])
        out = list(self._handler(ScalarUDTF, table_arg_offsets=None).run(0, iter([batch])))
        self.assertEqual([r["x"] for b in out for r in b.column(0).to_pylist()], [10, 20])

    def test_skip_rest_of_input_table(self):
        class SkipUDTF(_RecordingUDTF):
            _skip_after = 1

        handler = self._handler(SkipUDTF)
        batches = iter(
            [
                ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [1]})),
                ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [2]})),
                ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [3]})),
            ]
        )
        out = list(handler.run(0, batches))
        self.assertEqual([r["x"] for b in out for r in b.column(0).to_pylist()], [1, -1])
        self.assertEqual(handler._udtf.calls, ["eval", "eval", "terminate", "cleanup"])
        # The remaining input is left unconsumed.
        self.assertEqual(len(list(batches)), 1)

    def test_none_result_yields_empty_batch(self):
        class NoneUDTF:
            def eval(self, table):
                return None

        out = list(
            self._handler(NoneUDTF).run(
                0, iter([ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [1]}))])
            )
        )
        self.assertEqual(len(out), 1)
        self.assertEqual([r["x"] for b in out for r in b.column(0).to_pylist()], [])

    def test_errors(self):
        class Raises:
            def eval(self, table):
                raise ValueError("boom")

        class NotIterable:
            def eval(self, table):
                return 1

        class NotArrow:
            def eval(self, table):
                yield (1,)

        class WrongColumns:
            def eval(self, table):
                yield pa.table({"x": [1], "y": [2]})

        for udtf_cls, error_class in [
            (Raises, "UDTF_EXEC_ERROR"),
            (NotIterable, "UDTF_RETURN_NOT_ITERABLE"),
            (NotArrow, "UDTF_ARROW_TYPE_CONVERSION_ERROR"),
            (WrongColumns, "UDTF_RETURN_SCHEMA_MISMATCH"),
        ]:
            with self.subTest(error_class=error_class):
                with self.assertRaises(PySparkRuntimeError) as ctx:
                    list(
                        self._handler(udtf_cls).run(
                            0,
                            iter([ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [1]}))]),
                        )
                    )
                self.assertEqual(ctx.exception.getCondition(), error_class)

    def test_cleanup_on_error(self):
        class NotIterable(_RecordingUDTF):
            def eval(self, table):
                return 1

        handler = self._handler(NotIterable)
        with self.assertRaises(PySparkRuntimeError):
            list(
                handler.run(
                    0, iter([ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [1]}))])
                )
            )
        self.assertEqual(handler._udtf.calls, ["cleanup"])


class ArrowTableUDTFHandlerTestsMixin:
    """Tests shared by both SQL_ARROW_TABLE_UDF variants; subclasses set ``handler_cls``."""

    handler_cls: type

    _ROW_RETURN_TYPE = StructType([StructField("x", LongType()), StructField("y", LongType())])
    _ROW_INPUT_TYPE = StructType([StructField("a", LongType())])

    def _handler(self, udtf_cls, args=(0,), kwargs=None, return_type=None):
        udtf = self._udtf_record(udtf_cls, return_type or self._ROW_RETURN_TYPE, args, kwargs)
        return self.handler_cls(
            udfs=[udtf],
            runner_conf=RunnerConf({}),
            eval_conf=EvalConf({"input_type": self._ROW_INPUT_TYPE.json()}),
        )

    def _run(self, handler, *columns):
        batches = [pa.record_batch({"a": pa.array(c, type=pa.int64())}) for c in columns]
        return [r for b in handler.run(0, iter(batches)) for r in b.column(0).to_pylist()]

    def test_serializer_and_partition_wrapper(self):
        self.assertIsInstance(self._handler(_RowUDTF).serializer, ArrowStreamSerializer)
        self.assertIs(self.handler_cls.partition_wrapper, UDTFWithPartitions)

    def test_eval_terminate_cleanup(self):
        handler = self._handler(_RowUDTF)
        out = self._run(handler, [1, 2], [3])
        self.assertEqual([(r["x"], r["y"]) for r in out], [(1, 2), (2, 4), (3, 6), (-1, -2)])
        self.assertEqual(handler._udtf.calls, ["eval", "eval", "eval", "terminate", "cleanup"])

    def test_kwargs(self):
        class KwargsUDTF:
            def eval(self, *, a):
                yield a, -a

        out = self._run(self._handler(KwargsUDTF, args=(), kwargs={"a": 0}), [7])
        self.assertEqual([(r["x"], r["y"]) for r in out], [(7, -7)])

    def test_skip_rest_of_input_table(self):
        class SkipUDTF(_RowUDTF):
            _skip_after = 1

        handler = self._handler(SkipUDTF)
        out = self._run(handler, [1, 2, 3])
        self.assertEqual([(r["x"], r["y"]) for r in out], [(1, 2), (-1, -2)])
        self.assertEqual(handler._udtf.calls, ["eval", "eval", "terminate", "cleanup"])

    def test_none_result(self):
        class NoneUDTF:
            def eval(self, a):
                return None

        self.assertEqual(self._run(self._handler(NoneUDTF), [1]), [])

    def test_non_nullable_column(self):
        class NullUDTF:
            def eval(self, a):
                yield None, a

        return_type = StructType(
            [StructField("x", LongType(), nullable=False), StructField("y", LongType())]
        )
        with self.assertRaises(PySparkRuntimeError) as ctx:
            self._run(self._handler(NullUDTF, return_type=return_type), [1])
        self.assertEqual(ctx.exception.getCondition(), "UDTF_EXEC_ERROR")

    def test_errors(self):
        class Raises:
            def eval(self, a):
                raise ValueError("boom")

        class NotIterable:
            def eval(self, a):
                return 1

        class WrongColumns:
            def eval(self, a):
                yield a, a, a

        for udtf_cls, error_class in [
            (Raises, "UDTF_EXEC_ERROR"),
            (NotIterable, "UDTF_RETURN_NOT_ITERABLE"),
            (WrongColumns, "UDTF_RETURN_SCHEMA_MISMATCH"),
        ]:
            with self.subTest(error_class=error_class):
                with self.assertRaises(PySparkRuntimeError) as ctx:
                    self._run(self._handler(udtf_cls), [1])
                self.assertEqual(ctx.exception.getCondition(), error_class)

    def test_cleanup_on_error(self):
        class NotIterable(_RowUDTF):
            def eval(self, a):
                return 1

        handler = self._handler(NotIterable)
        with self.assertRaises(PySparkRuntimeError):
            self._run(handler, [1])
        self.assertEqual(handler._udtf.calls, ["cleanup"])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowTableUDTFArrowHandlerTests(ArrowTableUDTFHandlerTestsMixin, UDTFEvalTypeHandlerTestCase):
    handler_cls = ArrowTableUDTFArrowHandler if have_pyarrow else None

    def test_scalar_result_wrapped_for_single_column(self):
        class ScalarUDTF:
            def eval(self, a):
                yield a

        out = self._run(self._handler(ScalarUDTF, return_type=self._RETURN_TYPE), [1, 2])
        self.assertEqual([r["x"] for r in out], [1, 2])


@unittest.skipIf(
    not (have_pandas and have_pyarrow), pandas_requirement_message or pyarrow_requirement_message
)
class ArrowTableUDTFLegacyPandasHandlerTests(
    ArrowTableUDTFHandlerTestsMixin, UDTFEvalTypeHandlerTestCase
):
    handler_cls = ArrowTableUDTFLegacyPandasHandler if have_pyarrow else None


class UDTFHandlerInitTests(UDTFEvalTypeHandlerTestCase):
    def _new(self, handler_cls, udtf_cls, **kwargs):
        """A new ``handler_cls`` for ``udtf_cls``."""
        udtf = self._udtf_record(udtf_cls, self._RETURN_TYPE, **kwargs)
        return handler_cls([udtf], RunnerConf({}), EvalConf({}))

    def test_creates_udtf_instance(self):
        class Udtf:
            def eval(self, x):
                yield (x,)

        handler = self._new(TableUDTFHandler, Udtf)
        self.assertIsInstance(handler._udtf, Udtf)
        udtf = handler._udfs[0]
        self.assertIsInstance(udtf, UDTF)
        self.assertIs(handler._udtf_desc, udtf)
        self.assertFalse(hasattr(handler, "_udf_desc"))
        self.assertEqual(udtf.args_offsets, [0])
        self.assertEqual(udtf.kwargs_offsets, {})
        self.assertEqual(udtf.return_type, self._RETURN_TYPE)

    def test_analyze_result_passed_to_init(self):
        class Udtf:
            def __init__(self, analyze_result):
                self.analyze_result = analyze_result

            def eval(self, x):
                yield (x,)

        analyze_result = AnalyzeResult(schema=self._RETURN_TYPE)
        handler = self._new(TableUDTFHandler, Udtf, analyze_result=analyze_result)
        self.assertEqual(handler._udtf.analyze_result, analyze_result)

    def test_wrapped_in_partition_wrapper(self):
        class Udtf:
            def eval(self, row):
                yield (row,)

        handler = self._new(TableUDTFHandler, Udtf, partition_child_indexes=[0])
        self.assertIsInstance(handler._udtf, UDTFWithPartitions)

    @unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
    def test_arrow_partition_wrapper(self):
        class Udtf:
            def eval(self, table):
                yield table

        handler = self._new(ArrowUDTFHandler, Udtf, partition_child_indexes=[0])
        self.assertIsInstance(handler._udtf, ArrowUDTFWithPartition)

    def test_invalid_udtf(self):
        class NoEval:
            pass

        class Udtf:
            def eval(self, x):
                yield (x,)

        with self.assertRaisesRegex(PySparkRuntimeError, "Invalid UDTF handler type"):
            self._new(TableUDTFHandler, Udtf())
        with self.assertRaisesRegex(PySparkRuntimeError, "'eval' method"):
            self._new(TableUDTFHandler, NoEval)
        with self.assertRaises(PySparkRuntimeError) as ctx:
            self._new(TableUDTFHandler, Udtf, args=(0, 1))
        self.assertEqual(
            ctx.exception.getCondition(), "UDTF_EVAL_METHOD_ARGUMENTS_DO_NOT_MATCH_SIGNATURE"
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
