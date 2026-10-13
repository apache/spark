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

The handler gets the ``(udtf, args_offsets, kwargs_offsets, return_type)`` tuple
``read_single_udtf`` produces; each test builds that tuple around a UDTF instance,
calls ``run(0, <input batches>)`` and asserts on the output batches.
"""

import os
import unittest
from unittest.mock import patch

from pyspark.errors import PySparkRuntimeError
from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.sql.functions import SkipRestOfInputTableException
from pyspark.sql.pandas.serializers import ArrowStreamSerializer
from pyspark.sql.types import LongType, StructField, StructType
from pyspark.testing.utils import (
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
        UDTFWithPartitions,
    )
    from pyspark.sql.conversion import ArrowBatchTransformer


_RETURN_TYPE = StructType([StructField("x", LongType())])


class _RecordingUDTF:
    """Arrow UDTF that echoes its TABLE argument and records the lifecycle calls."""

    def __init__(self, skip_after=None):
        self.calls = []
        self._skip_after = skip_after

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


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowUDTFHandlerTests(unittest.TestCase):
    def _handler(self, udtf, args=(0,), kwargs=None, table_arg_offsets="0"):
        conf = {} if table_arg_offsets is None else {"table_arg_offsets": table_arg_offsets}
        return ArrowUDTFHandler(
            udfs=[(udtf, list(args), kwargs or {}, _RETURN_TYPE)],
            runner_conf=RunnerConf({}),
            eval_conf=EvalConf(conf),
        )

    def test_registered(self):
        self.assertIs(
            get_eval_type_handler(PythonEvalType.SQL_ARROW_UDTF, RunnerConf({})), ArrowUDTFHandler
        )
        self.assertIsInstance(self._handler(_RecordingUDTF()).serializer, ArrowStreamSerializer)

    def test_partition_wrapper(self):
        self.assertIs(UDTFEvalTypeHandler.partition_wrapper, UDTFWithPartitions)
        self.assertIs(ArrowUDTFHandler.partition_wrapper, ArrowUDTFWithPartition)

    def test_partitioned_terminate_per_partition(self):
        # Column 1 is the projected PARTITION BY key; the UDTF only sees column 0.
        created = []

        def create_udtf():
            created.append(_RecordingUDTF(skip_after=1))
            return created[-1]

        udtf = ArrowUDTFWithPartition(create_udtf, [1])
        batches = [
            ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [1, 2, 3], "k": [0, 0, 1]})),
            ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [4, 5], "k": [1, 2]})),
        ]
        out = list(self._handler(udtf).run(0, iter(batches)))
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
        udtf = _RecordingUDTF()
        out = list(
            self._handler(udtf).run(
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
        self.assertEqual(udtf.calls, ["eval", "eval", "terminate", "cleanup"])

    def test_kwargs(self):
        class KwargsUDTF:
            def eval(self, *, table):
                yield pa.table({"x": table.column(0)})

        out = list(
            self._handler(KwargsUDTF(), args=(), kwargs={"table": 0}).run(
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
        out = list(self._handler(ScalarUDTF(), table_arg_offsets=None).run(0, iter([batch])))
        self.assertEqual([r["x"] for b in out for r in b.column(0).to_pylist()], [10, 20])

    def test_skip_rest_of_input_table(self):
        udtf = _RecordingUDTF(skip_after=1)
        batches = iter(
            [
                ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [1]})),
                ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [2]})),
                ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [3]})),
            ]
        )
        out = list(self._handler(udtf).run(0, batches))
        self.assertEqual([r["x"] for b in out for r in b.column(0).to_pylist()], [1, -1])
        self.assertEqual(udtf.calls, ["eval", "eval", "terminate", "cleanup"])
        # The remaining input is left unconsumed.
        self.assertEqual(len(list(batches)), 1)

    def test_none_result_yields_empty_batch(self):
        class NoneUDTF:
            def eval(self, table):
                return None

        out = list(
            self._handler(NoneUDTF()).run(
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

        for udtf, error_class in [
            (Raises(), "UDTF_EXEC_ERROR"),
            (NotIterable(), "UDTF_RETURN_NOT_ITERABLE"),
            (NotArrow(), "UDTF_ARROW_TYPE_CONVERSION_ERROR"),
            (WrongColumns(), "UDTF_RETURN_SCHEMA_MISMATCH"),
        ]:
            with self.subTest(error_class=error_class):
                with self.assertRaises(PySparkRuntimeError) as ctx:
                    list(
                        self._handler(udtf).run(
                            0,
                            iter([ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [1]}))]),
                        )
                    )
                self.assertEqual(ctx.exception.getCondition(), error_class)

    def test_cleanup_on_error(self):
        udtf = _RecordingUDTF()
        udtf.eval = lambda table: 1  # not iterable
        with self.assertRaises(PySparkRuntimeError):
            list(
                self._handler(udtf).run(
                    0, iter([ArrowBatchTransformer.wrap_struct(pa.record_batch({"x": [1]}))])
                )
            )
        self.assertEqual(udtf.calls, ["cleanup"])


class _RowUDTF:
    """Row-wise UDTF that emits its argument and its double, and records the lifecycle calls."""

    def __init__(self, skip_after=None):
        self.calls = []
        self._skip_after = skip_after

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


_ROW_RETURN_TYPE = StructType([StructField("x", LongType()), StructField("y", LongType())])
_ROW_INPUT_TYPE = StructType([StructField("a", LongType())])


class ArrowTableUDTFHandlerTestsMixin:
    """Tests shared by both SQL_ARROW_TABLE_UDF variants; subclasses set ``legacy``."""

    legacy: bool

    def _runner_conf(self):
        return RunnerConf(
            {
                "spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled": str(
                    self.legacy
                ).lower()
            }
        )

    def _handler(self, udtf, args=(0,), kwargs=None, return_type=_ROW_RETURN_TYPE):
        runner_conf = self._runner_conf()
        handler_cls = get_eval_type_handler(PythonEvalType.SQL_ARROW_TABLE_UDF, runner_conf)
        return handler_cls(
            udfs=[(udtf, list(args), kwargs or {}, return_type)],
            runner_conf=runner_conf,
            eval_conf=EvalConf({"input_type": _ROW_INPUT_TYPE.json()}),
        )

    def _run(self, handler, *columns):
        batches = [pa.record_batch({"a": pa.array(c, type=pa.int64())}) for c in columns]
        return [r for b in handler.run(0, iter(batches)) for r in b.column(0).to_pylist()]

    def test_registered(self):
        handler = self._handler(_RowUDTF())
        self.assertIsInstance(handler, self.handler_cls)
        self.assertIsInstance(handler.serializer, ArrowStreamSerializer)
        self.assertIs(self.handler_cls.partition_wrapper, UDTFWithPartitions)

    def test_eval_terminate_cleanup(self):
        udtf = _RowUDTF()
        out = self._run(self._handler(udtf), [1, 2], [3])
        self.assertEqual([(r["x"], r["y"]) for r in out], [(1, 2), (2, 4), (3, 6), (-1, -2)])
        self.assertEqual(udtf.calls, ["eval", "eval", "eval", "terminate", "cleanup"])

    def test_kwargs(self):
        class KwargsUDTF:
            def eval(self, *, a):
                yield a, -a

        out = self._run(self._handler(KwargsUDTF(), args=(), kwargs={"a": 0}), [7])
        self.assertEqual([(r["x"], r["y"]) for r in out], [(7, -7)])

    def test_skip_rest_of_input_table(self):
        udtf = _RowUDTF(skip_after=1)
        out = self._run(self._handler(udtf), [1, 2, 3])
        self.assertEqual([(r["x"], r["y"]) for r in out], [(1, 2), (-1, -2)])
        self.assertEqual(udtf.calls, ["eval", "eval", "terminate", "cleanup"])

    def test_none_result(self):
        class NoneUDTF:
            def eval(self, a):
                return None

        self.assertEqual(self._run(self._handler(NoneUDTF()), [1]), [])

    def test_non_nullable_column(self):
        class NullUDTF:
            def eval(self, a):
                yield None, a

        return_type = StructType(
            [StructField("x", LongType(), nullable=False), StructField("y", LongType())]
        )
        with self.assertRaises(PySparkRuntimeError) as ctx:
            self._run(self._handler(NullUDTF(), return_type=return_type), [1])
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

        for udtf, error_class in [
            (Raises(), "UDTF_EXEC_ERROR"),
            (NotIterable(), "UDTF_RETURN_NOT_ITERABLE"),
            (WrongColumns(), "UDTF_RETURN_SCHEMA_MISMATCH"),
        ]:
            with self.subTest(error_class=error_class):
                with self.assertRaises(PySparkRuntimeError) as ctx:
                    self._run(self._handler(udtf), [1])
                self.assertEqual(ctx.exception.getCondition(), error_class)

    def test_cleanup_on_error(self):
        udtf = _RowUDTF()
        udtf.eval = lambda a: 1  # not iterable
        with self.assertRaises(PySparkRuntimeError):
            self._run(self._handler(udtf), [1])
        self.assertEqual(udtf.calls, ["cleanup"])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowTableUDTFArrowHandlerTests(ArrowTableUDTFHandlerTestsMixin, unittest.TestCase):
    legacy = False

    @property
    def handler_cls(self):
        return ArrowTableUDTFArrowHandler

    def test_scalar_result_wrapped_for_single_column(self):
        class ScalarUDTF:
            def eval(self, a):
                yield a

        out = self._run(self._handler(ScalarUDTF(), return_type=_RETURN_TYPE), [1, 2])
        self.assertEqual([r["x"] for r in out], [1, 2])


@unittest.skipIf(
    not (have_pandas and have_pyarrow), pandas_requirement_message or pyarrow_requirement_message
)
class ArrowTableUDTFLegacyPandasHandlerTests(ArrowTableUDTFHandlerTestsMixin, unittest.TestCase):
    legacy = True

    @property
    def handler_cls(self):
        return ArrowTableUDTFLegacyPandasHandler


if __name__ == "__main__":
    from pyspark.testing import main

    main()
