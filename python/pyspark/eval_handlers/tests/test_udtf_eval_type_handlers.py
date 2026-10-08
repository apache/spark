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
from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message
from pyspark.util import PythonEvalType

with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker_util import EvalConf, RunnerConf

if have_pyarrow:
    import pyarrow as pa

    from pyspark.eval_handlers._udtf import (
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
        self.assertIs(get_eval_type_handler(PythonEvalType.SQL_ARROW_UDTF), ArrowUDTFHandler)
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


if __name__ == "__main__":
    from pyspark.testing import main

    main()
