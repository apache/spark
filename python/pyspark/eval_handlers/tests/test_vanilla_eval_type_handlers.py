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

"""Tests for the plain Python (pickle) UDF eval type handler (``_vanilla``).

The handler is built from ``UDF`` records and fed
pickled-row input via ``run(0, <rows>)``.
"""

import datetime
import io
import os
from unittest.mock import patch

from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.eval_handlers._typing import UDF
from pyspark.eval_handlers._vanilla import BatchedUDFHandler
from pyspark.serializers import BatchedSerializer, CPickleSerializer
from pyspark.sql.types import DateType, LongType
from pyspark.testing.utils import PySparkBaseTestCase
from pyspark.util import PythonEvalType

with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker_util import EvalConf, RunnerConf


class BatchedUDFHandlerTests(PySparkBaseTestCase):
    @staticmethod
    def _handler(*udfs, kwargs_offsets=None, return_type=None):
        """Build the handler from ``(arg_offsets, func)`` pairs."""
        return BatchedUDFHandler(
            udfs=[
                UDF(
                    raw_func=f,
                    args_offsets=arg_offsets,
                    kwargs_offsets=kwargs_offsets or {},
                    return_type=return_type or LongType(),
                    result_id=0,
                )
                for arg_offsets, f in udfs
            ],
            runner_conf=RunnerConf({}),
            eval_conf=EvalConf({}),
        )

    def test_registered(self):
        self.assertIs(
            get_eval_type_handler(PythonEvalType.SQL_BATCHED_UDF, RunnerConf({})),
            BatchedUDFHandler,
        )

    def test_serializer_default_batch_size(self):
        with patch.dict(os.environ):
            os.environ.pop("PYTHON_UDF_BATCH_SIZE", None)
            ser = self._handler(([0], lambda x: x)).serializer
        self.assertIsInstance(ser, BatchedSerializer)
        self.assertIsInstance(ser.serializer, CPickleSerializer)
        self.assertEqual(ser.batchSize, 100)

    def test_serializer_batch_size_from_env(self):
        with patch.dict(os.environ, {"PYTHON_UDF_BATCH_SIZE": "7"}):
            ser = self._handler(([0], lambda x: x)).serializer
        self.assertEqual(ser.batchSize, 7)

    def test_single_udf_yields_bare_results(self):
        handler = self._handler(([1, 0], lambda a, b: a - b))
        rows = [(1, 10), (2, 20)]
        self.assertEqual(list(handler.run(0, iter(rows))), [9, 18])

    def test_multiple_udfs_yield_tuples(self):
        handler = self._handler(([0], lambda a: a + 1), ([0, 1], lambda a, b: a * b))
        rows = [(1, 10), (2, 20)]
        self.assertEqual(list(handler.run(0, iter(rows))), [(2, 10), (3, 40)])

    def test_kwargs_are_bound_by_name(self):
        handler = self._handler(([1], lambda a, b: a - b), kwargs_offsets={"b": 0})
        rows = [(1, 10), (2, 20)]
        self.assertEqual(list(handler.run(0, iter(rows))), [9, 18])

    def test_result_converted_when_return_type_needs_conversion(self):
        handler = self._handler(
            ([0], lambda d: d + datetime.timedelta(days=1)), return_type=DateType()
        )
        rows = [(datetime.date(1970, 1, 1),), (datetime.date(1970, 1, 10),)]
        self.assertEqual(list(handler.run(0, iter(rows))), [1, 10])

    def test_empty_input(self):
        handler = self._handler(([0], lambda a: a))
        self.assertEqual(list(handler.run(0, iter([]))), [])

    def test_lazy_over_input(self):
        consumed = []

        def rows():
            for i in range(3):
                consumed.append(i)
                yield (i,)

        out = self._handler(([0], lambda a: a * 2)).run(0, rows())
        self.assertEqual(consumed, [])
        self.assertEqual(next(out), 0)
        self.assertEqual(consumed, [0])

    def test_serializer_round_trip(self):
        handler = self._handler(([0], lambda a: a * 2))
        ser = handler.serializer
        buf = io.BytesIO()
        ser.dump_stream(iter([(i,) for i in range(5)]), buf)
        buf.seek(0)
        out = io.BytesIO()
        ser.dump_stream(handler.run(0, ser.load_stream(buf)), out)
        out.seek(0)
        self.assertEqual(list(ser.load_stream(out)), [0, 2, 4, 6, 8])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
