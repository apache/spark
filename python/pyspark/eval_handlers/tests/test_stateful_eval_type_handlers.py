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

"""Tests for the stateful streaming eval type handlers (``_stateful``).

The handler tests build handler input in the wire format the JVM writers produce
(e.g. ``ApplyInPandasWithStateWriter``) and assert on the output batches via
``run(0, <input>)``. Handlers are built on a real ``RunnerConf`` / ``EvalConf`` (from
plain conf dicts).
"""

import json
import os
import unittest
from unittest.mock import patch

from pyspark.errors import PySparkRuntimeError, PySparkTypeError
from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.serializers import CPickleSerializer
from pyspark.sql.types import LongType, StructField, StructType
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

if have_pandas and have_pyarrow:
    import pandas as pd
    import pyarrow as pa

    from pyspark.eval_handlers._stateful import PandasGroupedMapWithStateUDFHandler

_missing_message = pandas_requirement_message or pyarrow_requirement_message


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class StatefulEvalTypeHandlerRegistrationTests(PySparkBaseTestCase):
    def test_stateful_eval_types_are_registered(self):
        # Each migrated stateful eval type dispatches to its handler by lookup.
        for eval_type, handler_cls in (
            (
                PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
                PandasGroupedMapWithStateUDFHandler,
            ),
        ):
            with self.subTest(eval_type=eval_type):
                self.assertIs(get_eval_type_handler(eval_type, RunnerConf({})), handler_cls)


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasGroupedMapWithStateUDFHandlerTests(PySparkBaseTestCase):
    """SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE: input batches carry the data columns plus a
    trailing state-info struct column (ApplyInPandasWithStateWriter.STATE_METADATA_SCHEMA),
    one state-info row per (grouping key, chunk)."""

    _RETURN_TYPE = StructType([StructField("k", LongType()), StructField("n", LongType())])
    _STATE_SCHEMA = StructType([StructField("total", LongType())])
    # [length, num_keys, *key_cols, *value_cols]: key "k" at 0, value "v" at 1.
    _ARG_OFFSETS = [3, 1, 0, 1]

    @staticmethod
    def _state_properties(has_timed_out=False):
        return json.dumps(
            {
                "batchProcessingTimeMs": 0,
                "eventTimeWatermarkMs": -1,
                "timeoutConf": "NoTimeout",
                "hasTimedOut": has_timed_out,
                "watermarkPresent": False,
                "defined": False,
                "updated": False,
                "removed": False,
                "timeoutTimestamp": -1,
            }
        )

    @classmethod
    def _batch(cls, keys, values, chunks):
        """A data + state-info RecordBatch. ``chunks`` is a list of
        ``(key, start_offset, num_rows, is_last_chunk, has_timed_out)``; state-info rows past
        the chunks are null, as the JVM writer pads them."""
        state_rows = [
            {
                "properties": cls._state_properties(has_timed_out),
                "keyRowAsUnsafe": ("key-%d" % key).encode("utf-8"),
                "object": None,
                "startOffset": start,
                "numRows": num_rows,
                "isLastChunk": is_last,
            }
            for key, start, num_rows, is_last, has_timed_out in chunks
        ]
        state_rows += [None] * (len(keys) - len(state_rows))
        state_type = pa.struct(
            [
                ("properties", pa.string()),
                ("keyRowAsUnsafe", pa.binary()),
                ("object", pa.binary()),
                ("startOffset", pa.int32()),
                ("numRows", pa.int32()),
                ("isLastChunk", pa.bool_()),
            ]
        )
        return pa.RecordBatch.from_arrays(
            [
                pa.array(keys, type=pa.int64()),
                pa.array(values, type=pa.int64()),
                pa.array(state_rows, type=state_type),
            ],
            ["k", "v", "__state"],
        )

    @classmethod
    def _handler(cls, udf, runner_conf=None):
        return PandasGroupedMapWithStateUDFHandler(
            udfs=[(udf, cls._ARG_OFFSETS, cls._RETURN_TYPE)],
            runner_conf=runner_conf or RunnerConf({}),
            eval_conf=EvalConf({"state_value_schema": cls._STATE_SCHEMA.json()}),
        )

    @staticmethod
    def _decode(batches):
        """Return (data rows, state rows) from the count/data/state output batches, dropping
        the padding rows past each batch's counts."""
        data_rows, state_rows = [], []
        for b in batches:
            counts = b.column(0).to_pylist()
            data_cnt, state_cnt = counts[0]["dataCount"], counts[0]["stateCount"]
            data_rows += b.column(1).to_pylist()[:data_cnt]
            state_rows += b.column(2).to_pylist()[:state_cnt]
        return data_rows, state_rows

    def test_groups_chunks_and_updates_state(self):
        calls = []

        def udf(key, pdfs, state):
            pdfs = list(pdfs)
            calls.append((key, [pdf["v"].tolist() for pdf in pdfs]))
            total = sum(int(pdf["v"].sum()) for pdf in pdfs)
            state.update((total,))
            yield pd.DataFrame({"k": [key[0]], "n": [total]})

        # Key 1 spans two batches (two chunks, one state instance); key 2 is one chunk.
        batches = [
            self._batch([1, 1], [10, 20], [(1, 0, 2, False, False)]),
            self._batch([1, 2, 2], [30, 1, 2], [(1, 0, 1, True, False), (2, 1, 2, True, False)]),
        ]
        out = list(self._handler(udf).run(0, iter(batches)))

        self.assertEqual(calls, [((1,), [[10, 20], [30]]), ((2,), [[1, 2]])])
        data_rows, state_rows = self._decode(out)
        self.assertEqual(data_rows, [{"k": 1, "n": 60}, {"k": 2, "n": 3}])
        self.assertEqual([s["keyRowAsUnsafe"] for s in state_rows], [b"key-1", b"key-2"])
        self.assertEqual(
            [CPickleSerializer().loads(s["object"]) for s in state_rows], [(60,), (3,)]
        )
        self.assertTrue(all(json.loads(s["properties"])["updated"] for s in state_rows))

    def test_timed_out_key_receives_single_empty_dataframe(self):
        seen = []

        def udf(key, pdfs, state):
            pdfs = list(pdfs)
            seen.append((state.hasTimedOut, [(list(pdf.columns), len(pdf)) for pdf in pdfs]))
            return iter([])

        batch = self._batch([7], [0], [(7, 0, 1, True, True)])
        out = list(self._handler(udf).run(0, iter([batch])))

        self.assertEqual(seen, [(True, [(["v"], 0)])])
        data_rows, state_rows = self._decode(out)
        self.assertEqual(data_rows, [])
        self.assertEqual(len(state_rows), 1)
        self.assertIsNone(state_rows[0]["object"])

    def test_flushes_batches_by_max_records(self):
        def udf(key, pdfs, state):
            for pdf in pdfs:
                yield pd.DataFrame({"k": [key[0]] * len(pdf), "n": pdf["v"]})

        batch = self._batch(
            [1, 1, 2, 2],
            [1, 2, 3, 4],
            [(1, 0, 2, True, False), (2, 2, 2, True, False)],
        )
        runner_conf = RunnerConf({"spark.sql.execution.arrow.maxRecordsPerBatch": "1"})
        out = list(self._handler(udf, runner_conf).run(0, iter([batch])))

        # Each key's 2 rows exceed the threshold of 1, so each flushes its own batch;
        # the state of each key lands in the following batch.
        self.assertEqual(len(out), 3)
        data_rows, state_rows = self._decode(out)
        self.assertEqual([r["n"] for r in data_rows], [1, 2, 3, 4])
        self.assertEqual([s["keyRowAsUnsafe"] for s in state_rows], [b"key-1", b"key-2"])

    def test_rejects_dataframe_result(self):
        def udf(key, pdfs, state):
            return pd.DataFrame({"k": [1], "n": [1]})

        batch = self._batch([1], [1], [(1, 0, 1, True, False)])
        with self.assertRaisesRegex(PySparkTypeError, "iterable of pandas.DataFrame"):
            list(self._handler(udf).run(0, iter([batch])))

    def test_rejects_non_dataframe_element(self):
        def udf(key, pdfs, state):
            return iter([1])

        batch = self._batch([1], [1], [(1, 0, 1, True, False)])
        with self.assertRaisesRegex(PySparkTypeError, "iterator of pandas.DataFrame"):
            list(self._handler(udf).run(0, iter([batch])))

    def test_rejects_column_count_mismatch(self):
        def udf(key, pdfs, state):
            yield pd.DataFrame({"k": [1]})

        batch = self._batch([1], [1], [(1, 0, 1, True, False)])
        with self.assertRaises(PySparkRuntimeError) as ctx:
            list(self._handler(udf).run(0, iter([batch])))
        self.assertEqual(ctx.exception.getCondition(), "RESULT_COLUMN_SCHEMA_MISMATCH")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
