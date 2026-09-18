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

"""Tests for the Arrow eval type handlers (``_arrow``)."""

import unittest

from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.sql.pandas.serializers import ArrowStreamCoGroupSerializer, ArrowStreamSerializer
from pyspark.sql.types import LongType, StructField, StructType
from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message
from pyspark.util import PythonEvalType

if have_pyarrow:
    import pyarrow as pa

    # The handlers live in ``_arrow``, which imports pyarrow at module top.
    from pyspark.eval_handlers._arrow import (
        ArrowCoGroupedMapUDFHandler,
        ArrowGroupedMapIterUDFHandler,
        ArrowGroupedMapUDFHandler,
        ArrowMapUDFHandler,
        ArrowScalarIterUDFHandler,
        ArrowScalarUDFHandler,
    )


class _RunnerConf:
    """Minimal stand-in for the worker's RunnerConf, exposing only the fields
    the handlers under test read."""

    use_large_var_types = False
    assign_cols_by_name = True
    map_in_batch_legacy_accept_any_iterable = False


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowEvalTypeHandlerRegistrationTests(unittest.TestCase):
    def test_arrow_eval_types_are_registered(self):
        # Every migrated Arrow map/iter eval type dispatches to its handler by lookup.
        for eval_type, handler_cls in (
            (PythonEvalType.SQL_SCALAR_ARROW_UDF, ArrowScalarUDFHandler),
            (PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF, ArrowScalarIterUDFHandler),
            (PythonEvalType.SQL_MAP_ARROW_ITER_UDF, ArrowMapUDFHandler),
            (PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF, ArrowGroupedMapUDFHandler),
            (PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF, ArrowGroupedMapIterUDFHandler),
            (PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF, ArrowCoGroupedMapUDFHandler),
        ):
            self.assertIs(get_eval_type_handler(eval_type), handler_cls)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowScalarUDFHandlerTests(unittest.TestCase):
    def test_end_to_end_output(self):
        # One UDF reading column 0 (a pa.Array) and returning column + 1.
        def add_one(col):
            return pa.array([v.as_py() + 1 for v in col], type=pa.int64())

        udfs = [(add_one, [0], {}, LongType())]
        handler = ArrowScalarUDFHandler(udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None)

        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3], type=pa.int64())], ["_0"])
        out = list(handler.run(0, iter([batch])))

        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].num_columns, 1)
        self.assertEqual(out[0].column(0).to_pylist(), [2, 3, 4])

    def test_output_schema_enforced(self):
        # The UDF returns int32, but the declared return type is LongType (int64).
        # run must enforce the declared schema onto the output batch.
        def add_one(col):
            return pa.array([v.as_py() + 1 for v in col], type=pa.int32())

        udfs = [(add_one, [0], {}, LongType())]
        handler = ArrowScalarUDFHandler(udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None)

        batch = pa.RecordBatch.from_arrays([pa.array([10, 20], type=pa.int64())], ["_0"])
        out = list(handler.run(0, iter([batch])))
        # The int32 the UDF produced is coerced to the declared LongType (int64).
        self.assertEqual(out[0].schema.field(0).type, pa.int64())
        self.assertEqual(out[0].column(0).to_pylist(), [11, 21])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowScalarIterUDFHandlerTests(unittest.TestCase):
    def test_end_to_end_output(self):
        # The UDF receives an iterator of the single argument column and yields
        # an iterator of pa.Array; the handler assembles each into a RecordBatch.
        def add_one(col_iter):
            for col in col_iter:
                yield pa.array([v.as_py() + 1 for v in col], type=pa.int64())

        udfs = [(add_one, [0], {}, LongType())]
        handler = ArrowScalarIterUDFHandler(udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None)

        batches = [
            pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.int64())], ["_0"]),
            pa.RecordBatch.from_arrays([pa.array([3], type=pa.int64())], ["_0"]),
        ]
        out = list(handler.run(0, iter(batches)))
        self.assertEqual([b.column(0).to_pylist() for b in out], [[2, 3], [4]])

    def test_row_count_mismatch_is_rejected(self):
        from pyspark.errors import PySparkRuntimeError

        # Emitting more rows than were consumed must fail (fail-fast row limit).
        def too_many(col_iter):
            for col in col_iter:
                yield pa.array(list(range(len(col) + 1)), type=pa.int64())

        udfs = [(too_many, [0], {}, LongType())]
        handler = ArrowScalarIterUDFHandler(udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None)
        batch = pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.int64())], ["_0"])
        with self.assertRaises(PySparkRuntimeError):
            list(handler.run(0, iter([batch])))


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowMapUDFHandlerTests(unittest.TestCase):
    def test_end_to_end_output(self):
        from pyspark.sql.conversion import ArrowBatchTransformer

        # mapInArrow exchanges a single struct column on the wire; the handler
        # flattens it for the UDF and re-wraps the UDF's output.
        def double_a(batch_iter):
            for batch in batch_iter:
                doubled = pa.array([v.as_py() * 2 for v in batch.column(0)], type=pa.int64())
                yield pa.RecordBatch.from_arrays([doubled], ["a"])

        inner = pa.RecordBatch.from_arrays([pa.array([1, 2, 3], type=pa.int64())], ["a"])
        wrapped = ArrowBatchTransformer.wrap_struct(inner)

        udfs = [(double_a, None, None, None)]
        handler = ArrowMapUDFHandler(udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None)
        out = list(handler.run(0, iter([wrapped])))

        self.assertEqual(len(out), 1)
        # Output is a single struct column; its "a" field carries the doubled values.
        self.assertEqual(out[0].num_columns, 1)
        self.assertEqual(out[0].column(0).field("a").to_pylist(), [2, 4, 6])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedMapUDFHandlerTests(unittest.TestCase):
    # arg_offsets encoding for one DataFrame with key column 0 and value column 1:
    #   [group_len=3, num_keys=1, key_offset=0, value_offset=1]
    _ARG_OFFSETS = [3, 1, 0, 1]

    def _grouped_input(self):
        from pyspark.sql.conversion import ArrowBatchTransformer

        inner = pa.RecordBatch.from_arrays(
            [pa.array([7, 7], type=pa.int64()), pa.array([1, 2], type=pa.int64())], ["k", "v"]
        )
        wrapped = ArrowBatchTransformer.wrap_struct(inner)
        # One group, whose batches arrive as an iterator (matching the group serializer).
        return iter([iter([wrapped])])

    def test_values_only(self):
        return_type = StructType([StructField("v", LongType())])

        def grouped_udf(value_table):
            return pa.table({"v": pa.array([c.as_py() * 10 for c in value_table.column("v")])})

        udfs = [(grouped_udf, self._ARG_OFFSETS, return_type, 1)]
        handler = ArrowGroupedMapUDFHandler(udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None)
        out = list(handler.run(0, self._grouped_input()))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [10, 20])

    def test_key_and_values(self):
        return_type = StructType([StructField("v", LongType())])

        def grouped_udf(key, value_table):
            # key is the grouping-key tuple; add it to every value.
            k = key[0].as_py()
            return pa.table({"v": pa.array([c.as_py() + k for c in value_table.column("v")])})

        udfs = [(grouped_udf, self._ARG_OFFSETS, return_type, 2)]
        handler = ArrowGroupedMapUDFHandler(udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None)
        out = list(handler.run(0, self._grouped_input()))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [8, 9])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedMapIterUDFHandlerTests(unittest.TestCase):
    def test_end_to_end_output(self):
        from pyspark.sql.conversion import ArrowBatchTransformer

        return_type = StructType([StructField("v", LongType())])

        def grouped_udf(value_batches):
            for batch in value_batches:
                yield pa.RecordBatch.from_arrays(
                    [pa.array([c.as_py() + 1 for c in batch.column("v")], type=pa.int64())], ["v"]
                )

        inner = pa.RecordBatch.from_arrays(
            [pa.array([7], type=pa.int64()), pa.array([41], type=pa.int64())], ["k", "v"]
        )
        wrapped = ArrowBatchTransformer.wrap_struct(inner)
        udfs = [(grouped_udf, [3, 1, 0, 1], return_type, 1)]
        handler = ArrowGroupedMapIterUDFHandler(
            udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None
        )
        out = list(handler.run(0, iter([iter([wrapped])])))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [42])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowCoGroupedMapUDFHandlerTests(unittest.TestCase):
    def test_end_to_end_output(self):
        return_type = StructType([StructField("out", LongType())])

        def cogrouped_udf(left_values, right_values):
            total = left_values.column("lv")[0].as_py() + right_values.column("rv")[0].as_py()
            return pa.table({"out": pa.array([total], type=pa.int64())})

        # A co-group deserializes to a pair of lists of (non-struct-wrapped) batches.
        left = pa.RecordBatch.from_arrays(
            [pa.array([5], type=pa.int64()), pa.array([10], type=pa.int64())], ["k", "lv"]
        )
        right = pa.RecordBatch.from_arrays(
            [pa.array([5], type=pa.int64()), pa.array([20], type=pa.int64())], ["k", "rv"]
        )
        # Two DataFrames, each key column 0 and value column 1.
        arg_offsets = [3, 1, 0, 1, 3, 1, 0, 1]
        udfs = [(cogrouped_udf, arg_offsets, return_type, 2)]
        handler = ArrowCoGroupedMapUDFHandler(udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None)
        out = list(handler.run(0, iter([([left], [right])])))
        self.assertEqual(out[0].column(0).field("out").to_pylist(), [30])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class CoGroupedBatchTests(unittest.TestCase):
    def test_deserialized_co_group_is_a_pair_of_lists(self):
        # CoGroupedBatch must match what ArrowStreamCoGroupSerializer yields: the
        # serializer eagerly materializes each side as a list, not an iterator.
        import io

        from pyspark.serializers import write_int

        def arrow_bytes(batches):
            buf = io.BytesIO()
            ArrowStreamSerializer().dump_stream(iter(batches), buf)
            return buf.getvalue()

        left = pa.RecordBatch.from_arrays([pa.array([1, 2])], ["_0"])
        right = pa.RecordBatch.from_arrays([pa.array([9])], ["_0"])

        stream = io.BytesIO()
        write_int(2, stream)  # two DataFrames in the co-group
        stream.write(arrow_bytes([left]))
        stream.write(arrow_bytes([right]))
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
